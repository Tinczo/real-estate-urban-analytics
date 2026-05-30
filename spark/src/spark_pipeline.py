import argparse
import sys
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, broadcast, udf, split, corr, lit
)
from pyspark.sql.types import StringType

__base32 = '0123456789bcdefghjkmnpqrstuvwxyz'

def geohash_encode(latitude, longitude, precision=6):
    """Pure Python geohash encoder (length 6)"""
    lat_interval = (-90.0, 90.0)
    lon_interval = (-180.0, 180.0)
    geohash = []
    bits = [16, 8, 4, 2, 1]
    bit = 0
    ch = 0
    even = True
    while len(geohash) < precision:
        if even:
            mid = (lon_interval[0] + lon_interval[1]) / 2
            if longitude > mid:
                ch |= bits[bit]
                lon_interval = (mid, lon_interval[1])
            else:
                lon_interval = (lon_interval[0], mid)
        else:
            mid = (lat_interval[0] + lat_interval[1]) / 2
            if latitude > mid:
                ch |= bits[bit]
                lat_interval = (mid, lat_interval[1])
            else:
                lat_interval = (lat_interval[0], mid)
        even = not even
        if bit < 4:
            bit += 1
        else:
            geohash.append(__base32[ch])
            bit = 0
            ch = 0
    return ''.join(geohash)

def run_e1_1(spark, stop_times_path, cache_dir, output_path, disable_broadcast=False):
    print("==> Running E1.1: GTFS Extract + Map Side Join")
    stop_times = spark.read.csv(stop_times_path, header=True)
    trips = spark.read.csv(f"{cache_dir}/trips.txt", header=True)
    routes = spark.read.csv(f"{cache_dir}/routes.txt", header=True)
    calendar = spark.read.csv(f"{cache_dir}/calendar.txt", header=True)
    stops = spark.read.csv(f"{cache_dir}/stops.txt", header=True)
    
    # Filter calendar for workdays
    calendar = calendar.filter((col("monday") == "1") | (col("tuesday") == "1") | 
                               (col("wednesday") == "1") | (col("thursday") == "1") | 
                               (col("friday") == "1")).select("service_id").distinct()
    
    routes = routes.select("route_id", "route_type")
    trips = trips.select("trip_id", "route_id", "service_id")
    
    def b(df):
        return df if disable_broadcast else broadcast(df)
    
    trips_info = trips.join(b(routes), "route_id").join(b(calendar), "service_id")
    
    stops = stops.select("stop_id", col("stop_lat").cast("double"), col("stop_lon").cast("double"))
    geohash_udf = udf(lambda lat, lon: geohash_encode(lat, lon, 6), StringType())
    stops = stops.withColumn("geoId", geohash_udf("stop_lat", "stop_lon"))
    
    stop_times = stop_times.select("trip_id", "arrival_time", "stop_id")
    
    joined = stop_times.join(b(trips_info), "trip_id") \
                       .join(b(stops), "stop_id")
    
    res = joined.select("stop_id", "stop_lat", "stop_lon", "geoId", "route_type", "arrival_time")
    res.write.parquet(output_path, mode="overwrite")

def run_e1_5(spark, input_path, output_path, reducers=1):
    print(f"==> Running E1.5: GTFS Aggregation (reducers={reducers})")
    df = spark.read.parquet(input_path)
    df = df.withColumn("hour", split(col("arrival_time"), ":")[0].cast("int"))
    df = df.filter((col("hour") >= 7) & (col("hour") <= 9))
    
    if reducers:
        spark.conf.set("spark.sql.shuffle.partitions", str(reducers))
    
    agg_df = df.groupBy("stop_id", "stop_lat", "stop_lon", "geoId") \
               .count().withColumnRenamed("count", "score")
               
    agg_df.write.parquet(output_path, mode="overwrite")

def run_e1_2(spark, input_path, output_path):
    print("==> Running E1.2: OSM Extraction")
    df = spark.read.csv(input_path, header=True)
    df = df.select(
        col("lat").cast("double"),
        col("lon").cast("double"),
        col("category"),
        col("waga_obiektu").cast("double")
    ).filter(~((col("lat") == 0.0) & (col("lon") == 0.0)))
    
    geohash_udf = udf(lambda lat, lon: geohash_encode(lat, lon, 6), StringType())
    df = df.withColumn("geoId", geohash_udf("lat", "lon"))
    df.write.parquet(output_path, mode="overwrite")

def run_e1_3(spark, input_path, output_path):
    print("==> Running E1.3: Real Estate Normalization")
    df = spark.read.option("header", "true").option("sep", ";").csv(input_path)
    
    # rename columns to match MR logic (Cena m2 -> cenaM2, itd)
    df = df.select(
        col("Miejscowość").alias("miasto"),
        col("Cena m2").cast("double").alias("cenaM2"),
        col("Cena całkowita").cast("double").alias("cenaCalkowita"),
        col("Powierzchnia").cast("double").alias("powierzchnia"),
        col("lat").cast("double"),
        col("lon").cast("double")
    ).filter(~((col("lat") == 0.0) & (col("lon") == 0.0)))
    
    geohash_udf = udf(lambda lat, lon: geohash_encode(lat, lon, 6), StringType())
    df = df.withColumn("geoId", geohash_udf("lat", "lon"))
    df.write.parquet(output_path, mode="overwrite")

def haversine(lat1, lon1, lat2, lon2):
    R = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2.0) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * \
        math.sin(delta_lambda / 2.0) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def compute_indicators(key, iterator):
    # key: geoId
    # iterator yields (geoId, (row_type, dict))
    
    stops = []
    pois = []
    offers = []
    
    for _, (r_type, row) in iterator:
        if r_type == 'stop': stops.append(row)
        elif r_type == 'poi': pois.append(row)
        elif r_type == 'offer': offers.append(row)
        
    if not offers:
        return
        
    walking_radius = 800.0
    alpha_edukacja = 0.002
    
    for offer in offers:
        wsk_przep = 0.0
        ind_handlu = 0.0
        ind_gastro = 0.0
        ind_rekr = 0.0
        ind_eduk = 0.0
        min_droga_dist = float('inf')
        waga_najblizszej_drogi = 0.0
        
        olat, olon = offer['lat'], offer['lon']
        
        for stop in stops:
            d = haversine(olat, olon, stop['stop_lat'], stop['stop_lon'])
            if 0 < d <= walking_radius:
                wsk_przep += stop['score'] / d
                
        for poi in pois:
            d = haversine(olat, olon, poi['lat'], poi['lon'])
            if d <= 0 or d > walking_radius:
                continue
                
            d2 = d * d
            cat = poi['category']
            waga = poi['waga_obiektu']
            
            if cat == 'HANDEL':
                ind_handlu += waga / d2
            elif cat == 'GASTRO':
                ind_gastro += waga / d2
            elif cat == 'REKREACJA':
                ind_rekr += waga / d2
            elif cat == 'EDUKACJA':
                ind_eduk += math.exp(-alpha_edukacja * d)
            elif cat == 'DROGA':
                if d < min_droga_dist:
                    min_droga_dist = d
                    waga_najblizszej_drogi = waga
                    
        wsp_halas = 0.0
        if min_droga_dist < float('inf') and min_droga_dist > 0:
            wsp_halas = waga_najblizszej_drogi / min_droga_dist
            
        yield f"{offer['miasto']}|{key}\t,{key},{offer['cenaM2']},{offer['cenaCalkowita']},{offer['powierzchnia']},{offer['miasto']},{wsk_przep},{ind_handlu},{ind_gastro},{ind_rekr},{ind_eduk},{wsp_halas}"

def run_e2(spark, t1_5_dir, t1_2_dir, t1_3_dir, output_dir):
    print("==> Running E2: Join & Indicators calculation")
    stops_df = spark.read.parquet(t1_5_dir)
    pois_df = spark.read.parquet(t1_2_dir)
    offers_df = spark.read.parquet(t1_3_dir)
    
    stops_rdd = stops_df.rdd.map(lambda r: (r.geoId, ('stop', r.asDict())))
    pois_rdd = pois_df.rdd.map(lambda r: (r.geoId, ('poi', r.asDict())))
    offers_rdd = offers_df.rdd.map(lambda r: (r.geoId, ('offer', r.asDict())))
    
    joined_rdd = stops_rdd.union(pois_rdd).union(offers_rdd)
    
    res_rdd = joined_rdd.groupByKey().mapPartitions(lambda it: (out for key, vals in it for out in compute_indicators(key, [(key, v) for v in vals])))
    
    res_rdd.saveAsTextFile(output_dir)

def run_e3(spark, input_dir, output_dir):
    print("==> Running E3: Pearson Correlation Matrix")
    rdd = spark.sparkContext.textFile(input_dir)
    
    def parse_line(line):
        def f(s):
            try: return float(s)
            except: return 0.0
        csv_part = line.split('\t')[1]
        parts = csv_part.split(',')
        return {
            'miasto': parts[5],
            'cenaM2': f(parts[2]),
            'cenaCalkowita': f(parts[3]),
            'powierzchnia': f(parts[4]),
            'wskPrzep': f(parts[6]),
            'indHandlu': f(parts[7]),
            'indGastro': f(parts[8]),
            'indRekr': f(parts[9]),
            'indEduk': f(parts[10]),
            'wspHalas': f(parts[11])
        }
        
    df = rdd.map(parse_line).toDF()
    
    indicators = ['wskPrzep', 'indHandlu', 'indGastro', 'indRekr', 'indEduk', 'wspHalas']
    prices = ['cenaM2', 'cenaCalkowita', 'powierzchnia']
    
    # Calculate correlations
    # To output exact format as E3Reducer (miasto, wskaznik, rM2, rCal, rPow)
    # We will compute aggregations manually per city
    aggs = []
    for x in indicators:
        for y in prices:
            aggs.append(corr(x, y).alias(f"{x}_{y}"))
            
    res_df = df.groupBy("miasto").agg(*aggs)
    
    # map to final format
    def format_output(row):
        out = []
        mapping = {
            'wskPrzep': 'wsk_przepustowosci', 'indHandlu': 'ind_handlu',
            'indGastro': 'ind_gastro', 'indRekr': 'ind_rekreacji',
            'indEduk': 'ind_edukacji', 'wspHalas': 'wsp_halas'
        }
        for ind in indicators:
            c_m2 = row[f"{ind}_cenaM2"] or 0.0
            c_cal = row[f"{ind}_cenaCalkowita"] or 0.0
            c_pow = row[f"{ind}_powierzchnia"] or 0.0
            
            fmt = lambda v: f"{v:.6f}" if not math.isnan(v) else "0.000000"
            line = f"{row['miasto']}\t{mapping[ind]},{fmt(c_m2)},{fmt(c_cal)},{fmt(c_pow)}"
            out.append(line)
        return out
        
    final_rdd = res_df.rdd.flatMap(format_output)
    final_rdd.saveAsTextFile(output_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark GTFS/RealEstate Pipeline")
    parser.add_argument("--stage", required=True, choices=["e1_1", "e1_5", "e1_2", "e1_3", "e2", "e3", "all"])
    parser.add_argument("--stop_times", default="/gtfs/stop_times.txt")
    parser.add_argument("--gtfs_cache", default="/gtfs")
    parser.add_argument("--osm_csv", default="/osm_csv")
    parser.add_argument("--re_geocoded", default="/real_estate_geocoded")
    parser.add_argument("--t1_1", default="/user/hadoop/t1_1_py")
    parser.add_argument("--t1_5", default="/user/hadoop/t1_5_py")
    parser.add_argument("--t1_2", default="/user/hadoop/t1_2_py")
    parser.add_argument("--t1_3", default="/user/hadoop/t1_3_py")
    parser.add_argument("--t2", default="/user/hadoop/t2_py")
    parser.add_argument("--t3", default="/user/hadoop/t3_py")
    parser.add_argument("--reducers", type=int, default=1)
    parser.add_argument("--disable_broadcast", action="store_true")
    
    args = parser.parse_args()
    
    spark = SparkSession.builder.appName(f"PySpark Pipeline: {args.stage}").getOrCreate()
    
    if args.stage in ["e1_1", "all"]:
        run_e1_1(spark, args.stop_times, args.gtfs_cache, args.t1_1, args.disable_broadcast)
    if args.stage in ["e1_5", "all"]:
        run_e1_5(spark, args.t1_1, args.t1_5, args.reducers)
    if args.stage in ["e1_2", "all"]:
        run_e1_2(spark, args.osm_csv, args.t1_2)
    if args.stage in ["e1_3", "all"]:
        run_e1_3(spark, args.re_geocoded, args.t1_3)
    if args.stage in ["e2", "all"]:
        run_e2(spark, args.t1_5, args.t1_2, args.t1_3, args.t2)
    if args.stage in ["e3", "all"]:
        run_e3(spark, args.t2, args.t3)
        
    spark.stop()
