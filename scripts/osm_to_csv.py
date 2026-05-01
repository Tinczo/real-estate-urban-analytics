"""
Konwertuje dane OSM z formatu GeoParquet (HDFS) na CSV (HDFS),
filtrując POI istotne dla modelu wskaźników nieruchomości.

Uruchomienie: spark-submit --master yarn --deploy-mode client osm_to_csv.py
"""

import os
os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3.9'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3.9'

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType

HDFS_INPUT  = "hdfs://master:9000/user/hadoop/data/osm/"
HDFS_OUTPUT = "hdfs://master:9000/user/hadoop/data/osm_csv/"

# Wagi obiektów wg typu — wartości zgodne z raportem 4
ROAD_WEIGHTS = {
    "motorway": 3.0,
    "trunk":    2.0,
    "primary":  1.5,
    "secondary":1.0,
}

CATEGORY_RULES = [
    # (klucz_tagu, wartości_tagu, kategoria, waga_bazowa)
    # HANDEL
    ("shop", ["supermarket", "mall", "department_store", "convenience",
              "clothes", "electronics", "furniture"], "HANDEL", 3.0),
    ("amenity", ["marketplace"], "HANDEL", 2.0),
    # GASTRONOMIA
    ("amenity", ["restaurant", "cafe", "fast_food", "bar", "pub",
                 "food_court", "biergarten"], "GASTRO", 2.0),
    # REKREACJA (waga = powierzchnia z geometrii lub stała 10000.0)
    ("leisure", ["park", "garden", "playground", "sports_centre",
                 "stadium", "fitness_centre", "swimming_pool"], "REKREACJA", 10000.0),
    ("landuse", ["forest", "grass", "meadow", "recreation_ground"], "REKREACJA", 10000.0),
    ("natural", ["wood", "scrub", "heath"], "REKREACJA", 10000.0),
    # EDUKACJA
    ("amenity", ["school", "university", "college", "kindergarten",
                 "library", "language_school"], "EDUKACJA", 1.0),
    # DROGI (waga kodowana jako W_road * lanes w skrypcie poniżej)
    ("highway", list(ROAD_WEIGHTS.keys()), "DROGA", 1.0),
]


def flatten_tags(tags_col):
    """
    Spłaszcza kolumnę tagów do słownika {klucz: wartość}.
    Obsługuje zarówno MapType jak i ArrayType(StructType(key,value)).
    """
    return tags_col


def assign_category_and_weight(tags: dict) -> tuple:
    """
    Przypisuje kategorię i wagę obiektu na podstawie tagów OSM.
    Zwraca (category, waga) lub (None, None) jeśli brak dopasowania.
    """
    if not tags:
        return None, None

    for key, values, category, base_weight in CATEGORY_RULES:
        tag_val = tags.get(key)
        if tag_val in values:
            if category == "DROGA":
                w_road = ROAD_WEIGHTS.get(tag_val, 1.0)
                try:
                    lanes = float(tags.get("lanes", "2"))
                except (ValueError, TypeError):
                    lanes = 2.0
                # waga koduje W_road * lanes — reducer dzieli przez dystans
                return category, w_road * lanes
            return category, base_weight

    return None, None


def main():
    spark = SparkSession.builder \
        .appName("OSM Parquet to CSV") \
        .master("yarn") \
        .config("spark.cores.max", "6") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "12") \
        .config("spark.executorEnv.PYSPARK_PYTHON", "/usr/bin/python3.9") \
        .config("spark.executor.extraJavaOptions", "-XX:-UseContainerSupport") \
        .config("spark.driver.extraJavaOptions", "-XX:-UseContainerSupport") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"Wczytywanie OSM Parquet z: {HDFS_INPUT}")
    df = spark.read.parquet(HDFS_INPUT)

    print("Schemat OSM:")
    df.printSchema()
    print(f"Liczba rekordów (przed filtrowaniem): {df.count()}")

    # Zidentyfikuj kolumny z tagami i geometrią
    # Typowe nazwy w GeoParquet OSM: tags (MapType), geometry (lub lat/lon)
    cols = [c.lower() for c in df.columns]

    # Kolumny geometrii
    has_lat = "lat" in cols and "lon" in cols
    has_geometry = "geometry" in cols

    # UDF do wyciągania kategorii i wagi
    @F.udf(returnType="struct<category:string,waga:double>")
    def get_category_weight(tags):
        cat, waga = assign_category_and_weight(tags)
        if cat is None:
            return None
        return {"category": cat, "waga": float(waga)}

    # Zastosuj UDF
    df_with_cat = df.withColumn("cat_weight", get_category_weight(F.col("tags"))) \
                    .filter(F.col("cat_weight").isNotNull()) \
                    .withColumn("category", F.col("cat_weight.category")) \
                    .withColumn("waga_obiektu", F.col("cat_weight.waga"))

    # Wyciągnij lat/lon
    if has_lat:
        df_out = df_with_cat.select(
            F.col("lat").cast(DoubleType()),
            F.col("lon").cast(DoubleType()),
            F.col("category"),
            F.col("waga_obiektu")
        )
    elif has_geometry:
        # GeoParquet: wyciągnij centroid z geometrii WKB/WKT
        # Próbujemy ST_X/ST_Y jeśli dostępne, inaczej parsujemy string
        try:
            df_out = df_with_cat.select(
                F.expr("ST_Y(geometry)").alias("lat").cast(DoubleType()),
                F.expr("ST_X(geometry)").alias("lon").cast(DoubleType()),
                F.col("category"),
                F.col("waga_obiektu")
            )
        except Exception:
            # Fallback: geometry jako string "POINT(lon lat)"
            df_out = df_with_cat \
                .withColumn("geom_str", F.col("geometry").cast(StringType())) \
                .withColumn("lon", F.regexp_extract("geom_str", r"POINT\s*\(([0-9.\-]+)", 1).cast(DoubleType())) \
                .withColumn("lat", F.regexp_extract("geom_str", r"POINT\s*\([0-9.\-]+\s+([0-9.\-]+)", 1).cast(DoubleType())) \
                .select("lat", "lon", "category", "waga_obiektu")
    else:
        raise RuntimeError(f"Brak kolumn lat/lon lub geometry. Dostępne kolumny: {df.columns}")

    # Usuń rekordy bez współrzędnych
    df_out = df_out.filter(
        F.col("lat").isNotNull() & F.col("lon").isNotNull() &
        (F.col("lat") != 0.0) & (F.col("lon") != 0.0)
    )

    count = df_out.count()
    print(f"Liczba rekordów po filtrowaniu: {count}")

    # Zapisz jako CSV
    print(f"Zapisywanie CSV do: {HDFS_OUTPUT}")
    df_out.coalesce(12).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(HDFS_OUTPUT)

    print("Gotowe!")
    spark.stop()


if __name__ == "__main__":
    main()
