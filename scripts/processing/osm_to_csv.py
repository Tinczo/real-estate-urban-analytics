"""
Konwertuje dane OSM z formatu GeoParquet na CSV,
filtrujac POI istotne dla modelu wskaznikow nieruchomosci.

Uzycie lokalnie: spark-submit --master yarn --deploy-mode client osm_to_csv.py
Uzycie na Databricks: spark_python_task z parametrami --input ... --output ...
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType

ROAD_WEIGHTS = {
    "motorway": 3.0,
    "trunk":    2.0,
    "primary":  1.5,
    "secondary":1.0,
}

CATEGORY_RULES = [
    ("shop", ["supermarket", "mall", "department_store", "convenience",
              "clothes", "electronics", "furniture"], "HANDEL", 3.0),
    ("amenity", ["marketplace"], "HANDEL", 2.0),
    ("amenity", ["restaurant", "cafe", "fast_food", "bar", "pub",
                 "food_court", "biergarten"], "GASTRO", 2.0),
    ("leisure", ["park", "garden", "playground", "sports_centre",
                 "stadium", "fitness_centre", "swimming_pool"], "REKREACJA", 10000.0),
    ("landuse", ["forest", "grass", "meadow", "recreation_ground"], "REKREACJA", 10000.0),
    ("natural", ["wood", "scrub", "heath"], "REKREACJA", 10000.0),
    ("amenity", ["school", "university", "college", "kindergarten",
                 "library", "language_school"], "EDUKACJA", 1.0),
    ("highway", list(ROAD_WEIGHTS.keys()), "DROGA", 1.0),
]


def assign_category_and_weight(tags: dict) -> tuple:
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
                return category, w_road * lanes
            return category, base_weight
    return None, None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",  default=os.environ.get("OSM_INPUT",  "hdfs://master:9000/osm/parquet/"))
    parser.add_argument("--output", default=os.environ.get("OSM_OUTPUT", "hdfs://master:9000/osm_csv/"))
    args = parser.parse_args()

    builder = SparkSession.builder.appName("OSM Parquet to CSV")
    # Lokalnie dodaj YARN, na Databricks SparkSession jest juz skonfigurowana
    if not os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        builder = builder \
            .master("yarn") \
            .config("spark.cores.max", "6") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "12") \
            .config("spark.executorEnv.PYSPARK_PYTHON", "/usr/bin/python3.9") \
            .config("spark.executor.extraJavaOptions", "-XX:-UseContainerSupport") \
            .config("spark.driver.extraJavaOptions", "-XX:-UseContainerSupport")
        os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3.9'
        os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3.9'

    spark = builder.getOrCreate()

    input_path = args.input.rstrip("/") + "/parquet"
    print(f"Wczytywanie OSM Parquet z: {input_path}")
    df = spark.read.option("basePath", args.input).parquet(input_path)
    df.printSchema()
    print(f"Liczba rekordow (przed filtrowaniem): {df.count()}")

    cols = [c.lower() for c in df.columns]
    has_lat = "lat" in cols and "lon" in cols
    has_geometry = "geometry" in cols

    @F.udf(returnType="struct<category:string,waga:double>")
    def get_category_weight(tags):
        cat, waga = assign_category_and_weight(tags)
        if cat is None:
            return None
        return {"category": cat, "waga": float(waga)}

    df_with_cat = df.withColumn("cat_weight", get_category_weight(F.col("tags"))) \
                    .filter(F.col("cat_weight").isNotNull()) \
                    .withColumn("category", F.col("cat_weight.category")) \
                    .withColumn("waga_obiektu", F.col("cat_weight.waga"))

    if has_lat:
        df_out = df_with_cat.select(
            F.col("lat").cast(DoubleType()),
            F.col("lon").cast(DoubleType()),
            F.col("category"),
            F.col("waga_obiektu")
        )
    elif has_geometry:
        try:
            df_out = df_with_cat.select(
                F.expr("ST_Y(geometry)").cast(DoubleType()).alias("lat"),
                F.expr("ST_X(geometry)").cast(DoubleType()).alias("lon"),
                F.col("category"),
                F.col("waga_obiektu")
            )
            df_out.limit(1).count()
        except Exception:
            df_out = df_with_cat \
                .withColumn("geom_str", F.col("geometry").cast(StringType())) \
                .withColumn("lon", F.regexp_extract("geom_str", r"POINT\s*\(([0-9.\-]+)", 1).cast(DoubleType())) \
                .withColumn("lat", F.regexp_extract("geom_str", r"POINT\s*\([0-9.\-]+\s+([0-9.\-]+)", 1).cast(DoubleType())) \
                .select("lat", "lon", "category", "waga_obiektu")
    else:
        raise RuntimeError(f"Brak kolumn lat/lon lub geometry. Dostepne kolumny: {df.columns}")

    df_out = df_out.filter(
        F.col("lat").isNotNull() & F.col("lon").isNotNull() &
        (F.col("lat") != 0.0) & (F.col("lon") != 0.0)
    )

    print(f"Liczba rekordow po filtrowaniu: {df_out.count()}")
    print(f"Zapisywanie CSV do: {args.output}")
    df_out.coalesce(12).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(args.output)

    print("Gotowe!")
    spark.stop()


if __name__ == "__main__":
    main()
