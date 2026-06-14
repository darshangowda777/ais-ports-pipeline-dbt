from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when, row_number
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window
import math

spark = SparkSession.builder \
    .appName("AIS Vessel Geofence Join") \
    .getOrCreate()

PROJECT = "ais-ports-pipeline-2"
BUCKET  = "gs://ais-ports-pipeline-bucket"

print("✓ Spark session started")

# ── Haversine UDF ─────────────────────────────────────────────
def haversine(lat1, lon1, lat2, lon2):
    try:
        R  = 3440.065
        p1 = math.radians(float(lat1))
        p2 = math.radians(float(lat2))
        dp = math.radians(float(lat2) - float(lat1))
        dl = math.radians(float(lon2) - float(lon1))
        a  = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
        return float(R * 2 * math.asin(math.sqrt(max(0, min(1, a)))))
    except:
        return None

haversine_udf = udf(haversine, FloatType())

# ── Load vessel pings from GCS ────────────────────────────────
print("Loading vessel pings from GCS...")
vessels = spark.read.json(f"{BUCKET}/raw-data/vessel_pings/*.json")
vessel_count = vessels.count()
print(f"✓ Loaded {vessel_count} vessel pings")

# ── Load ports from BigQuery ──────────────────────────────────
print("Loading ports from BigQuery...")
ports = spark.read.format("bigquery") \
    .option("table", f"{PROJECT}.raw.world_port_index") \
    .load() \
    .select(
        col("port_id"),
        col("port_name"),
        col("country_code"),
        col("region"),
        col("water_body"),
        col("harbor_size"),
        col("harbor_type"),
        col("channel_depth_m"),
        col("latitude").alias("port_lat"),
        col("longitude").alias("port_lon")
    )
port_count = ports.count()
print(f"✓ Loaded {port_count} ports")

# ── Broadcast ports (small table) ────────────────────────────
from pyspark.sql.functions import broadcast
ports_broadcast = broadcast(ports)

# ── Cross join + Haversine distance ──────────────────────────
print("Running Haversine geofence join...")
crossed = vessels.crossJoin(ports_broadcast)

with_distance = crossed.withColumn(
    "distance_nmi",
    haversine_udf(
        col("latitude").cast("double"),
        col("longitude").cast("double"),
        col("port_lat").cast("double"),
        col("port_lon").cast("double")
    )
)

# ── Filter to 50 nmi ──────────────────────────────────────────
in_zone = with_distance.filter(
    (col("distance_nmi").isNotNull()) &
    (col("distance_nmi") <= 50)
)

# ── Pick nearest port per vessel per timestamp ────────────────
window = Window.partitionBy("mmsi", "timestamp") \
               .orderBy(col("distance_nmi").asc())

nearest = in_zone.withColumn("rank", row_number().over(window)) \
                 .filter(col("rank") == 1) \
                 .drop("rank", "port_lat", "port_lon")

# ── Add congestion signal ─────────────────────────────────────
result = nearest.withColumn(
    "is_congestion",
    when(
        (col("nav_status") == "At Anchor") &
        (col("distance_nmi") > 5),
        True
    ).otherwise(False)
)

result_count = result.count()
print(f"✓ Geofence join complete — {result_count} rows matched to ports")

# ── Write to BigQuery ─────────────────────────────────────────
print("Writing to BigQuery...")
result.write.format("bigquery") \
    .option("table", f"{PROJECT}.raw.vessel_pings_spark") \
    .option("writeMethod", "direct") \
    .option("createDisposition", "CREATE_IF_NEEDED") \
    .option("writeDisposition", "WRITE_APPEND") \
    .mode("append") \
    .save()

print(f"✓ Written {result_count} rows to BigQuery raw.vessel_pings_spark")
spark.stop()
