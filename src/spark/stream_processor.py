import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.spark.schemas import STATE_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flight-raw-stream")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "default")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/checkpoints")

JDBC_URL = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}"
JDBC_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"

spark = SparkSession.builder \
    .appName("flight-stream") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# read raw bytes from kafka
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# parse json value
df = raw.select(
    F.from_json(F.col("value").cast("string"), STATE_SCHEMA).alias("d")
).select("d.*")

# drop rows with no position fix
df = df.filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())

# fill callsign and on_ground only — nulls in velocity/altitude/vertical_rate are real
df = df.withColumn(
    "callsign",
    F.when(F.col("callsign").isNull() | (F.trim(F.col("callsign")) == ""), "unknown")
    .otherwise(F.trim(F.col("callsign")))
)
df = df.withColumn("on_ground", F.coalesce(F.col("on_ground"), F.lit(False)))

# prefer time_position, fall back to last_contact
df = df.withColumn(
    "event_time",
    F.coalesce(F.col("time_position"), F.col("last_contact")).cast("timestamp")
)

# watermark before dropDuplicates — spark needs it to know when to evict dedup state
clean = (
    df.withWatermark("event_time", "10 minutes")
    .dropDuplicates(["icao24", "event_time"])
)

# 5-min tumbling window — nulls in velocity/altitude are skipped by avg/min/max
agg = clean.groupBy(
    F.window("event_time", "5 minutes")
).agg(
    F.countDistinct("icao24").alias("active_aircraft"),
    F.avg("velocity").alias("avg_velocity"),
    F.avg("baro_altitude").alias("avg_baro_altitude"),
    F.min("velocity").alias("min_velocity"),
    F.max("velocity").alias("max_velocity"),
)


def write_states(batch_df, batch_id):
    # cast on_ground bool -> int for clickhouse UInt8 and ensure exact column order
    rows = batch_df \
        .withColumn("on_ground", F.col("on_ground").cast("int")) \
        .select(
            "icao24",
            "callsign",
            "origin_country",
            "time_position",
            "last_contact",
            "longitude",
            "latitude",
            "baro_altitude",
            "on_ground",
            "velocity",
            "true_track",
            "vertical_rate",
            "geo_altitude",
            "event_time",
        )
    n = rows.count()
    rows.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "flight_states") \
        .option("driver", JDBC_DRIVER) \
        .mode("append") \
        .save()
    logger.info(f"wrote {n} states")


def write_agg(batch_df, batch_id):
    rows = batch_df \
        .withColumn("window_start", F.col("window.start")) \
        .withColumn("window_end", F.col("window.end")) \
        .withColumn("active_aircraft", F.col("active_aircraft").cast("int")) \
        .withColumn("avg_velocity", F.col("avg_velocity").cast("float")) \
        .withColumn("avg_baro_altitude", F.col("avg_baro_altitude").cast("float")) \
        .withColumn("min_velocity", F.col("min_velocity").cast("float")) \
        .withColumn("max_velocity", F.col("max_velocity").cast("float")) \
        .withColumn("updated_at", F.current_timestamp()) \
        .select(
            "window_start",
            "window_end",
            "active_aircraft",
            "avg_velocity",
            "avg_baro_altitude",
            "min_velocity",
            "max_velocity",
            "updated_at",
        )
    rows.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "flight_analytics_5min") \
        .option("driver", JDBC_DRIVER) \
        .mode("append") \
        .save()
    logger.info(f"wrote agg batch {batch_id}")


states_query = (
    clean.writeStream
    .foreachBatch(write_states)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/states")
    .start()
)

agg_query = (
    agg.writeStream
    .outputMode("update")
    .foreachBatch(write_agg)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/agg")
    .start()
)

logger.info("streaming started")
spark.streams.awaitAnyTermination()
