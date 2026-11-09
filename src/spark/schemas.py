from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
)

STATE_SCHEMA = StructType([
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("origin_country", StringType()),
    StructField("time_position", LongType()),
    StructField("last_contact", LongType()),
    StructField("longitude", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("baro_altitude", FloatType()),
    StructField("on_ground", BooleanType()),
    StructField("velocity", FloatType()),
    StructField("true_track", FloatType()),
    StructField("vertical_rate", FloatType()),
    StructField("geo_altitude", FloatType()),
])
