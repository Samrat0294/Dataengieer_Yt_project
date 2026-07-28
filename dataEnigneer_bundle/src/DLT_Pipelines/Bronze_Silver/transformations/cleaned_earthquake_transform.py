from pyspark.sql.types import *
from pyspark.sql.functions import *
import dlt

catalog_name = spark.conf.get("catalog_name")
volume_path = f"/Volumes/{catalog_name}/bronze/earthquake_data_vol"
primary_key = "id"

properties_schema = StructType(
    [
        StructField("mag", StringType()),
        StructField("place", StringType()),
        StructField("time", StringType()),
        StructField("updated", StringType()),
        StructField("url", StringType()),
        StructField("detail", StringType()),
        StructField("felt", StringType()),
        StructField("cdi", StringType()),
        StructField("mmi", StringType()),
        StructField("alert", StringType()),
        StructField("status", StringType()),
        StructField("tsunami", StringType()),
        StructField("sig", StringType()),
        StructField("net", StringType()),
        StructField("code", StringType()),
        StructField("ids", StringType()),
        StructField("sources", StringType()),
        StructField("types", StringType()),
        StructField("nst", StringType()),
        StructField("dmin", StringType()),
        StructField("rms", StringType()),
        StructField("gap", StringType()),
        StructField("magType", StringType()),
        StructField("type", StringType()),
        StructField("title", StringType()),
    ]
)


geometry_schema = StructType(
    [
        StructField("coordinates", ArrayType(DoubleType()), True),
    ]
)

features_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("properties", properties_schema, True),
        StructField("geometry", geometry_schema, True),
    ]
)

schema = ArrayType(features_schema, True)


@dlt.view(name="earthquake_data_view")
def earthquake_data():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(volume_path)
        .withColumn("load_timestamp", current_timestamp())
    )
    df = df.withColumn("parsed_data", from_json(col("features"), schema))
    df = df.select(explode(col("parsed_data")).alias("features"), "load_timestamp") #explode(...): Takes a list or array of items in a column and makes a separate row for every single item.
    df = df.select(
        "features.properties.*",
        "features.id",
        col("features.geometry.coordinates")[0].alias("longitude"),
        col("features.geometry.coordinates")[1].alias("latitude"),
        col("features.geometry.coordinates")[2].alias("depth"),
        "load_timestamp"
    )
    df = (
        df.withColumn("time", from_unixtime(col("time") / 1000).cast("timestamp"))
        .withColumn("updated", from_unixtime(col("updated") / 1000).cast("timestamp"))
        .withColumn("mag", col("mag").cast("double"))
        .withColumn("sig", col("sig").cast("double"))
        .withColumn("nst", col("nst").cast("double"))
        .withColumn("rms", col("rms").cast("double"))
        .withColumn("gap", col("gap").cast("double"))
        .withColumn("felt", col("felt").cast("double"))
        .withColumn("tsunami", col("tsunami").cast("double"))
    )

    return df


dlt.create_streaming_table(name="earthquake_data_final", comment="SCD1 silver target table")

# When to Use apply_changes -> when you need to load data from a source streaming view or table into a target table while handling Change Data Capture (CDC) updates, deletes, and inserts.
# Merging updates: When your source data has updates to existing rows.
# Handling deletes: When rows are deleted from the source system.
# Building SCD tables: When creating Slowly Changing Dimension Type 1 (overwrite) or Type 2 (history tracking) tables.
# Ordering records: When data arrives out of order and needs sequencing.
dlt.apply_changes(
    target="earthquake_data_final",
    source="earthquake_data_view",
    keys=[primary_key],
    sequence_by="load_timestamp",
    stored_as_scd_type=1,
)
