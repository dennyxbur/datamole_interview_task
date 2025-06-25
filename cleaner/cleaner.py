from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StringType, BooleanType, TimestampType, IntegerType, MapType
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# Define paths
RAW_PATH = "/delta/raw_github_events"
CLEANED_PATH = "/delta/clean_github_events"
CHECKPOINT_PATH = "/delta/_checkpoints/cleaner"


# spark init
def create_spark_session():
    builder = (
        SparkSession.builder.appName("GitHubEventCleaner")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .master("local[*]")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


# Define schema of GitHub event JSON
def get_event_schema():
    return StructType().add("id", StringType()) \
        .add("type", StringType()) \
        .add("actor", MapType(StringType(), StringType())) \
        .add("repo", MapType(StringType(), StringType())) \
        .add("payload", MapType(StringType(), StringType())) \
        .add("public", BooleanType()) \
        .add("created_at", StringType())  # StringType due to some weird data formats for this field. Trnsformed later


# ensure table's been created
def initialize_cleaned_table(spark, event_schema):
    if not DeltaTable.isDeltaTable(spark, CLEANED_PATH):
        print("🛠 Initializing clean Delta table with inferred schema...")
        df_raw = spark.read.format("delta").load(RAW_PATH)
        df_sample = df_raw.select("raw_json") \
            .withColumn("json", from_json(col("raw_json"), event_schema)) \
            .select("json.*") \
            .filter(col("type").isin(["WatchEvent", "PullRequestEvent", "IssuesEvent"]))

        df_sample.limit(1).write.format("delta").mode("overwrite").save(CLEANED_PATH)
        print("Initialized cleaned Delta table.")
    else:
        print("Clean Delta table already exists.")


# main function of cleaner
def start_cleaning_stream(spark, event_schema):
    print("Starting cleaning stream...")

    df_stream = spark.readStream.format("delta").load(RAW_PATH)

    df_clean = df_stream.select("raw_json") \
        .withColumn("json", from_json(col("raw_json"), event_schema)) \
        .select("json.*") \
        .filter(col("type").isin(["WatchEvent", "PullRequestEvent", "IssuesEvent"])) \
        .withColumn("created_at_ts", to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ssX")) \

    # drop duplicates with watermark - across batches
    df_deduplicated = df_clean\
        .withWatermark("created_at_ts", "1 hour")\
        .drop_duplicates(["id"])

    # write result df
    df_deduplicated.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .option("mergeSchema", "true") \
        .start(CLEANED_PATH) \
        .awaitTermination()


if __name__ == "__main__":
    spark = create_spark_session()
    event_schema = get_event_schema()
    initialize_cleaned_table(spark, event_schema)
    start_cleaning_stream(spark, event_schema)
