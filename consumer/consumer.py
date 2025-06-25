import os
import json
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip

# define paths
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = "github_events"  # check with the producer
DELTA_PATH = "/delta/github_events"


# wait till Kafka is ready
def wait_for_kafka_consumer():
    retries = 10
    delay = 3
    for i in range(retries):
        try:
            print(f"Connecting to Kafka... try {i + 1}")
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='github-group'
            )
            print("Kafka Consumer connected.")
            return consumer
        except NoBrokersAvailable:
            print("Kafka not ready for consumer.")
            time.sleep(delay)
    raise RuntimeError("Failed to connect Kafka consumer.")


def create_spark_session():
    builder = (
        SparkSession.builder.appName("GitHubEventConsumer")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .master("local[*]")
    )

    # ensure spark is configured
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


if __name__ == "__main__":
    consumer = wait_for_kafka_consumer()
    spark = create_spark_session()
    print("Consumer started. Writing to Delta Lake...")

    for message in consumer:
        data = message.value
        print("data check: ", data)

        # Raw data
        raw_df = spark.createDataFrame([{"raw_json": json.dumps(data)}])
        raw_df.write.format("delta").mode("append").save("/delta/raw_github_events")
