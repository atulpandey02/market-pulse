from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# CONFIG
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "stock.price.ticks"
CHECKPOINT_DIR = "/tmp/checkpoints/bronze_stream"
BRONZE_OUTPUT = "/tmp/delta/bronze/stock_ticks"

# SCHEMA
TICK_SCHEMA = StructType([
    StructField("symbol",    StringType(),  nullable=False),
    StructField("price",     DoubleType(),  nullable=False),
    StructField("volume",    IntegerType(), nullable=False),
    StructField("timestamp", StringType(),  nullable=False),
    StructField("event_id",  StringType(),  nullable=False),
])

# ── SPARK SESSION ─────────────────────────────────────────
class SparkStreamProcessor:
    """Manages Spark session lifecycle.
    Single responsibility: session creation and configuration.
    """

    def __init__(self, app_name: str):
        self.spark = (
            SparkSession.builder
            .appName(app_name)
            .master("spark://spark-master:7077")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "io.delta:delta-spark_2.12:3.0.0"
            )
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension"
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("WARN")
        print(f"Spark session created: {app_name}")

    def stop(self) -> None:
        self.spark.stop()
        print("Spark session stopped.")


# BRONZE STREAM
class BronzeStreamWriter :
    """Reads from Kafka, writes raw ticks to Delta Lake Bronze.
    Single responsibility: Bronze layer ingestion only.
    Immutable raw data — no transformations applied.
    """
    def __init__(self,spark: SparkSession):
        self.spark = spark

    def read_from_kafka(self):
        """Create streaming DataFrame from Kafka topic."""
        return(
            self.spark.readStream
            .format('kafka')
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )

    def parse_tick(self, kafka_df):
        """Parse raw Kafka bytes into structured tick DataFrame.
        Bronze principle: parse only, never transform.
        """
        return (
            kafka_df
            # Kafka value comes as bytes — cast to string first
            .withColumn("value_str", col("value").cast(StringType()))
            # Parse JSON string into structured columns
            .withColumn("tick", from_json(col("value_str"), TICK_SCHEMA))
            # Extract individual fields
            .select(
                col("tick.symbol").alias("symbol"),
                col("tick.price").alias("price"),
                col("tick.volume").alias("volume"),
                col("tick.timestamp").alias("source_timestamp"),
                col("tick.event_id").alias("event_id"),
                # Kafka metadata — always keep in Bronze
                col("partition").alias("kafka_partition"),
                col("offset").alias("kafka_offset"),
                # When this record landed in our pipeline
                current_timestamp().alias("ingestion_timestamp"),
            )
        )

    def write_to_bronze(self, parsed_df) -> None:
        """Write parsed ticks to Delta Lake Bronze layer.
        Append-only — Bronze is immutable.
        """
        query = (
            parsed_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT_DIR)
            .option("path", BRONZE_OUTPUT)
            # Trigger every 10 seconds — micro-batch interval
            .trigger(processingTime="10 seconds")
            .start()
        )

        print(f"Bronze stream started.")
        print(f"Reading from: {KAFKA_TOPIC}")
        print(f"Writing to:   {BRONZE_OUTPUT}")
        print(f"Checkpoint:   {CHECKPOINT_DIR}")
        print("Waiting for data...\n")

        # Block until stream is terminated
        query.awaitTermination()

# Pipeline
class BronzeIngestionPipeline:
    """Orchestrates the Bronze layer streaming ingestion.
    Single responsibility: coordination only.
    """

    def __init__(self, processor: SparkStreamProcessor):
        self.processor = processor
        self.writer = BronzeStreamWriter(processor.spark)

    def run(self) -> None:
        try:
            kafka_df = self.writer.read_from_kafka()
            parsed_df = self.writer.parse_tick(kafka_df)
            self.writer.write_to_bronze(parsed_df)
        except KeyboardInterrupt:
            print("\nShutdown signal received.")
        finally:
            self.processor.stop()
    
# Entry Point
if __name__ == "__main__":
    processor = SparkStreamProcessor(app_name="MarketPulse-Bronze-Stream")
    pipeline = BronzeIngestionPipeline(processor=processor)
    pipeline.run()



