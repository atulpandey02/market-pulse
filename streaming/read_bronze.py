from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadBronze") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n=== BRONZE LAYER — stock_ticks ===\n")

df = spark.read.format("delta").load("/tmp/spark-data/delta/bronze/stock_ticks")

print(f"Total records: {df.count()}")
print(f"\nSchema:")
df.printSchema()
print(f"\nSample data:")
df.show(10, truncate=False)

spark.stop()
