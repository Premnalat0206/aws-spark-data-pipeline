from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, current_timestamp

# Start Spark session (FIXED CONFIG)
spark = SparkSession.builder \
    .appName("DataGeneration") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

print("SPARK STARTED")

# Create data (start small)
df = spark.range(0, 100000)   # 100K rows (safe)

# Add columns
df = df.withColumn("amount", (rand() * 1000)) \
       .withColumn("category", (rand() * 10).cast("int")) \
       .withColumn("user_id", (rand() * 100000).cast("int")) \
       .withColumn("created_at", current_timestamp())

print("BEFORE SHOW")
df.show(5)

print("BEFORE WRITE")

# Write to absolute path (IMPORTANT)
output_path = "D:/PREM/aws-spark-data-pipeline-main/aws-spark-data-pipeline-main/data/raw/"

df.write.mode("overwrite").parquet(output_path)

print("AFTER WRITE")

# Stop Spark (clean shutdown)
spark.stop()

print("JOB COMPLETED")
