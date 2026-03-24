from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, current_timestamp, col

# Start Spark session
spark = SparkSession.builder \
    .appName("DataGeneration") \
    .getOrCreate()

# Create 10 million rows
df = spark.range(0, 10000000)

# Add realistic columns
df = df.withColumn("amount", (rand() * 1000)) \
       .withColumn("category", (rand() * 10).cast("int")) \
       .withColumn("user_id", (rand() * 100000).cast("int")) \
       .withColumn("created_at", current_timestamp())

# Show sample
df.show(5)

# Save as Parquet (important)
df.write.mode("overwrite").parquet("D:/PREM/aws-spark-data-pipeline-main/aws-spark-data-pipeline-main/data/raw/")

print(" Data generation completed")
