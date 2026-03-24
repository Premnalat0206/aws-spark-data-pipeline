from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, current_timestamp, col

# Start Spark session
spark = SparkSession.builder \
    .appName("DataGeneration") \
    .getOrCreate()

# Create 10 million rows
# df = spark.range(0, 10000000)
df = spark.range(0, 1000000)  # 1 million first

# Add realistic columns
df = df.withColumn("amount", (rand() * 1000)) \
       .withColumn("category", (rand() * 10).cast("int")) \
       .withColumn("user_id", (rand() * 100000).cast("int")) \
       .withColumn("created_at", current_timestamp())

print("Before show")
df.show(5)

print("Before write")
df.write.mode("overwrite").parquet("D:/PREM/aws-spark-data-pipeline-main/aws-spark-data-pipeline-main/data/raw/")
print("After write")
