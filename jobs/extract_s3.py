from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def create_spark_session():
    spark = SparkSession.builder \
        .appName("S3_Data_Extraction") \
        .getOrCreate()
    return spark

def read_s3_data(spark, input_path):
    return spark.read.parquet(input_path)

def transform_data(df):
    df_filtered = df.filter(col("status") == "ACTIVE")
    df_selected = df_filtered.select(
        "customer_id",
        "location",
        "updated_date"
    )
    return df_selected

def write_intermediate_data(df, output_path):
    df.write.mode("overwrite").parquet(output_path)

if __name__ == "__main__":
    spark = create_spark_session()

    input_path = "s3://source-bucket/customer_migration/"
    output_path = "s3://intermediate-bucket/s3_extracted/"

    source_df = read_s3_data(spark, input_path)
    transformed_df = transform_data(source_df)
    write_intermediate_data(transformed_df, output_path)

    spark.stop()
