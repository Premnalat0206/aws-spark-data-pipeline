from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def create_spark_session():
    return SparkSession.builder \
        .appName("Master_Data_Aggregation_Job") \
        .getOrCreate()

def read_intermediate_data(spark):
    s3_df = spark.read.parquet(
        "s3://intermediate-bucket/s3_extracted/"
    )
    snowflake_df = spark.read.parquet(
        "s3://intermediate-bucket/snowflake_extracted/"
    )
    webapi_df = spark.read.parquet(
        "s3://intermediate-bucket/webapi_extracted/"
    )
    return s3_df, snowflake_df, webapi_df

def transform_data(s3_df, snowflake_df, webapi_df):
    df = s3_df.join(
        snowflake_df,
        "customer_id",
        "left"
    ).join(
        webapi_df,
        "customer_id",
        "left"
    )

    final_df = df.filter(
        col("location").isNotNull()
    )

    return final_df

def write_final_data(df):
    df.write.mode("overwrite").parquet(
        "s3://final-bucket/customer_master/"
    )

def write_to_opensearch(df):
    # Placeholder for OpenSearch integration
    # Actual implementation depends on OpenSearch cluster config
    pass

if __name__ == "__main__":
    spark = create_spark_session()

    s3_df, snowflake_df, webapi_df = read_intermediate_data(spark)
    final_df = transform_data(s3_df, snowflake_df, webapi_df)

    write_final_data(final_df)
    write_to_opensearch(final_df)

    spark.stop()
