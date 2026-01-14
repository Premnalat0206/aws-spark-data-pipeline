import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType

def create_spark_session():
    return SparkSession.builder \
        .appName("WebAPI_Data_Extraction") \
        .getOrCreate()

def fetch_api_data(api_url):
    response = requests.get(api_url)
    response.raise_for_status()
    return response.json()

def create_dataframe(spark, api_data):
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("current_location", StringType(), True),
        StructField("last_updated", StringType(), True)
    ])

    return spark.createDataFrame(api_data, schema)

def transform_data(df):
    return df.filter(col("current_location").isNotNull())

def write_intermediate_data(df, output_path):
    df.write.mode("overwrite").parquet(output_path)

if __name__ == "__main__":
    spark = create_spark_session()

    api_url = "https://api.example.com/customer/location"
    output_path = "s3://intermediate-bucket/webapi_extracted/"

    api_data = fetch_api_data(api_url)
    api_df = create_dataframe(spark, api_data)
    transformed_df = transform_data(api_df)
    write_intermediate_data(transformed_df, output_path)

    spark.stop()
