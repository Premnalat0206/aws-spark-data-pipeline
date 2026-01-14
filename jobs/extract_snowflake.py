from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def create_spark_session():
    return SparkSession.builder \
        .appName("Snowflake_Data_Extraction") \
        .getOrCreate()

def read_snowflake_table(spark, table_name, sf_options):
    return spark.read \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", table_name) \
        .load()

def transform_data(customers_df, purchases_df, violations_df):
    df = customers_df.join(
        purchases_df,
        customers_df.customer_id == purchases_df.customer_id,
        "left"
    ).join(
        violations_df,
        customers_df.customer_id == violations_df.customer_id,
        "left"
    )

    df_final = df.select(
        customers_df.customer_id,
        customers_df.customer_name,
        purchases_df.purchase_amount,
        violations_df.violation_type
    )

    return df_final

def write_intermediate_data(df, output_path):
    df.write.mode("overwrite").parquet(output_path)

if __name__ == "__main__":
    spark = create_spark_session()

    sf_options = {
        "sfURL": "account.snowflakecomputing.com",
        "sfUser": "SF_USER",
        "sfPassword": "FETCHED_FROM_SECRETS_MANAGER",
        "sfDatabase": "CUSTOMER_DB",
        "sfSchema": "PUBLIC",
        "sfWarehouse": "COMPUTE_WH"
    }

    customers_df = read_snowflake_table(
        spark, "CUSTOMERS", sf_options
    )
    purchases_df = read_snowflake_table(
        spark, "PURCHASES", sf_options
    )
    violations_df = read_snowflake_table(
        spark, "VIOLATIONS", sf_options
    )

    final_df = transform_data(
        customers_df, purchases_df, violations_df
    )

    output_path = "s3://intermediate-bucket/snowflake_extracted/"
    write_intermediate_data(final_df, output_path)

    spark.stop()
