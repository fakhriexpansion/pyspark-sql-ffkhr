# import library
import os
import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from google.cloud import storage

# Read CSV file
def read_csv_file(spark, directory): 
    dataframes = {}
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            df_name = filename[:-4]
            dataframes[df_name] = spark.read.csv(file_path, header=True, inferSchema=True)
    return dataframes

# function to help clean_processing below
# need to make string null into real null
def replace_null_strings(df):
    for col in df.columns:
        null_condition = (F.col(col) == F.lit("null")) | (F.col(col) == F.lit("Null"))
        df = df.withColumn(col, F.when(null_condition, F.lit(None)).otherwise(F.col(col)))
    return df

# Data cleaning & processing phase 1
def clean_processing(df):

    # Handling Null & dupliacates
    df = replace_null_strings(df)
    df = df.dropna()
    df = df.drop_duplicates()

    print("finish drop null and duplicates")

    # Detect timestamp columns
    timestamp_cols = [col for col, dtype in df.dtypes if dtype.startswith('timestamp')]

    # Convert timestamp columns to UTC+7
    for tkolom in timestamp_cols:
        df = df.withColumn(tkolom, F.from_utc_timestamp(F.to_utc_timestamp(tkolom, 'UTC'), 'Asia/Jakarta'))
    
    # String adjustment: initcap for all string columns except 'club_name'
    string_cols = [skol for skol, dtype in df.dtypes if dtype == 'string']
    for skol in string_cols:
        if skol != 'homeclub' or skol != 'transaction_location': # not initcap except that column
            df = df.withColumn(skol, F.initcap(skol))
    
    return df

# read sql file and Processing phase 2 by calling SQL
def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        sql_query = file.read()
    return sql_query

# upload to gcs
def upload_blob_to_gcs(bucket_name, local_file_name, gcs_blob_name, sa_file):
    storage_client = storage.Client.from_service_account_json(sa_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_blob_name)
    blob.upload_from_filename(local_file_name)
    print("finish upload to GCS")

# set main function with default not to upload into gcp
def main(upload_gcp=0, bucket_name="", sa_file=""):

    # Initialize Spark session
    spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

    # calling functions above and set table as temp view
    dataframes = read_csv_file(spark, directory="dataset/")

    for table in dataframes:
        dataframes[table] = clean_processing(dataframes[table])
        # print(f"====checking table {table}====")
        # dataframes[table].show()
        dataframes[table].createOrReplaceTempView(table)


    # Function to read SQL query from sql file
    sql_query = read_sql_file(file_path='query.sql')
    result_df = spark.sql(sql_query)

    # cast is_business_logic_valid into boolean type column
    result_df = result_df.withColumn(
        "is_business_logic_valid", F.col("is_business_logic_valid_int").cast("boolean")).drop("is_business_logic_valid_int")

    # filter & drop duplicate
    result_df = result_df.drop_duplicates(subset=["referrer_id"])
    # result_df = result_df.filter(
    #     (F.col("referrer_id") != "Null") & (F.col("referee_name") != "Null") & (F.col("transaction_id") != "Null")
    # )

    # generate output
    output_file = 'output.csv'
    result_df.toPandas().to_csv(output_file)

    if upload_gcp == 1 :
        # upload to gcs
        upload_blob_to_gcs(bucket_name=bucket_name, local_file_name=output_file, gcs_blob_name="fithub/output.csv", sa_file=sa_file)

    result_df.show()
    result_df.printSchema()

    # Stop spark session
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process argument GCP")
    parser.add_argument('--upload_gcp', type=int, choices=[0, 1], required=False, help='Enable (1) or disable (0) uploading to GCP')
    parser.add_argument('--bucket_name', type=str, required=False, help='bucket name on GCS. Mandatory if upload_gcp = 1')
    parser.add_argument('--sa_file', type=str, required=False, help='SA file name you have. Mandatory if upload_gcp = 1')


    args = parser.parse_args()
    main(bool(args.upload_gcp), args.bucket_name, args.sa_file)
