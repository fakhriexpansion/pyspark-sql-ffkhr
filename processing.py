# import library
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_utc_timestamp, from_utc_timestamp, initcap
from google.cloud import storage

# Initialize Spark session
spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

# Read CSV file
def read_csv_file(spark, directory): 
    dataframes = {}
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            df_name = filename[:-4]
            dataframes[df_name] = spark.read.csv(file_path, header=True, inferSchema=True)
    return dataframes

# Data cleaning & processing phase 1
def clean_processing(df):
    # Handling Null & dupliacates
    df = df.dropna()
    df = df.drop_duplicates()

    # Detect timestamp columns
    timestamp_cols = [col for col, dtype in df.dtypes if dtype.startswith('timestamp')]

    # Convert timestamp columns to UTC+7
    for col in timestamp_cols:
        df = df.withColumn(col, from_utc_timestamp(to_utc_timestamp(col, 'UTC'), 'Asia/Jakarta'))
    
    # String adjustment: initcap for all string columns except 'club_name'
    string_cols = [col for col, dtype in df.dtypes if dtype == 'string']
    for col in string_cols:
        if col != 'homeclub' or col != 'transaction_location': # not initcap except that column
            df = df.withColumn(col, initcap(col))
    
    return df

# read sql file and Processing phase 2 by calling SQL
def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        sql_query = file.read()
    return sql_query

# upload to gcs
def upload_blob_to_gcs(bucket_name, local_file_name, gcs_blob_name):
    storage_client = storage.Client.from_service_account_json("faas-kubernetes-creds.json")
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_blob_name)
    blob.upload_from_filename(local_file_name)
    print("finish upload to GCS")

# calling functions above and set table as temp view
dataframes = read_csv_file(spark, directory="dataset/")
for table in dataframes:
    dataframes[table] = clean_processing(dataframes[table])
    dataframes[table].createOrReplaceTempView(table)


# Function to read SQL query from sql file
sql_query = read_sql_file(file_path='query.sql')
result_df = spark.sql(sql_query)

# filter & drop duplicate
result_df = result_df.drop_duplicates(subset=["referrer_id"])
result_df = result_df.filter(
    (col("referrer_id") != "Null") & (col("referee_name") != "Null") & (col("transaction_id") != "Null")
)

# generate output
result_df.toPandas().to_csv('output.csv')

# upload to gcs
upload_blob_to_gcs(bucket_name="mxfaas-storage", local_file_name="output.csv", gcs_blob_name="fithub/output.csv")

result_df.show()

# Stop spark session
spark.stop()