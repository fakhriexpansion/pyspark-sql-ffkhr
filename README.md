# Pyspark with SQL

## Key Features
- Collecting source file (csv) and read it seamlessly from folder dataset as pyspark dataframe `(processing.py)`
- Having SQL file for additional advance processing `(query.sql)`
- Show the data and write into `output.csv`

## List of table name
Here are the list of table name  available in my dataset according to https://dbdiagram.io/d/take-home-test-6694f17a9939893daef1260f which I put in the `dataset` folder that I dont show here

- lead_logs.csv (i rename it from lead_log.csv)
- paid_transactions.csv
- referral_rewards.csv
- user_logs.csv
- user_referral_statuses.csv
- user_referrals_logs.csv (i rename it from user_referral_logs.csv)
- user_referrals

I rename it just to make sure the name is similar with the diagram provided

## Running with python locally
Requires following dependencies: 
- Python (version 3.9 or above)
- Docker

To run python command, at least we will have two big schenarios. First, if we dont want to upload it into Google Cloud Storage (GCS), the python command would be look like below

```shell
python3 processing.py
```

However, if you want to upload it to GCS, you can run it with the following command. The bucket name and sa file should be following the name of your bucket and service account.

```shell
python3 processing.py --upload_gcp 1 --bucket_name bucket-storage --sa_file service_account.json
```

As you can see on the command and inside the file, the script default will not upload to GCS. So, you need to set some variables above to upload it into GCS. For more details, below i give explanation of the argument 

- `upload_gcp` = enable (1) or disable (0) uploading to Google Cloud Platform (GCP)
- `bucket_name` = bucket name on GCS. Mandatory if upload_gcp = 1
- `sa_file` = SA file name you have. Mandatory if upload_gcp = 1

If you dont have GCP access, please just run it with default as it will throw authentication error.

If you want to run it locally with python, please make sure to install python package dependencies written on `requirements.txt`. But, the best practices run it with docker to meet similar dependencies.


## Running with docker locally

Run following commands to build Dockerfile and run the image
```shell
docker build -t <image-name> .
docker run -d <image-name>
```
I also give the log of my docker running in local. Kindly check `Running log` below

## Running log
I provide two log here. One running container without upload to GCS and another one with upload to GCS. If you want to run it without upload to GCS, below shows the command

```shell
docker run -d <image-name>
```

However, if you want to run with upload to GCS, below shows the command. Note that this step is not ideal for production purposes. This only shows you that my code also works to upload into GCP by make the container keep alive.

```shell
docker run -d <image-name> sleep infinity
python3 processing.py --upload_gcp 1 --bucket_name mxfaas-storage --sa_file faas-kubernetes-creds.json
```

For more detail, you can see the log I provide inside `additional_info` folder. The main differences of those log is `finish upload to GCS` statement when trying to upload it to GCS.

For further development, the script can be run on top of k8s and can add the other argument seamlessly inside yaml file.

## Important notes
There are some important notes regarding this repo: 
- I hide the dataset as it is privacy
- I hide my json service account (SA) files. So you will got error while running my code by setting `upload_gcp` equals 1, unless you have your own SA.
- There are some additional things on `additional_info` folder, like screenshot on GCS and data_dictionary files to explain the result (output.csv) on excel format.
- Query explanation also available on `additional_info` folder to give detail explanation related to the query
`(query_sql.txt)`

## Upload to Google Storage

Go to `IAM and admin -> Service Accounts` in GCP to generate json file that will be used as service account to authenticate our action to write in google cloud storage. However my json is hidden here.

The screenshot after success upload to GCS can be seen on `additional_info/gcs_screenshot.png` above

Lastly, dont forget to create your own bucket on GCS.
