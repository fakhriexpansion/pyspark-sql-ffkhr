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

## Running locally
Requires following dependencies: 
- Python (version 3.9 or above)
- Docker

To run python command, at least we will have two big schenarios. First, if we dont want to upload it into Google Cloud Storage (GCS), the python command would be look like below

```shell
python3 processing.py
```

However, if you want to upload it to GCS, you can run it with the following command. The bucket name and sa file should be following the name of your bucket and service account.

```shell
python3 processing.py --upload_gcp 1 --bucket_name "mxfaas-storage" --sa_file faas-kubernetes-creds.json
```

However, as you can see on the command and inside the file, the script default will not upload to GCS. So, you need to set some variables above to upload it into GCS.

## Running with docker locally

Currently, I set it to be run as default in dockerfile (not upload to GCS). For further development, the script can be run on top of k8s and can add the other argument seamlessly inside yaml file.

Run following commands to build Dockerfile and run the image
```shell
docker build -t <image-name> .
docker run -d <image-name>
```
I also give the log of my running in local (with `sleep infinity` for debugging purposes) that is attached on `additional_info/container_run.log`


## Running log
I provide two log here. One running container without upload to GCS and another one with upload to GCS. If you want to run it without upload to GCS, below shows the command

```shell
docker run -d <image-name>
```

However, if you want to run with upload to GCS, below shows the command

```shell
docker run -d <image-name> sleep infinity
python3 processing.py --upload_gcp 1 --bucket_name "mxfaas-storage" --sa_file faas-kubernetes-creds.json
```

For more detail, you can see the log I provide inside `additional_info` folder. The main differences of those log is `finish upload to GCS` statement when trying to upload it to GCS.

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
