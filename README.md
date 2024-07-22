# Pyspark with SQL

## Key Features
- Collecting source file (csv) and read it seamlessly from folder dataset as pyspark dataframe `(processing.py)`
- Having SQL file for additional advance processing `(query.sql)`
- Show the data and write into `output.csv`

## Running locally
Requires following dependencies: 
- Python (version 3.9 or above)
- Docker

Run following commands to build Dockerfile
```shell
docker build -t <image-name> .
```

## Important notes
I hide the dataset as it is privacy

There are some additional things on `additional_info` folder, like screenshot on GCS and data_dictionary files to explain the result (output.csv) on excel format.

## Upload to Google Storage

Requirement : 
- gcloud : https://cloud.google.com/sdk/docs/install

Run the following command to initialize gcloud CLI
```shell
#initialize gcloud CLI
gcloud init

#switch to intended project
gcloud config set project <project-name>

```
Go to `IAM and admin -> Service Accounts` in GCP to generate json file that will be used as service account to authenticate our action to write in google cloud storage


The screenshot after success upload to GCS can be seen on `additional_info/gcs_screenshot.png` above
