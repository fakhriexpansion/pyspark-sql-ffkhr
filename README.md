# Pyspark with SQL

## Key Features
- Collecting source file (csv) and read it seamlessly from folder dataset as pyspark dataframe `(processing.py)`
- Having SQL file for additional advance processing `(query.sql)`
- Show the data and write into `output.csv`

## Running locally
Requires following dependencies: 
- Python (version 3.9 or above)
- Docker

Run following commands to build Dockerfile and run the image
```shell
docker build -t <image-name> .
docker run -d <image-name>
```
I also give the log of my running in local (with `sleep infinity` for debugging purposes) that is attached on `additional_info/container_run.log`

## Important notes
There are some important notes regarding this repo: 
- I hide the dataset as it is privacy
- I hide my json service account (SA) files. So you will got error while running my code, unless you have your own SA.
- There are some additional things on `additional_info` folder, like screenshot on GCS and data_dictionary files to explain the result (output.csv) on excel format.

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
Go to `IAM and admin -> Service Accounts` in GCP to generate json file that will be used as service account to authenticate our action to write in google cloud storage. However my json is hidden here.

The screenshot after success upload to GCS can be seen on `additional_info/gcs_screenshot.png` above
