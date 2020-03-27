# Sparkify Data Lakes with Spark

This repository contains the project submission for the Udacity Data Engineering Nanodegree. The project introduces the following concepts:
* Data modeling with [Amazon EMR Clusters](https://aws.amazon.com/emr/)
* Building an ETL pipeline using [Python](https://www.python.org/)
* Loading data for processing and saving processed data to [Amazon S3](https://aws.amazon.com/S3/) 


# Context 

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project, we will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. We deploy this Spark process on an AWS EMR cluster.


# Project Structure

## Main files used in the project:

|Filename|Description|
|---|---|
|[configs.py](./configs.py)| Python script that loads config files contents and exposes these |
|[dl.cfg](./dl.cfg)| Config file containing AWS datalake configurations |
|[etl.py](./etl.py)| Python script for the ETL process of reading data from S3, process that data using Spark, and writes the data back to S3 |
|[iac-submit-etl.py](./iac-submit-etl.py)| Python script that uploads the [etl.py](./etl.py) file and supporting files to S3, creates a new job step flow for the running EMR cluster, and starts processing [etl.py](./etl.py) on the remote EMR cluster |
|[keys.cfg](./keys.cfg)| Config file containing AWS key and secret credentials (empty in this repo) |
|[README.md](./README.md)| This file, descriping the repository and the content |

## Support/Utility files used in the project:

|Filename|Description|
|---|---|
|[data](./data/)| Data folder containing zipped song and log data for local testing of [etl.py](./etl.py) |
|[demo](./demo/)| Folder containing demo code from the study material presented by Udacity |
|[exploration](./exploration/)| Jupyter Notebooks for exploration of project, including notebook for running local etl job |
|[get-cluster-status.py](./get-cluster-status.py)| Supporting Python script that queries current status of EMR clusters on AWS using boto3, to quickly identify status of clusters. Useful when starting new clusters and remembering to terminate existing clusters |
|[iac-create.py](./iac-create.py)| Supporting Python script creates an EMR cluster from code. This makes it easy to quickly spin up a new cluster infrastructure without having to go through the AWS GUI |
|[iac-terminate.py](./iac-terminate.py)| Supporting Python script terminates all running EMR clusters from code. Quite handy to avoid unnecessary costs when forgetting to stop an EMR cluster |
|[unpack-local.sh](./unpack-local.sh)| Supporting bash script that extracts data files in data folder to a folder structure resembling that of the production data on S3 for testing etl.py locally |
|[zip_workspace.ipynb](./zip_workspace.ipynb)|A supporting notebook containing code to compress this workspace into a single zip file for easy downloading the repository from the workspace|


# How to run 

## Execute locally

### Setup Configurations 

1) Make sure you have a local Spark installation running, and python dependencies installed
2) Unpack the data in the [data/](./data/) folder using [unpack-local.sh](./unpack-local.sh)
3) Fill in ```S3_BUCKET_IN``` and ```S3_BUCKET_OUT``` in [dl.cfg](./dl.cfg) with:
    * S3_BUCKET_IN  = ./data/
    * S3_BUCKET_OUT = ./output/

### Running

1) Run ```python etl.py``` in the terminal
2) Output is saved to the selected S3_BUCKET_OUT location

## Execute ETL on AWS EMR Cluster

### Setup Configurations 

1) Fill in everything in [dl.cfg](./dl.cfg), as well as your AWS key and secret in [keys.cfg](./keys.cfg).
2) Create an EMR cluster, either through [AWS GUI](https://console.aws.amazon.com/elasticmapreduce/) or by executing ```python iac-create.py``` in the terminal

### Running

1) Once the cluster is up and running (you can verify through ```python get-cluster-status.py```), you submit [etl.py](./etl.py) to the cluster using ```python iac-submit-etl.py```
    * This checks for a running cluster, uploads the needed files to S3, and adds the ETL process to the cluster as a job flow on the cluster
    * Output of the ETL process is saved to the selected S3_BUCKET_OUT location
2) [Optional]: You can terminate any running EMR cluster using ```python iac-terminate.py``` to make sure you don't get any unexpected bills

# ETL Pipeline

## Loading data from S3

The ETL process reads song and log data from S3, using the specifiers: 

* Song data: ```S3_BUCKET_IN/song_data/*/*/*/*.json```
* Log data: ```S3_BUCKET_IN/log_data/*/*/*.json```

## Data Processing

Data loaded from S3 is processed and transformed into five main Fact and Dimensional tables:

| Table | Columns |
| --- | --- |
| songplays | *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent* |
| users | *user_id, first_name, last_name, gender, level* |
| songs | *song_id, title, artist_id, year, duration* |
| artists | *artist_id, name, location, lattitude, longitude* |
| time | *start_time, hour, day, week, month, year, weekday* |

## Loading data to S3

Each of the five tables are loaded back to S3 as parquet files to individual folders per table: /songplays/, /users/, /songs/, /artists/, and /time/, in the S3 bucket ```S3_BUCKET_OUT```.

