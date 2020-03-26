# Sparkify Data Lakes with Spark

TThis repository contains the project submission for the Udacity Data Engineering Nanodegree. The project introduces the following concepts:
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

TODO

## Setup Configurations

TODO

## Execute ETL

TODO

# ETL Pipeline

TODO