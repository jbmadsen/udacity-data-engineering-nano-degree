# Sparkify Data Lakes with Spark

TODO


# Context 

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project, we will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. We deploy this Spark process on a cluster using AWS.


# Project Structure

## Main files used in the project:

|Filename|Description|
|---|---|
|[data](./assets/)|Data folder containing zipped song and log data|
|[dl.cfg](./dwh.cfg)|Config file containing AWS credentials (empty in this repo)|
|[etl.py](./etl.py)|Python script for the ETL process of reading data from S3, process that data using Spark, and writes the data back to S3|
|[README.md](./README.md)|This file, descriping the repository and the content|

## Support/Utility files used in the project:

|Filename|Description|
|---|---|
|[zip_workspace.ipynb](./zip_workspace.ipynb)|A supporting notebook containing code to compress this workspace into a single zip file for easy downloading the repository from the workspace|


# The rest

TODO
