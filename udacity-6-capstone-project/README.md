# Data Engineering Capstone Project

## Overview
The purpose of the data engineering capstone project is to combine what I've learned throughout the program. In this project, I can define the scope and data for a project of my own design. I am expected to go through the same steps outlined below.

## Instructions

### Step 1: Scope the Project and Gather Data
Since the scope of the project will be highly dependent on the data, these two things happen simultaneously. In this step:

* Identify and gather the data used for the project (at least two sources and more than 1 million rows).
* Explain what end use cases the data should be prepared for (e.g., analytics table, app back-end, source-of-truth database, etc.)

### Step 2: Explore and Assess the Data
* Explore the data to identify data quality issues, like missing values, duplicate data, etc.
* Document steps necessary to clean the data

### Step 3: Define the Data Model
* Map out the conceptual data model and explain why that model was chosen
* List the steps necessary to pipeline the data into the chosen data model

### Step 4: Run ETL to Model the Data
* Create the data pipelines and the data model
* Include a data dictionary
* Run data quality checks to ensure the pipeline ran as expected
* Integrity constraints on the relational database (e.g., unique key, data type, etc.)
* Unit tests for the scripts to ensure they are doing the right thing
* Source/count checks to ensure completeness

### Step 5: Complete Project Write Up
* What's the goal? What queries will be run? How would Spark or Airflow be incorporated? 
* Clearly state the rationale for the choice of tools and technologies for the project.
* Document the steps of the process.
* Propose how often the data should be updated and why.
* Post the write-up and final data model in a GitHub repo.
* Include a description of how to approach the problem differently under the following scenarios:
    * If the data was increased by 100x.
    * If the pipelines were run on a daily basis by 7am.
    * If the database needed to be accessed by 100+ people.
