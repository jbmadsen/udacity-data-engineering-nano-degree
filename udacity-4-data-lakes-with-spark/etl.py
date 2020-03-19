import configparser
from datetime import datetime
import os
import pyspark.sql as Spark
import pyspark.sql.functions as F
import pyspark.sql.types as T
#from pyspark.sql.functions import udf, col
#from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from configs import KEY, SECRET, S3_BUCKET_OUT


def create_spark_session():
    """
    Creates a spark session, or retrieve a matching one if it exists
    """
    spark = Spark.SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Loads data from S3, processes it to songs and artist tables, which are saved back to S3
    
    Input:
    spark:          A SparkSession instance
    input_data:     location of the json files for processing
    output_data:    S3 bucket for outputting dimensional data in parquet format
    """
    # get filepath to song data file
    # Documentation example from Udacity: song_data/A/B/C/TRABCEI128F424C983.json
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = 

    # extract columns to create songs table
    songs_table = 
    
    # write songs table to parquet files partitioned by year and artist
    songs_table

    # extract columns to create artists table
    artists_table = 
    
    # write artists table to parquet files
    artists_table


def process_log_data(spark, input_data, output_data):
    """
    Loads data from S3, processes it to event tables, which are saved back to S3
    
    Input:
    spark:          A SparkSession instance
    input_data:     location of the json files for processing
    output_data:    S3 bucket for outputting dimensional data in parquet format
    """
    # get filepath to log data file
    # Documentation example from Udacity: log_data/2018/11/2018-11-12-events.json
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = 
    
    # filter by actions for song plays
    df = 

    # extract columns for users table    
    artists_table = 
    
    # write users table to parquet files
    artists_table

    # create timestamp column from original timestamp column
    get_timestamp = F.udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = F.udf()
    df = 
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    """
    Loads songs and event data from S3, 
    transforms this data to dimensional tables format, 
    and saves it back to S3 in Parquet format
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = S3_BUCKET_OUT
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    """
    Runs main function for this module: sets access credentials and calls main() function
    """
    config = configparser.ConfigParser()
    config.read('./dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID'] = KEY
    os.environ['AWS_SECRET_ACCESS_KEY'] = SECRET
    
    main()
