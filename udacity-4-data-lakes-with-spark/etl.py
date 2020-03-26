import os
import pyspark.sql as Spark
import pyspark.sql.functions as F
import pyspark.sql.types as T
from datetime import datetime
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
    df = spark.read.json(song_data, 
                         columnNameOfCorruptRecord='corrupt_record').drop_duplicates()

    # extract columns to create songs table
    songs_table = df.select(df.song_id,
                            df.title,
                            df.artist_id,
                            df.year,
                            df.duration).drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs/", 
                              mode="overwrite",
                              partitionBy=["year","artist_id"])

    # extract columns to create artists table
    artists_table = df.select(df.artist_id,
                              df.artist_name.alias("name"),
                              df.artist_location.alias("location"),
                              df.artist_latitude.alias("latitude"),
                              df.artist_longitude.alias("longitude")).drop_duplicates()

    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/", 
                                mode="overwrite")


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
    df = spark.read.json(log_data, 
                         columnNameOfCorruptRecord='corrupt_record').drop_duplicates()
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select(df.userId.alias("user_id"),
                            df.firstName.alias("first_name"),
                            df.lastName.alias("last_name"),
                            df.gender,
                            df.level).drop_duplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users/", 
                              mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), T.TimestampType())
    df = df.withColumn("start_time", get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    # get_datetime = F.udf()
    # df = 
    
    # extract columns to create time table
    time_table = df.withColumn("start_time", F.col("start_time")) \
                   .withColumn("hour", F.hour(F.col("start_time"))) \
                   .withColumn("day", F.dayofmonth(F.col("start_time"))) \
                   .withColumn("week", F.weekofyear(F.col("start_time"))) \
                   .withColumn("month", F.month(F.col("start_time"))) \
                   .withColumn("year", F.year(F.col("start_time"))) \
                   .withColumn("weekday", F.dayofweek(F.col("start_time"))) \
                   .select("ts","start_time","hour", "day", "week", "month", "year", "weekday").drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "time/", 
                            mode="overwrite",
                            partitionBy=["year","month"])

    # read in song data to use for songplays table
    songs_parquet = output_data + 'songs/*/*/*.parquet'
    song_df = spark.read.parquet(songs_parquet)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, [df.song == song_df.title], how='inner') \
                        .join(time_table, df.start_time == time_table.start_time, how="inner") \
                        .select(F.monotonically_increasing_id().alias("songplay_id"),
                                df.start_time,
                                df.userId.alias("user_id"),
                                df.level,
                                song_df.song_id,
                                df.artist.alias("artist_id"), 
                                df.sessionId.alias("session_id"), 
                                df.location, 
                                df.userAgent.alias("user_agent"),
                                time_table.year,
                                time_table.month) \
                        .repartition("year", "month") \
                        .drop_duplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays/", 
                                  mode="overwrite",
                                  partitionBy=["year","month"])


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

    os.environ['AWS_ACCESS_KEY_ID'] = KEY
    os.environ['AWS_SECRET_ACCESS_KEY'] = SECRET
    
    main()
