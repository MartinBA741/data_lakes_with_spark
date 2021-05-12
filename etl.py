import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''Create a spark session.'''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    ''' Process the song data from AWS S3 to song and artist tables.
        These are stored on AWS S3 again.'''

    # get filepath to song data file
    song_data = 's3://udacity-dend/song_data/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = 

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'name', 'location', 'lattitude', 'longitude')
    
    # write artists table to parquet files
    artists_table = 



def process_log_data(spark, input_data, output_data):
    '''Process log data from the AWS S3.'''

    # get filepath to log data file
    log_data = 's3://udacity-dend/log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = 

    # extract columns for users table    
    #artists_table
    users_table = df.select('user_id', 'first_name', 'last_name', 'gender', 'level')
    
    # write users table to parquet files
    #artists_table =
    users_table = 

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = df.select('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    
    # write time table to parquet files partitioned by year and month
    time_table  =

    # read in song data to use for songplays table
    song_df = spark.read.parquet('output/songs/year=*/artist_name=*/*.parquet')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.select('songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent') 
    #               + song_df.select('song_id', 'title', 'artist_id', 'year', 'duration')
                    WHERE page='NextSong'

    # write songplays table to parquet files partitioned by year and month
    songplays_table = 


def main():
    '''Execute the previously defined functions.'''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
