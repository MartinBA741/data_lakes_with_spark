import configparser
from datetime import datetime
import os
from pyspark import SparkContext
from pyspark import conf
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''Create a spark session.'''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    ''' Process the song data from AWS S3 to song and artist tables.
        These are stored on AWS S3 again.
        Args:
        - spark (spark session object): a Spark session
        - input_data (string): directory containing the data used as input
        - output_data (string): a directory where the output tables should be stored'''

    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    songs_table = songs_table.dropDuplicates(['song_id'])

    # write songs table to parquet files partitioned by year and artist (Note: Parquet is a columnar format)
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'name', 'location', 'lattitude', 'longitude')
    artists_table = artists_table.dropDuplicates(['artist_id'])

    # write artists table to parquet files (Note: Parquet is a columnar format)
    artists_table.write.parquet(os.path.join(output_data, 'artists'))



def process_log_data(spark, input_data, output_data):
    '''Process log data from the AWS S3.
    Args:
        - spark (spark session object): a Spark session
        - input_data (string): directory containing the data used as input
        - output_data (string): a directory where the output tables should be stored'''

    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select('user_id', 'first_name', 'last_name', 'gender', 'level')
    users_table = users_table.dropDuplicates(['userId'])

    # write users table to parquet files (Note: Parquet is a columnar format)
    users_table.write.parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda dt: format_datetime(int(dt)), TimestampType())
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda dt: format_datetime(int(dt)), DateType())
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.withColumn("hour", hour("start_time"))           \
                    .withColumn("day", dayofmonth("start_time"))     \
                    .withColumn("week", weekofyear("start_time"))    \
                    .withColumn("month", month("start_time"))        \
                    .withColumn("year", year("start_time"))          \
                    .withColumn("weekday", dayofweek("start_time"))  \
                    .select("ts", "start_time", "hour", "day", "week", "month", "year", "weekday").drop_duplicates()
    
    # write time table to parquet files partitioned by year and month (Note: Parquet is a columnar format)
    time_table.write.partitionBy('year', 'month').mode('overwrite').parquet(os.path.join(output_data, 'time'))

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.artist == song_df.artist_name) & (df.title == song_df.song)) \
        .select(col("start_time"),
        col("userId").alias("user_id"),
        col("level"),
        col("song_id"),
        col("artist_id"),
        col("sessionId").alias('session_id'),
        col("artist_location").alias("location"),
        col("userAgent").alias("user_agent")
    )

    # write songplays table to parquet files partitioned by year and month (Note: Parquet is a columnar format)
    songplays_table.write.partitionBy('year', 'month').mode('overwrite').parquet(os.path.join(output_data, 'songplays'))


def main():
    '''Execute the previously defined functions.'''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://aws-logs-662261384119-us-east-1/elasticmapreduce/"
    
    # Local test
    #spark = SparkSession.builder.getOrCreate()
    #spark = SparkSession.builder.master("local[1]").appName('localTest').getOrCreate()
    #input_data = r"C:\Users\Ma-Bi\OneDrive\joyfulWorld\Data Engineering\Spark_data_lakes\data_lakes_with_spark\data"
    #output_data = r"C:\Users\Ma-Bi\OneDrive\joyfulWorld\Data Engineering\Spark_data_lakes\data_lakes_with_spark\data\outdata"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
