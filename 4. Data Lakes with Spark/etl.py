import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DateType as Dat, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    song_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = df.select(song_columns).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + 'songs/')

    # extract columns to create artists table
    artist_columns = ['artist_id', 'artist_name as name', 'artist_location as location', 'artist_latitude as latitude', 'artist_longitude as longitude']
    artists_table = df.selectExpr(artist_columns).dropDuplicates() 
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong') 

    # extract columns for users table    
    user_columns = ['userId as user_id', 'firstName as first_name', 'lastName as last_name', 'gender', 'level']
    users_table = df.selectExpr(user_columns).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/')

    # create timestamp column from original timestamp column
    df = df.withColumn('timestamp', F.to_timestamp(F.from_unixtime((F.col('ts') / 1000) , 'yyyy-MM-dd HH:mm:ss.SSS')).cast('timestamp'))
    
    # create datetime column from original timestamp column
    df = df.withColumn('datetime', F.to_date(F.col('timestamp')))
    
    # extract columns to create time table
    time_table = (
        df.withColumn('hour', F.hour(F.col('timestamp')))
          .withColumn('day', F.dayofmonth(F.col('timestamp')))
          .withColumn('week', F.weekofyear(F.col('timestamp')))
          .withColumn('month', F.month(F.col('timestamp')))
          .withColumn('year', F.year(F.col('timestamp')))
          .withColumn('weekday', F.dayofweek(F.col('timestamp')))
          .select(
            F.col('timestamp').alias('start_time'),
            F.col('hour'),
            F.col('day'),
            F.col('week'),
            F.col('month'),
            F.col('year'),
            F.col('weekday')
          )
    )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time/')

    # read in song data to use for songplays table
    songs_df = spark.read.parquet(output_data + 'songs/*/*/*')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = (
        df.withColumn('songplay_id', F.monotonically_increasing_id())
          .join(songs_df, songs_df.title == df.song)
          .select(
            'songplay_id', 
            F.col('timestamp').alias('start_time'),
            F.col('userId').alias('user_id'),
            'level',
            'song_id',
            'artist_id',
            F.col('sessionId').alias('session_id'),
            'location',
            F.col('userAgent').alias('user_agent')
          )
    ) 

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + 'songplays/')


def main():
    spark = create_spark_session()
    input_data = 's3n://udacity-dend/'
    output_data = 's3n://sparkify-datalake/'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == '__main__':
    main()
