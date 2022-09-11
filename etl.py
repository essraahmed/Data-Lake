import configparser
import pyspark.sql.functions as F
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.window import Window


#Read Configuration
config = configparser.ConfigParser()
config.read('dl.cfg')

#Read AWS Access key
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']

#Read AWS Secret key
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    
    """
    Description:
    Create SparkSession
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
    Read the song data and extract columns to create songs and artists tables.
    store data in parquet format
    
    Args:
    -spark: SparkSession
    -input_data: input data file path
    -output_data: output data path
    """
    
    # get filepath to song data file
    song_data = os.path.join(input_data + "song_data/A/A/A/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year","duration"]).where(df.song_id.isNotNull()).dropDuplicates()

    
    # write songs table to parquet files partitioned by year and artist
    #songs_table.write.partitionBy(['year', 'artist_id']).parquet(output_data + "songs/")
    songs_table.write.partitionBy(['year', 'artist_id']).parquet(output_data + "songs/",mode = "overwrite")

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude",                                                                       "artist_longitude"]).where(df.artist_id.isNotNull()).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists/")


def process_log_data(spark, input_data, output_data):
    
    """
    Read the log data and extract columns to create user, time and songplays tables
    store data in parquet format
    
    Args:
    -spark: SparkSession
    -input_data: input data file path
    -output_data: output data path
    """
    
    
    # get filepath to log data file
    log_data = os.path.join(input_data + "log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select(["userId", "firstName", "lastName", "gender", "level"]).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "users/")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:  datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df = df.withColumn("start_time", get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = (df.select("start_time") \
                   .withColumn("hour", hour("start_time")) \
                   .withColumn("day", dayofmonth("start_time")) \
                   .withColumn("week", weekofyear("start_time")) \
                   .withColumn("month", month("start_time")) \
                   .withColumn("year", year("start_time")) \
                   .withColumn("weekday", F.dayofweek("start_time"))).dropDuplicates()
   
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(['year', 'month']).parquet(output_data + "time/",mode = "overwrite")

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, "song_data/A/A/A/*.json")
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    df = df.join(song_df, (df.song == song_df.title) & (df.artist == song_df.artist_name) & (df.length == song_df.duration),'left')
    songplays_table = df.select(df.timestamp, 
                                col("userId").alias('user_id'),
                                df.level,
                                song_df.song_id,
                                song_df.artist_id,
                                col("sessionId").alias('session_id'),
                                df.location,
                                col("userAgent").alias("user_agent"),
                                year('start_time').alias('year'),
                                month('start_time').alias('month'))

    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(['year', 'month']).parquet(output_data + "songplays/",mode = "overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkifydata-output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
