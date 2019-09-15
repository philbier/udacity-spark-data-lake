import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

# create access key pair on AWS an copy it to the dl.cfg file
os.environ['AWS_ACCESS_KEY_ID']=config['AWS_CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_CREDS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*"
    
    # read song data file
    print("Retrieving song data...")
    df = spark.read.json(song_data)

    # extract columns to create songs table
    df.createOrReplaceTempView("songs")

    songs_table = spark.sql("""
        SELECT     
            DISTINCT song_id,
            title,
            artist_id,
            year,
            duration
        FROM songs
    """)
    
    # write songs table to parquet files partitioned by year and artist
    print("Creating songs dimensional table...")
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "songs_table")

    # extract columns to create artists table
    df.createOrReplaceTempView("artists")
    
    artists_table = spark.sql("""
        SELECT     
            DISTINCT artist_id, 
            artist_name as name, 
            artist_location as location, 
            artist_latitude as latitude, 
            artist_longitude as longitude
        FROM artists
    """) 
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists_table")

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/"
    
    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where(df.page=="NextSong")
    
    df.createOrReplaceTempView("log_data")
    
    # extract columns for users table    
    users_table = spark.sql("""
        SELECT     
            DISTINCT userId as user_id, 
            firstName as first_name, 
            lastName as last_name, 
            gender, 
            level
        FROM log_data
    """)

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output + "users_table")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    df = df.withColumn('start_time', get_timestamp('ts'))
    
    # extract columns to create time table
    time_table = df.select(
        "start_time",
            hour("start_time").alias('hour'),
            dayofmonth("start_time").alias('day'),
            weekofyear("start_time").alias('week'),
            month("start_time").alias('month'),
            year("start_time").alias('year'),
            date_format("start_time","u").alias('weekday')
        ).distinct() 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output + "time_table")
    
    # read songs_table, artists_table and time_table to use it for songplays_table
    song_df = spark.read.parquet(output_data + "songs_table")
    song_df.createOrReplaceTempView("songs_table")

    artist_df = spark.read.parquet(output_data + "artists_table")
    artist_df.createOrReplaceTempView("artists_table")

    user_df = spark.read.parquet(output_data + "users_table")
    user_df.createOrReplaceTempView("users_table")

    time_df = spark.read.parquet(output_data + "time_table")
    time_df.createOrReplaceTempView("time_table")

    # create schema-on-read for table "songplays"
    df.createOrReplaceTempView("songplays_table")

    songplays_table = spark.sql("""
        SELECT     
            monotonically_increasing_id() as songplay_id, 
                time.start_time,
                users.user_id,
                users.level,
                songs.song_id,
                artists.artist_id,
                log.sessionId as session_id,
                log.location,
                log.userAgent as user_agent
        FROM log_data log
        INNER JOIN users_table users ON users.user_id = log.userId
        INNER JOIN time_table time ON time.start_time = log.start_time
        INNER JOIN artists_table artists ON artists.name = log.artist
        INNER JOIN songs_table songs ON songs.title = log.song
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").parquet(output + "songplays_table")


def main():
    spark = create_spark_session()

    ### replace path ###
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://aws-emr-resources-668194637820-us-east-2/data/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
