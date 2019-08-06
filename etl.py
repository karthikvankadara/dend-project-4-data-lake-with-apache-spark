import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as typ

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Processing of song files
    
    Purpose of this funciton is to loop in all the files from data/song_data folder
    using which the following tables are populated -
    
    dim_songs
    dim_artists
    
    Parameters
    ----------
    spark : object
        Connection object passed on from the main function to perform the 
        data insertion into the tables
    input_data : string
        Filepath where all the song files are stored
    output_data : string
        Filepath where the processed data can be stored.
    Returns
    ----------
    No return value
    
    Raises
    ----------
    OSError
        If the filepaths are invalid
    
    """
    
    #
    # Step 1 - Load songs table 
    #
    
    # get filepath to song data file
    dim_song_data = input_data
    
    # read song data file
    df = spark.read.json(dim_song_data)
    
    # check the data load schema
    print("dim_song schema:")
    df.printSchema()

    # create the temp table to obtain the required data
    df.createOrReplaceTempView("dim_song")
    
    # construct the SQL to extract the songs table column data
    dim_song_table = spark.sql("""
        SELECT song_id
              ,title
              ,artist_id
              ,year
              ,duration
        FROM  dim_song
        ORDER BY song_id
    """)
    
    # ensure the records are as expected
    print("Showing sample dim_song table records - ")
    dim_song_table.show(5,truncate=False)
    
    # obtain the output path for the dim_song data
    dim_song_table_path = output_data + "dim_song.parquet"
    
    # write songs table to parquet files partitioned by year and artist
    # create partition by 'year' and 'artist_id', over
    dim_song_table.write.mode("overwrite").partitionBy("year","artist_id").parquet(dim_song_table_path)
    
    #
    # Step 2 - Load dim_artist table
    #
    
    # extract columns to create artists table
    df.createOrReplaceTempView("dim_artist")
    
    # construct the SQL to extract the songs table column data
    dim_artist_table = spark.sql("""
        SELECT   artist_id        AS artist_id
                ,artist_name      AS name
                ,artist_location  AS location
                ,artist_latitude  AS latitude
                ,artist_longitude AS longitude
        FROM    dim_artist
        ORDER BY artist_id
    """)
    
    # check the data load schema
    print("dim_artist_table schema:")
    dim_artist_table.printSchema()
    
    # ensure the records are as expected
    print("Showing sample dim_artist table records - ")
    dim_artist_table.show(5,truncate = False)
    
    # obtain the output path for the dim_artist table data
    dim_artist_table_path = output_data + "dim_artist.parquet"
    
    # write artists table to parquet files
    dim_artist_table.write.mode("overwrite").parquet(dim_artist_table_path)

def process_log_data(spark, input_data_ld, input_data_sd, output_data):
    """Processing of all log file records
    
    Purpose of this funciton is to loop in all the files from data/log_data folder
    using which the following tables are populated -
    
    dim_users
    dim_time
    fact_songplay
    
    Parameters
    ----------
    spark : object
        Connection object passed on from the main function to perform the 
        data insertion into the tables
    input_data_sd : string
        Filepath where all the song files are stored
    input_data_ld : string
        Filepath where all the log data files are stored
    output_data : string
        Filepath where the table's output data is written into
        
    Returns
    ----------
    No return value
    
    """
    #
    # Step 1 - Load dim_user data 
    #
    
    # get filepath to log data file
    log_data = input_data_ld

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays i.e. page set to 'NextPage'
    df_filtered = df.filter(df.page == 'NextSong') 
    
    # create the temp table to obtain the required data
    df_filtered.createOrReplaceTempView("dim_user")

    # extract columns for dim_user table    
    dim_user_table = spark.sql("""
        SELECT  DISTINCT 
                userId    AS user_id
                ,firstName AS first_name
                ,lastName  AS last_name
                ,gender
                ,level
        FROM    dim_user
        ORDER BY last_name
    """)
    
    # check the data load schema
    print("dim_user table schema:")
    dim_user_table.printSchema()
    
    # ensure the records are as expected
    print("Showing sample dim_user table records - ")
    dim_user_table.show(5 ,truncate = False)
    
    # obtain the output path for the songs table data
    dim_user_table_path = output_data + "dim_user.parquet"
    
    # write artists table to parquet files
    dim_user_table.write.mode("overwrite").parquet(dim_user_table_path)
    
    #
    # Step 2 - Load dim_time data 
    #    
    
    # get timestamp column from log_data
    @udf(typ.TimestampType())
    def get_timestamp(ts):
        return datetime.fromtimestamp(ts / 1000.0)
    
    # get date time value from the column ts in the log data set
    df_filtered = df_filtered.withColumn("timestamp", get_timestamp("ts"))
    
    # get datetime column from original timestamp column
    @udf(typ.StringType())
    def get_datetime(ts):
        return datetime.fromtimestamp(ts / 1000.0)\
                       .strftime('%Y-%m-%d %H:%M:%S')
    
    # get date time value from the column ts in the log data set
    df_filtered = df_filtered.withColumn("datetime", get_datetime("ts"))
    
    # create the temp table to obtain the required data
    df_filtered.createOrReplaceTempView("dim_time")
    
    # check to see if the new columns are added
    df_filtered.printSchema()    

    dim_time_table = spark.sql("""
        SELECT  DISTINCT 
                ts 
                ,datetime AS start_time
                ,hour(timestamp) AS hour
                ,day(timestamp)  AS day
                ,weekofyear(timestamp) AS week
                ,month(timestamp) AS month
                ,year(timestamp) AS year
                ,dayofweek(timestamp) AS weekday
        FROM dim_time
        ORDER BY start_time    
    """
    )
    
    # check the sample data
    print("showing sample dim_time records")
    dim_time_table.show(5)    

    # obtain the output path for the dim_time table data
    dim_time_table_path = output_data + "dim_time.parquet"
    
    # write dim_time table to parquet files partitioned by year and month
    dim_time_table.write.mode("overwrite").partitionBy("year","month").parquet(dim_time_table_path)
    
    #
    # Step 3 - Load fact_songplay data 
    #  
    
    # get songs_data input path
    song_data = input_data_sd
    
    # read in songs data to get songsplay data
    df_song = spark.read.json(song_data)
    
    # read in song data to use for songplays table
    df_song_log_data = df_filtered.join(df_song,(df_filtered.artist == df_song.artist_name) & (df_filtered.song == df_song.title))
    
    # add the surrogate column songplay_id by setting it as monotonically increasion id function
    df_song_log_data = df_song_log_data.withColumn("songplay_id",monotonically_increasing_id())
    
    # validate schema of the joined table
    print("joined schema:")
    df_song_log_data.printSchema()
    
    # create the temp table to obtain the required data
    df_song_log_data.createOrReplaceTempView("fact_songplay")
    
    # extract columns from joined song and log datasets to create songplays table 
    fact_songplay_table = spark.sql("""
        SELECT   songplay_id AS songplay_id
                ,timestamp   AS start_time
                ,userId      AS user_id
                ,level       AS level
                ,song_id     AS song_id
                ,artist_id   AS artist_id
                ,sessionId   AS session_id
                ,location    AS location
                ,userAgent   AS user_agent
        FROM    fact_songplay
    """)
    # get the output path for the fact_songplay table
    fact_songplay_table_path = output_data + "fact_songplay.parquet"
    
    # write songplays table to parquet files partitioned by year and month
    fact_songplay_table.write.mode("overwrite").parquet(fact_songplay_table_path)
    
    # print some sample records of the fact songplay table
    fact_songplay_table.show(5)
    

def main():
    """
    
    Load JSON input data from the folders song_data and log_data either locally
    or stored in S3 and extract information pertaining to songs, artists, songplays
    etc.
    
    """    
    spark = create_spark_session()
    
    # Use below section to load data from the local file location
    input_data_sd = config['INPUT_DATA']['SONG_DATA_LOCAL']
    input_data_ld = config['INPUT_DATA']['LOG_DATA_LOCAL']
    
    # Use below section to load data from S3 Location
    #input_data_sd = config['INPUT_DATA']['SONG_DATA_S3']
    #input_data_ld = config['INPUT_DATA']['LOG_DATA_S3']
    
    # Use below output to log the output data into local path
    #output_data = config['INPUT_DATA']['OUTPUT_DATA_LOCAL']
    
    # Use below output to log the output data into S3 path
    output_data = config['INPUT_DATA']['OUTPUT_DATA_S3']    
    
    process_song_data(spark, input_data_sd, output_data)
    
    # pass both song_data path and log_data path for songplays table data
    # extraction and insertion
    process_log_data(spark, input_data_ld, input_data_sd, output_data)


if __name__ == "__main__":
    main()
