import configparser
import logging.config
from datetime import datetime
import os
import findspark

findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType

# environment variables
''' config = configparser.ConfigParser()
config.read('/dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID') # AWS access key id from ['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY') # AWS secret access key from ['AWS_SECRET_ACCESS_KEY'] '''

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')  # AWS access key id from ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')  # AWS secret access key from ['AWS_SECRET_ACCESS_KEY']

# Setting up logger
logging.config.fileConfig("logger.conf")
logger = logging.getLogger(__name__)


def create_spark_session():
    try:
        spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
        logger.info("Spark session created")
        return spark
    except Exception as e:
        logger.error("Error creating spark session: {}".format(e))
        raise e


def process_song_data(spark, input_data, output_data):
    """
    Description:
        Process the songs' data files and create extract songs table and artist table data from it.

    :param spark: a spark session instance
    :param input_data: input file path
    :param output_data: output file path
    """
    # get filepath to song data file from subdirectory songs_data
    song_data = input_data + "song_data/*/*/*/*"

    # read song data file
    df = spark.read.json(song_data) #, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record').drop_duplicates()

    # extract columns to create songs table
    songs_table = df.select('song_id, title, artist_id, year, duration').drop_duplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + 'songs/', mode='overwrite')
    logger.info("Finished writing song data to parquet")

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude',
                              'artist_longitude').drop_duplicates()

    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/', mode='overwrite')
    logger.info("Finished writing artist data to parquet")


def process_log_data(spark, input_data, output_data):
    """
    Description:
            Process the event log file and extract data for table time, users and songplays from it.

    :param spark: a spark session instance
    :param input_data: input file path
    :param output_data: output file path
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    # df = spark.read.json(log_data, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record').drop_duplicates()
    df = spark.read.json(log_data).drop_duplicates()

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'leve').drop_duplicates()

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users/"), mode='overwrite')
    logger.info("Finished writing users table to parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    df = df.withColumn('timestamp', get_timestamp(df.ts))

    # create datetime column from original timestamp column
    ''' get_datetime = udf()
    df =  '''

    # extract columns to create timetable
    time_table = df.withColumn('hour', hour(df.timestamp)).withColumn('day', dayofmonth(df.timestamp)).withColumn(
        'week', weekofyear(df.timestamp)).withColumn('month', month(df.timestamp)).withColumn('year', year(
        df.timestamp)).withColumn('weekday', date_format(df.timestamp, 'E')).drop_duplicates()

    # write timetable to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data, "time_table/", mode='overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.format("parquet").options(path=output_data + "songs/").load(
        output_data + "songs/*/*/*.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df.song == song_df.title, how='inner') \
        .select(monotonically_increasing_id().alias("songplay_id"), col("start_time"), col("userId").alias("user_id"),
                "level", "song_id", "artist_id", col("sessionId").alias("session_id"), "location",
                col("userAgent").alias("user_agent"))

    songplays_table = songplays_table.join(time_table, songplays_table.start_time == time_table.start_time, how="inner") \
        .select("songplay_id", songplays_table.start_time, "user_id", "level", "song_id", "artist_id", "session_id",
                "location", "user_agent", "year", "month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.drop_duplicates().write.partitionBy('year', 'month').parquet(
        os.path.join(output_data + "songplays/"), mode='overwrite')
    logger.info("Finished writing songplays table to parquet")


def main():
    spark = create_spark_session()
    # input_data = "s3a://udacity-dend/"
    # output_data = "s3a://udacity-dend-output/"
    input_data = "data/"
    output_data = "data/output/"

    logger.info("Starting ETL process")
    logger.info("-" * 50)
    logger.info("Processing song data")
    process_song_data(spark, input_data, output_data)
    logger.info("Finished processing song data")
    logger.info("Processing log data")
    process_log_data(spark, input_data, output_data)
    logger.info("Finished processing log data")


if __name__ == "__main__":
    main()
