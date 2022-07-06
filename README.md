
# Project - Data Lake
---

The music streaming startup, Sparkify, has grown their user base and song database and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project, builds an ETL pipeline for a data lake hosted on S3. We will load data from S3, process the data into analytics tables using Spark, and load them back into S3. We will deploy this Spark process on a cluster using AWS.

## Deployement

File `dl.cfg` is not provided here. File contains :


```
KEY=YOUR_AWS_ACCESS_KEY
SECRET=YOUR_AWS_SECRET_KEY
```

If you are using local as your development environemnt - Moving project directory from local to EMR 


    scp -i <.pem-file> <Local-Path> <username>@<EMR-MasterNode-Endpoint>:~<EMR-path>

Running spark job (Before running job make sure EMR Role have access to s3)

    spark-submit etl.py --master yarn --deploy-mode client --driver-memory 4g --num-executors 2 --executor-memory 2g --executor-core 2

## ETL Pipeline
    
1.  Read data from S3
    
    -   Song data:  `s3://udacity-dend/song_data`
    -   Log data:  `s3://udacity-dend/log_data`
    
    The script reads song_data and load_data from S3.
    
3.  Process data using spark
    
    Transforms them to create five different tables listed below : 
    #### Fact Table
	 **songplays**  - records in log data associated with song plays i.e. records with page  `NextSong`
    
    Columns: `songplay_id`, `start_time`, `user_id`, `level`, `song_id`, `artist_id`, `session_id`, `location`, `user_agent`

	#### Dimension Tables
	**users**  - users in the app

	Columns:   `user_id`, `first_name`, `last_name`, `gender`, `level`

	
	**songs**  - songs in music database

    Columns: `song_id`, `title`, `artist_id`, `year`, `duration`

    
	**artists**  - artists in music database

    Columns: `artist_id`, `name`, `location`, `lattitude`, `longitude`

    
	**time**  - timestamps of records in  **songplays**  broken down into specific units

    Columns: `start_time`, `hour`, `day`, `week`, `month`, `year`, `weekday`
    
4.  Load it back to S3
    
    Write the data to partitioned parquet files in table directories on S3.
