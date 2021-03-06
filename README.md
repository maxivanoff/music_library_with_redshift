# Music Library Database with Redshift

This project covers a setup of Redshift database for a music library and ETL pipelines for data transfer into dimensional tables in Redshift.

## Getting Started

### Prerequisites: database

The data is distributed over two major datasets. The first dataset is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. 

The second dataset consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.

### Prerequisites: packages

Several external packages are needed to run the code

* psycopg2
* pandas

## Content

For the song and log datasets stored in S3 here we create ETL pipelines that load data into a star schema in Redshift
optimized for queries on song play analysis. 
This includes the following tables:

* Fact Table

    * songplays - records in log data associated with song plays: 
    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

* Dimension Tables

    * users - users in the app:
    user_id, first_name, last_name, gender, level
    * songs - songs in music database:
    song_id, title, artist_id, year, duration
    * artists - artists in music database:
    artist_id, name, location, latitude, longitude
    * time - timestamps of records in songplays broken down into specific units:
        start_time, hour, day, week, month, year, weekday

To create empty tables run `python create_tables.py`. To load data from JSON files on S3 into the Redshift run `python etl.py`. 
