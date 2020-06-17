import configparser


# CONFIG
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
IAM_ROLE_ARN = config.get("IAM", "IAM_ROLE_ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    user_id         int,
    first_name      varchar,
    last_name       varchar,
    gender          varchar,
    location        varchar,
    artist          varchar, 
    auth            varchar, 
    ts              varchar,
    item_in_session int,
    session_id      varchar,
    song            varchar,
    length          float,
    level           varchar,
    method          varchar,
    page            varchar,
    registration    float,
    status          int,
    user_agent      varchar
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    song_id             varchar,
    title               varchar,
    year                int,
    artist_id           varchar, 
    artist_name         varchar,
    artist_location     varchar,
    artist_latitude     float,
    artist_longitude    float,
    duration            float,
    num_songs           int,
    PRIMARY KEY (song_id)
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id     varchar, 
    start_time      varchar NOT NULL, 
    user_id         varchar NOT NULL, 
    level           varchar, 
    song_id         varchar NOT NULL, 
    artist_id       varchar NOT NULL, 
    session_id      varchar, 
    location        varchar, 
    user_agent      varchar,
    PRIMARY KEY (songplay_id)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id         varchar, 
    first_name      varchar, 
    last_name       varchar, 
    gender          varchar, 
    level           varchar,
    PRIMARY KEY (user_id)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id         varchar, 
    title           varchar, 
    artist_id       varchar NOT NULL, 
    year            int, 
    duration        float,
    PRIMARY KEY (song_id)
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id       varchar, 
    name            varchar, 
    location        varchar, 
    latitude        varchar, 
    longitude       varchar,
    PRIMARY KEY (artist_id)
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time      varchar, 
    hour            int, 
    day             int, 
    week            int, 
    month           int, 
    year            int, 
    weekday         int,
    PRIMARY KEY (start_time)
);
""")


# STAGING TABLES

staging_events_copy = ("""
copy staging_events
from 's3://udacity-dend/log_data/'
iam_role '{}'
JSON 'auto';
""").format(IAM_ROLE_ARN)


staging_songs_copy = ("""
copy staging_songs
from 's3://udacity-dend/song_data/'
iam_role '{}'
JSON 'auto';
""").format(IAM_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
