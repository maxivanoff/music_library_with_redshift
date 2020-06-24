import configparser

# CONFIG
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
IAM_ROLE_ARN = config.get("IAM", "IAM_ROLE_ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
SONG_DATA = config.get("S3", "SONG_DATA")
JSON_PATH_EVENTS = config.get("S3", "JSON_PATH_EVENTS")

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
    artist          varchar, 
    auth            varchar, 
    firstName       varchar,
    gender          varchar,
    ItemInSession   int,
    lastName        varchar,
    length          float,
    level           varchar,
    location        varchar,
    method          varchar,
    page            varchar,
    registration    float,
    sessionId       varchar,
    song            varchar,
    status          int,
    ts              bigint,
    userAgent       varchar,
    userId          int
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
    userId          varchar NOT NULL,
    level           varchar,
    song_id         varchar NOT NULL,
    artist_id       varchar NOT NULL,
    sessionId       varchar,
    location        varchar,
    userAgent       varchar,
    PRIMARY KEY (songplay_id)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    userId          varchar, 
    firstName       varchar, 
    lastName        varchar, 
    gender          varchar, 
    level           varchar,
    PRIMARY KEY (userId)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id         varchar NOT NULL, 
    title           varchar, 
    artist_id       varchar NOT NULL, 
    year            int, 
    duration        float,
    PRIMARY KEY (song_id)
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id       varchar NOT NULL, 
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
from '{logs}'
iam_role '{iam}'
format as json '{json}';
""").format(logs=LOG_DATA,iam=IAM_ROLE_ARN,json=JSON_PATH_EVENTS)


staging_songs_copy = ("""
copy staging_songs
from '{songs}'
iam_role '{iam}'
json 'auto';
""").format(songs=SONG_DATA,iam=IAM_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, userId, level, song_id, artist_id, sessionId, location, userAgent)
(
SELECT (timestamp 'epoch' + se.ts / 1000 * interval '1 second') as start_time,
se.userId, se.level, ss.song_id, ss.artist_id, se.sessionId, ss.artist_location as location, se.userAgent
FROM staging_events se, staging_songs ss
WHERE (start_time IS NOT NULL) AND
(se.userId IS NOT NULL) AND
(ss.song_id IS NOT NULL) AND 
(ss.artist_id IS NOT NULL)
);
""")

songplay_table_insert = ("""
INSERT INTO songplays
(
SELECT
md5(events.sessionid || events.start_time) songplay_id,
events.start_time,
events.userid,
events.level,
songs.song_id,
songs.artist_id,
events.sessionid,
events.location,
events.useragent
FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, * FROM staging_events WHERE page='NextSong') events
LEFT JOIN staging_songs songs
ON events.song = songs.title AND events.artist = songs.artist_name AND events.length = songs.duration
WHERE (start_time IS NOT NULL) AND (events.userid IS NOT NULL) AND (songs.song_id IS NOT NULL) AND (songs.artist_id IS NOT NULL)
);
""")

user_table_insert = ("""
INSERT INTO users (userId, firstName, lastName, gender, level)
(
SELECT DISTINCT se.userId, se.firstName, se.lastName, se.gender, se.level FROM staging_events se
WHERE (se.userId IS NOT NULL)
);
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
(
SELECT DISTINCT ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration FROM staging_songs ss
WHERE (ss.song_id IS NOT NULL) AND (ss.artist_id IS NOT NULL)
);
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
(
SELECT DISTINCT ss.artist_id, ss.artist_name, ss.artist_location, ss.artist_latitude, ss.artist_longitude FROM staging_songs ss
WHERE (ss.artist_id IS NOT NULL)
);
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
(
SELECT timestamp 'epoch' + se.ts / 1000 * interval '1 second' as start_time,
DATE_PART(hrs, start_time) as hour,
DATE_PART(dayofyear, start_time) as day,
DATE_PART(w, start_time) as week,
DATE_PART(mons ,start_time) as month,
DATE_PART(yrs , start_time) as year,
DATE_PART(dow, start_time) as weekday
from staging_events se
);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
