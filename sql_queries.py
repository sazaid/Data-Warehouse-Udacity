import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events_table
(
        artist              VARCHAR,
        auth                VARCHAR,
        firstName           VARCHAR,
        gender              VARCHAR,
        itemInSession       INTEGER,
        lastName            VARCHAR,
        length              FLOAT,
        level               VARCHAR,
        location            VARCHAR,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT,
        sessionId           INTEGER,
        song                VARCHAR,
        status              INTEGER,
        ts                  TIMESTAMP,
        userAgent           VARCHAR,
        userId              INTEGER 
    )
""")

staging_songs_table_create = ("""
 CREATE TABLE staging_songs_table
 (
        num_songs           INTEGER,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER
    )
""")

songplay_table_create = ("""
 CREATE TABLE songplay_table
 (
        songplay_id         INTEGER         IDENTITY(0,1)   PRIMARY KEY,
        start_time          TIMESTAMP,
        user_id             INTEGER ,
        level               VARCHAR,
        song_id             VARCHAR,
        artist_id           VARCHAR ,
        session_id          INTEGER,
        location            VARCHAR,
        user_agent          VARCHAR
    )
""")

user_table_create = ("""
   CREATE TABLE user_table
   (
        user_id             INTEGER PRIMARY KEY,
        first_name          VARCHAR,
        last_name           VARCHAR,
        gender              VARCHAR,
        level               VARCHAR
    )
""")

song_table_create = ("""
   CREATE TABLE song_table
   (
        song_id             VARCHAR PRIMARY KEY,
        title               VARCHAR ,
        artist_id           VARCHAR ,
        year                INTEGER ,
        duration            FLOAT
    )
""")

artist_table_create = ("""
CREATE TABLE artist_table
(
        artist_id           VARCHAR  PRIMARY KEY,
        name                VARCHAR ,
        location            VARCHAR,
        latitude            FLOAT,
        longitude           FLOAT
    )
""")

time_table_create = ("""
CREATE TABLE time_table
(
        start_time          TIMESTAMP       NOT NULL PRIMARY KEY,
        hour                INTEGER         NOT NULL,
        day                 INTEGER         NOT NULL,
        week                INTEGER         NOT NULL,
        month               INTEGER         NOT NULL,
        year                INTEGER         NOT NULL,
        weekday             VARCHAR(20)     NOT NULL
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events_table from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON {log_json_path}
    timeformat as 'epochmillisecs';
""").format(data_bucket=config['S3']['LOG_DATA'], role_arn=config['IAM_ROLE']['ARN'], log_json_path=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs_table from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON 'auto';
""").format(data_bucket=config['S3']['SONG_DATA'], role_arn=config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay_table (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT e.ts         AS start_time,
           e.userId     AS user_id,
           e.level,
           s.song_id,
           s.artist_id,
           e.sessionId  AS session_id,
           e.location,
           e.userAgent  AS user_agent
    FROM staging_events_table e
    JOIN staging_songs_table  s   ON (e.song = s.title AND e.artist = s.artist_name)
    AND e.page  LIKE  'NextSong'

""")

user_table_insert = ("""
    INSERT INTO user_table (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT (userId)   AS user_id,
           firstName           AS first_name,
           lastName            AS last_name,
           gender,
           level
    FROM staging_events_table
    WHERE user_id IS NOT NULL
    AND page LIKE 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO song_table (song_id, title, artist_id, year, duration)
    SELECT DISTINCT(song_id) AS song_id,
           title,
           artist_id,
           year,
           duration
    FROM staging_songs_table
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artist_table (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT(artist_id) AS artist_id,
           artist_name         AS name,
           artist_location     AS location,
           artist_latitude     AS latitude,
           artist_longitude    AS longitude
    FROM staging_songs_table
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time_table (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT(ts)                  AS start_time,
           EXTRACT (hour FROM start_time)        AS hour,         
           EXTRACT (day FROM start_time)         AS day,
           EXTRACT (week FROM start_time)        AS week,
           EXTRACT (month FROM start_time)       AS month,
           EXTRACT (year FROM start_time)        AS year,
           EXTRACT (weekday FROM start_time)     AS week_day
    FROM staging_events_table
    WHERE start_time IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
