import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
        artist text, 
        auth text, 
        firstName text, 
        gender text, 
        ItemInSession int,
        lastName text, 
        length float8, 
        level text, 
        location text, 
        method text,
        page text, 
        registration text, 
        sessionId INTEGER, 
        song text, 
        status INTEGER,
        ts bigint, 
        userAgent text, 
        user_id INTEGER)                        
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                                    song_id VARCHAR(500),
                                    num_songs INTEGER,
                                    artist_id VARCHAR(500) NOT NULL,
                                    artist_latitude float,
                                    artist_longitude float,
                                    artist_location VARCHAR(255),
                                    artist_name VARCHAR(255),
                                    title VARCHAR(255),
                                    duration float,
                                    year INTEGER,
                                    PRIMARY KEY(song_id))
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay(
                            songplay_id INT IDENTITY(0,1),
                            start_time TIMESTAMP NOT NULL,
                            user_id INTEGER NOT NULL,
                            level VARCHAR(250),
                            song_id TEXT  NOT NULL,
                            artist_id TEXT NOT NULL,
                            session_id bigint,
                            location TEXT,
                            userAgent TEXT,
                            PRIMARY KEY(songplay_id))
""")

user_table_create = ("""CREATE TABLE users(
                        user_id VARCHAR,
                        first_name VARCHAR(255) NOT NULL,
                        last_name VARCHAR(255) NOT NULL,
                        gender VARCHAR,
                        level VARCHAR(150),
                        PRIMARY KEY(user_id))
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                        song_id VARCHAR,
                        title TEXT,
                        artist_id TEXT  NOT NULL,
                        year INTEGER,
                        duration NUMERIC,
                        PRIMARY KEY(song_id))
""")

artist_table_create = ("""CREATE TABLE artists(
                           artist_id VARCHAR,
                           name VARCHAR(200),
                           location VARCHAR(255),
                           latitude NUMERIC,
                           longitude NUMERIC,
                           PRIMARY KEY(artist_id))
""")

time_table_create = ("""CREATE TABLE time(
                        start_time TIMESTAMP,
                        hour INTEGER,
                        day INTEGER,
                        week INTEGER,
                        month INTEGER,
                        year INTEGER,
                        weekday INTEGER,
                        PRIMARY KEY(start_time))
""")

# STAGING TABLES

staging_events_copy = (""" copy staging_events from '{}'
                           credentials 'aws_iam_role={}'
                           region 'us-west-2' 
                           COMPUPDATE OFF 
                           JSON '{}' """).format(config.get('S3','LOG_DATA'),
                           config.get('IAM_ROLE', 'ARN'),
                           config.get('S3','LOG_JSONPATH'))


staging_songs_copy = ("""copy staging_songs from '{}'
                      credentials 'aws_iam_role={}'
                      region 'us-west-2' 
                      COMPUPDATE OFF 
                      JSON 'auto'
                      """).format(config.get('S3','SONG_DATA'), 
                                  config.get('IAM_ROLE', 'ARN'))


# FINAL TABLES



user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
                        SELECT distinct  user_id, 
                        firstName, 
                        lastName, 
                        gender, 
                        level
                        FROM staging_events
                        WHERE page = 'NextSong'
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
                        SELECT DISTINCT 
                        song_id, 
                        title,
                        artist_id,
                        year,
                        duration
                        FROM staging_songs
                        WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
                    SELECT DISTINCT 
                    artist_id,
                    artist_name,
                    artist_location,
                    artist_latitude,
                    artist_longitude
                    FROM staging_songs
                    WHERE artist_id IS NOT NULL
""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT 
        start_time, 
        EXTRACT(HOUR from start_time) AS hour,
        EXTRACT(DAY from start_time) AS day,
        EXTRACT(WEEK from start_time) AS week,
        EXTRACT(MONTH from start_time) AS month,
        EXTRACT(YEAR from start_time) AS year, 
        EXTRACT(WEEKDAY from start_time) AS weekday 
    FROM (
    	SELECT DISTINCT  TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time 
        FROM staging_events s)     
    """)
    
songplay_table_insert = ("""INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, userAgent) 
    SELECT DISTINCT 
        TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time, 
        e.user_id, 
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId,
        e.location,
        e.userAgent
    FROM staging_events e
    JOIN staging_songs  s  ON (e.song = s.title AND e.artist = s.artist_name)
    AND e.page  =  'NextSong'
    
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
