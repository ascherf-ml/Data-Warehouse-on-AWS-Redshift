import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

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
CREATE TABLE IF NOT EXISTS staging_events
    (artist_name text, 
    auth text,
    user_firstname text,
    user_gender text,
    item_in_session int,
    user_lastname text,
    song_length double precision,
    user_level text,
    user_location text,
    method text,
    page text,
    registration double precision,
    session_id int,
    song_title text,
    status int,
    ts bigint,
    user_agent text,
    user_id int
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
    (artist_id text, 
    artist_latitude double precision,
    artist_location text,
    artist_longitude double precision,
    artist_name text,
    song_duration double precision,
    num_songs int,
    song_id text,
    song_title text,
    song_year int
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
    (songplay_id int NOT NULL IDENTITY(0,1), 
    start_time timestamp NOT NULL REFERENCES time(start_time), 
    user_id int NOT NULL REFERENCES users(user_id), 
    level text, 
    song_id text REFERENCES songs(song_id), 
    artist_id text REFERENCES artists(artist_id), 
    session_id int, 
    location text, 
    user_agent text,
PRIMARY KEY
    (songplay_id))
DISTKEY
    (song_id)
SORTKEY
    (session_id)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
    (user_id int NOT NULL, 
    first_name text NOT NULL, 
    last_name text NOT NULL, 
    gender text, 
    level text NOT NULL,
PRIMARY KEY
    (user_id))
DISTSTYLE all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
    (song_id text NOT NULL, 
    title text, 
    artist_id text NOT NULL REFERENCES artists(artist_id), 
    year int, 
    duration float,
PRIMARY KEY
    (song_id))
DISTKEY
    (song_id)
SORTKEY 
    (year)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
    (artist_id text NOT NULL,
    name text NOT NULL, 
    location text, 
    latitude float, 
    longitude float,
PRIMARY KEY
    (artist_id))
DISTSTYLE all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
    (start_time timestamp,
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday text,
PRIMARY KEY
    (start_time))
""")

# STAGING TABLES

staging_events_copy = ("""
DELETE FROM staging_events;
COPY staging_events 
    (artist_name, 
    auth, 
    user_firstname, 
    user_gender,
    item_in_session,
    user_lastname,
    song_length,
    user_level,
    user_location,
    method,
    page, 
    registration,
    session_id,
    song_title,
    status,
    ts, 
    user_agent, 
    user_id
)
FROM {}
FORMAT JSON AS {}
iam_role '{}'
""").format(config.get('S3', 'LOG_DATA'), config.get('S3', 'LOG_JSONPATH'),
    config.get('IAM_ROLE', 'REDSHIFT_ARN'))


staging_songs_copy = ("""
DELETE FROM staging_songs;
COPY staging_songs
    (artist_id,
    artist_latitude,
    artist_location,
    artist_longitude,
    artist_name,
    song_duration,
    song_id,
    song_title,
    song_year
)
FROM {}
FORMAT JSON AS 'auto'
iam_role '{}'
""").format(
    config.get('S3', 'SONG_DATA'), 
    config.get('IAM_ROLE', 'REDSHIFT_ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays
    (start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
SELECT DISTINCT
    TIMESTAMP 'epoch' + ts *INTERVAL '0.001 seconds' as start_time,
    e.user_id, 
    e.user_level,
    s.song_id,
    s.artist_id,
    e.session_id,
    e.user_location,
    e.user_agent
FROM 
    staging_events e,
    staging_songs s
WHERE
    e.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users
    (user_id,
    first_name,
    last_name,
    gender,
    level
)
SELECT DISTINCT 
    user_id, 
    user_firstname, 
    user_lastname, 
    user_gender, 
    user_level
FROM 
    staging_events
WHERE 
    page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs
    (song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT DISTINCT 
    song_id,
    song_title,
    artist_id,
    song_year,
    song_duration 
FROM 
    staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists
    (artist_id,
    name, 
    location,
    latitude,
    longitude
)
SELECT DISTINCT 
    artist_id, 
    artist_name, 
    artist_location, 
    artist_latitude, 
    artist_longitude
FROM 
    staging_songs
""")


time_table_insert = ("""
INSERT INTO time
    (start_time, 
    hour,
    day,
    week,
    month, 
    year,
    weekday
)
SELECT 
    start_time,
    EXTRACT(hour from start_time) AS hour,
    EXTRACT(day from start_time) AS day,
    EXTRACT(week from start_time) AS week,
    EXTRACT(month from start_time) AS month,
    EXTRACT(year from start_time) AS year, 
    EXTRACT(weekday from start_time) AS weekday
FROM
    songplays 
""")

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, staging_events_table_create, staging_songs_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
