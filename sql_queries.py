import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

#Load IAM Role and S3 bucket addrresses from config file
roleArn = config.get("IAM_ROLE","ARN")
logData = config.get("S3","LOG_DATA")
songData = config.get("S3","SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table;"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay;"
user_table_drop = "DROP TABLE IF EXISTS dim_users;"
song_table_drop = "DROP TABLE IF EXISTS dim_songs;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events_table(
"artist" VARCHAR(max),
"auth" VARCHAR(max),
"firstName" VARCHAR(max),
"gender" VARCHAR(max),
"itemInSession" Integer,
"lastName" VARCHAR(max),
"length" numeric(8,2),
"level" VARCHAR(10),
"location" VARCHAR(max),
"method" VARCHAR(max),
"page" VARCHAR(max),
"registration" numeric(15,2),
"sessionId" Integer,
"song" VARCHAR(max),
"status" Integer,
"ts" VARCHAR(50),
"userAgent" VARCHAR(max),
"userId" Integer

);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs_table(
"num_songs" Integer,
"artist_id" VARCHAR(max),
"artist_latitude" numeric(8,2),
"artist_longitude" numeric(8,2),
"artist_name" VARCHAR(max),
"artist_location" VARCHAR(max),
"song_id" VARCHAR(max),
"title" VARCHAR(max),
"duration" numeric(8,2),
"year" Integer
);
""")

songplay_table_create = ("""
CREATE TABLE fact_songplay(
"songplay_id" BIGINT IDENTITY(1,1) PRIMARY KEY,
"user_id" Integer,
"song_id" VARCHAR(max) distkey,
"artist_id" VARCHAR(max),
"start_time" TIMESTAMP,
"level" VARCHAR(10),
"sessionId" Integer,
"location" VARCHAR(max),
"userAgent" VARCHAR(max)
);

""")

user_table_create = ("""
CREATE TABLE dim_users(
"user_id" Integer PRIMARY KEY,
"firstName" VARCHAR(max),
"lastName" VARCHAR(max),
"gender" VARCHAR(25),
"level" VARCHAR(10)
) diststyle all;     
""")

song_table_create = ("""
CREATE TABLE dim_songs(
"song_id" VARCHAR(max) PRIMARY KEY distkey,
"title" VARCHAR(max),
"artist_id" VARCHAR(max),
"year" Integer,
"duration" numeric(8,2)
); 
""")

artist_table_create = ("""
CREATE TABLE dim_artists(
"artist_id" VARCHAR(max) PRIMARY KEY,
"artist_name" VARCHAR(max),
"artist_location" VARCHAR(max),
"artist_latitude" numeric(8,2),
"artist_longitude" numeric(8,2)
) diststyle all;  
""")

time_table_create = ("""
CREATE TABLE dim_time(
"time_id" BIGINT IDENTITY(1,1) PRIMARY KEY,
"start_time" TIMESTAMP,
"hour" Integer ,
"day" Integer,
"week" Integer,
"month" Integer,
"year" Integer,
"weekday" VARCHAR(25)
) diststyle all; 
""")

# STAGING TABLES

# Copy to events staging table. Set 'ignorecase' since matching process of COPY statement is case-sensitive by default
staging_events_copy = """
copy staging_events_table
from {}
iam_role '{}'
json 'auto ignorecase'
""".format(logData, roleArn)

# Copy to songs staging table.
staging_songs_copy = """
copy staging_songs_table
from {}
iam_role '{}'
json 'auto'
""".format(songData, roleArn)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_songplay(user_id, song_id, artist_id, start_time, level, sessionid, location, useragent)
select distinct se.userid, ss.song_id, ss.artist_id, TIMESTAMP 'epoch' + (ts::numeric)/1000 * INTERVAL '1 second' AS timestamp,
level,se.sessionid, location, useragent
from staging_events_table se
join staging_songs_table ss on se.song = ss.title
where page = 'NextSong' 
""")

#select unique user id by filtering out their previous subscription levels leaving the most recent 
user_table_insert = ("""
INSERT INTO dim_users(user_id, firstname, lastname, gender, level)
WITH uniq_users AS(select userid, firstname, 
lastname, gender, 
level, ROW_NUMBER() OVER(PARTITION BY userid ORDER BY ts DESC) AS rank
from staging_events_table
where userid is not null)
select userid, firstname, 
lastname, gender, level
from uniq_users
where rank = 1
""")

song_table_insert = ("""
INSERT into dim_songs(song_id, title, artist_id, "year", duration )
select distinct song_id, title, 
artist_id, "year", duration
from staging_songs_table
where song_id is not null
""")

artist_table_insert = ("""
INSERT into dim_artists(artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
select distinct artist_id, artist_name, 
artist_location, artist_latitude, artist_longitude
from staging_songs_table
where artist_id is not null
""")

time_table_insert = ("""
INSERT into dim_time(start_time, "hour", "day", "week","month", "year", "weekday")
select time_stamp, 
EXTRACT(hour from time_stamp) as "hour",
EXTRACT(day from time_stamp) as "day",
EXTRACT(week from time_stamp) as "week",
EXTRACT(month from time_stamp) as "month",
EXTRACT(year from time_stamp) as "year",
TO_CHAR(time_stamp, 'Day') as "weekday"
from
(
select distinct TIMESTAMP 'epoch' + (ts::numeric)/1000 * INTERVAL '1 second' AS time_stamp
from staging_events_table
where page = 'NextSong' and ts is not null
 ) as dim_table
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]


