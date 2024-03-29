# DROP TABLES
songplay_table_drop = "drop table IF EXISTS songplays;"
user_table_drop = "drop table IF EXISTS users;"
song_table_drop = "drop table IF EXISTS songs;"
artist_table_drop = "drop table IF EXISTS artists;"
time_table_drop = " drop table IF EXISTS time;"

# CREATE TABLES
songplay_table_create = (""" 
CREATE TABLE songplays 
  ( 
     songplay_id SERIAL PRIMARY KEY , 
     start_time  TIMESTAMP  NOT NULL, 
     user_id     INT  NOT NULL, 
     level       VARCHAR , 
     song_id     VARCHAR , 
     artist_id   VARCHAR , 
     session_id  INT , 
     location    VARCHAR, 
     user_agent  VARCHAR
  ); 
""")

user_table_create = (""" 
CREATE TABLE users 
  ( 
     user_id    INT PRIMARY KEY NOT NULL, 
     first_name VARCHAR, 
     last_name  VARCHAR, 
     gender     VARCHAR, 
     level      VARCHAR 
  ); 
""")

song_table_create = (""" 
CREATE TABLE songs 
  ( 
     song_id   VARCHAR PRIMARY KEY NOT NULL , 
     title     VARCHAR, 
     artist_id VARCHAR, 
     year      INT, 
     duration  FLOAT 
  ); 

""")

artist_table_create = (""" 
CREATE TABLE artists 
  ( 
     artist_id  VARCHAR PRIMARY KEY NOT NULL, 
     NAME       VARCHAR, 
     location   VARCHAR, 
     latiude    VARCHAR, 
     longtitude VARCHAR 
  ); 
""")

time_table_create = (""" CREATE TABLE time 
  ( 
     start_time TIMESTAMP PRIMARY KEY NOT NULL, 
     hour       INT, 
     day        INT, 
     week       INT, 
     month      INT, 
     year       INT, 
     weekday    INT 
  ); """)

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays 
            ( 
                        start_time, 
                        user_id, 
                        level, 
                        song_id, 
                        artist_id, 
                        session_id, 
                        location, 
                        user_agent 
            )
             values ( %s, %s, %s, %s, %s, %s,%s, %s) ;
""")

user_table_insert = ("""INSERT INTO users 
            ( 
                        user_id, 
                        first_name, 
                        last_name, 
                        gender, 
                        level 
            ) values (%s, %s, %s, %s, %s) ON CONFLICT (user_id) do update set level = EXCLUDED.level;
""")

song_table_insert = ("""
 INSERT INTO songs 
            ( 
                        song_id, 
                        title, 
                        artist_id, 
                        year, 
                        duration 
            )  VALUES (%s,%s,%s,%s,%s) ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
 INSERT INTO artists 
            ( 
                        artist_id , 
                        NAME, 
                        location, 
                        latiude, 
                        longtitude 
            ) values (%s,%s, %s, %s, %s) ON CONFLICT (artist_id) DO NOTHING; 
""")


time_table_insert = ("""
 INSERT INTO time 
            ( 
                        start_time, 
                        hour, 
                        day, 
                        week, 
                        month, 
                        year, 
                        weekday 
            ) values (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (start_time) DO NOTHING; 
""")

# FIND SONGS

song_select = ("""
  SELECT artists.artist_id, 
       songs.song_id 
FROM   songs 
       join artists 
         ON songs.artist_id = artists.artist_id 
WHERE  songs.title =%s 
       AND artists.name =%s 
       AND songs.duration =%s; """)

# QUERY LISTS
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]