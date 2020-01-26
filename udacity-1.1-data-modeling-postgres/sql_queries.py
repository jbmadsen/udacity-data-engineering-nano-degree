# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id BIGSERIAL PRIMARY KEY, 
    start_time BIGINT, --REFERENCES time(start_time),
    user_id INT NOT NULL,-- REFERENCES users(user_id), 
    level VARCHAR, 
    song_id VARCHAR,-- REFERENCES songs(song_id), 
    artist_id VARCHAR,-- REFERENCES artists(artist_id), 
    session_id INT, 
    location VARCHAR, 
    user_agent VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR
);
""")

# single song file example:
# {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY, 
    title VARCHAR,
    artist_id VARCHAR NOT NULL,-- REFERENCES artists(artist_id),
    year INT,
    duration DECIMAL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY, 
    name VARCHAR,
    location VARCHAR,
    latitude real,
    longitude real
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time BIGINT PRIMARY KEY, 
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s) 
ON CONFLICT (songplay_id) 
DO NOTHING;
/*
DO UPDATE
    SET songplay_id = EXCLUDED.songplay_id,
        start_time = EXCLUDED.start_time, 
        user_id = EXCLUDED.user_id,
        level = EXCLUDED.level,
        song_id = EXCLUDED.song_id,
        artist_id = EXCLUDED.artist_id,
        session_id = EXCLUDED.session_id,
        location = EXCLUDED.location,
        user_agent = EXCLUDED.user_agent;
*/
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES
    (%s, %s, %s, %s, %s) 
ON CONFLICT (user_id) 
DO NOTHING;
/*
DO UPDATE
    SET user_id = EXCLUDED.user_id,
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        gender = EXCLUDED.gender,
        level = EXCLUDED.level;
*/
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
VALUES
    (%s, %s, %s, %s, %s) 
ON CONFLICT (song_id) 
DO NOTHING;
/*
DO UPDATE
    SET song_id = EXCLUDED.song_id,
        title = EXCLUDED.title,
        artist_id = EXCLUDED.artist_id,
        year = EXCLUDED.year,
        duration = EXCLUDED.duration;
*/
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
VALUES
    (%s, %s, %s, %s, %s) 
ON CONFLICT (artist_id) 
DO NOTHING;
/*
DO UPDATE
    SET artist_id = EXCLUDED.artist_id,
        name = EXCLUDED.name,
        location = EXCLUDED.location,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude;
*/
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
VALUES
    (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) 
DO NOTHING;
""")

# FIND SONGS

song_select = ("""
SELECT s.song_id, s.artist_id
FROM songs s
LEFT JOIN artists a ON a.artist_id = s.artist_id
WHERE s.title = %(song)s
    and a.name = %(artist)s
    and round(s.duration::numeric, 3) = round(%(length)s::numeric, 3)
    --and s.duration = %(length)s 
LIMIt 1;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]