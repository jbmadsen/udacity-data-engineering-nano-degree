class SqlQueries:
    create_staging_tables = """
        DROP TABLE IF EXISTS public.staging_events;
        CREATE TABLE IF NOT EXISTS public.staging_events (
            artist varchar(256),
            auth varchar(256),
            firstname varchar(256),
            gender varchar(256),
            iteminsession int4,
            lastname varchar(256),
            length numeric(18,0),
            "level" varchar(256),
            location varchar(256),
            "method" varchar(256),
            page varchar(256),
            registration numeric(18,0),
            sessionid int4,
            song varchar(256),
            status int4,
            ts int8,
            useragent varchar(256),
            userid int4
        );

        DROP TABLE IF EXISTS public.staging_songs;
        CREATE TABLE IF NOT EXISTS public.staging_songs (
            num_songs int4,
            artist_id varchar(256),
            artist_name varchar(256),
            artist_latitude numeric(18,0),
            artist_longitude numeric(18,0),
            artist_location varchar(256),
            song_id varchar(256),
            title varchar(256),
            duration numeric(18,0),
            "year" int4
        );
    """

    create_main_tables = """
        CREATE TABLE IF NOT EXISTS public.artists (
            artist_id VARCHAR PRIMARY KEY, 
            name VARCHAR,
            location VARCHAR,
            latitude FLOAT,
            longitude FLOAT
        )
        SORTKEY (artist_id);

        CREATE TABLE IF NOT EXISTS public.songplays (
            songplay_id VARCHAR(32) NOT NULL,
            start_time TIMESTAMP,
            user_id INT, 
            level VARCHAR, 
            song_id VARCHAR, 
            artist_id VARCHAR, 
            session_id INT, 
            location VARCHAR, 
            user_agent VARCHAR,
            CONSTRAINT songplays_pkey PRIMARY KEY (songplay_id)
        )
        DISTSTYLE KEY
        DISTKEY (start_time)
        SORTKEY (start_time);

        CREATE TABLE IF NOT EXISTS public.songs (
            song_id VARCHAR PRIMARY KEY, 
            title VARCHAR,
            artist_id VARCHAR,
            year INT,
            duration FLOAT
        )
        SORTKEY (song_id);

        CREATE TABLE IF NOT EXISTS public.users (
            user_id INT PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR,
            gender VARCHAR,
            level VARCHAR
        )
        SORTKEY (user_id);

        CREATE TABLE IF NOT EXISTS public.time (
            start_time TIMESTAMP PRIMARY KEY, 
            hour INT,
            day INT,
            week INT,
            month INT,
            year INT,
            weekday INT
        )
        DISTSTYLE KEY
        DISTKEY (start_time)
        SORTKEY (start_time);
    """

    songplay_table_insert = """
        SELECT DISTINCT
            md5(events.sessionid || events.start_time) songplay_id,
            events.start_time, 
            events.userid, 
            events.level, 
            songs.song_id, 
            songs.artist_id, 
            events.sessionid, 
            events.location, 
            events.useragent
        FROM (
            SELECT 
                TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, 
                *
            FROM staging_events
            WHERE page='NextSong'
        ) events
        LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration;
    """

    user_table_insert = """
        SELECT DISTINCT
            userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
            AND userId IS NOT NULL;
    """

    song_table_insert = """
        SELECT DISTINCT 
            song_id, title, artist_id, year, duration
        FROM staging_songs
        WHERE song_id IS NOT NULL;
    """

    artist_table_insert = """
        SELECT DISTINCT 
            artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL;
    """

    time_table_insert = """
        SELECT DISTINCT 
            start_time, 
            EXTRACT(hour FROM start_time), 
            EXTRACT(day FROM start_time), 
            EXTRACT(week FROM start_time), 
            EXTRACT(month FROM start_time), 
            EXTRACT(year FROM start_time), 
            EXTRACT(dayofweek FROM start_time)
        FROM songplays
    """
