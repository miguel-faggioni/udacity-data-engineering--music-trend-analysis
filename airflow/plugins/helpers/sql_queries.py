class SqlQueries:
    create_tables = ("""
CREATE TABLE IF NOT EXISTS public.artists (
	artist_id varchar(256) NOT NULL,
	name varchar(256),
	location varchar(256),
	latitude numeric(18,0),
	longitude numeric(18,0),
	CONSTRAINT artists_pkey PRIMARY KEY (artist_id)
);

CREATE TABLE IF NOT EXISTS public.charts (
	chart_song_id varchar(32) NOT NULL,
	song_id varchar(256),
	artist_id varchar(256),
        rank int2,
        chart_name varchar(256),
        "year" int4,
	CONSTRAINT songplays_pkey PRIMARY KEY (chart_song_id)
);

CREATE TABLE IF NOT EXISTS public.songs (
	song_id varchar(256) NOT NULL,
	title varchar(256),
	artist_id varchar(256),
	"year" int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (song_id)
);

CREATE TABLE IF NOT EXISTS public.staging_charts (
        chart_title varchar(256),
        artist_name varchar(256),
        song_name varchar(256),
        chart_year int4,
        song_rank int2
);

CREATE TABLE IF NOT EXISTS public.staging_songs (
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

CREATE TABLE IF NOT EXISTS public.staging_features (
        spotify_id varchar(256),
        artist_name varchar(256),
        song_name varchar(256),
        duration numeric(28,10),
        end_of_fade_in numeric(32,16),
        start_of_fade_out numeric(32,16),
        loudness numeric(32,16),
        tempo numeric(32,16),
        tempo_confidence numeric(32,16),
        key int4,
        key_confidence numeric(32,16),
        mode int4,
        mode_confidence numeric(32,16),
        danceability numeric(32,16),
        energy numeric(32,16),
        speechiness numeric(32,16),
        acousticness numeric(32,16),
        instrumentalness numeric(32,16),
        liveness numeric(32,16),
        valence numeric(32,16),
        time_signature int4
);
    """)
    
    chart_table_insert = ("""
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
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

    select_nulls_count = (["""
        SELECT SUM(CASE WHEN songid IS NULL THEN 1 ELSE 0 END) FROM songplays
    ""","""
        SELECT SUM(CASE WHEN artistid IS NULL THEN 1 ELSE 0 END) FROM songplays
    ""","""
        SELECT SUM(CASE WHEN userid IS NULL THEN 1 ELSE 0 END) FROM songplays
    ""","""
        SELECT SUM(CASE WHEN start_time IS NULL THEN 1 ELSE 0 END) FROM songplays
    ""","""
        SELECT SUM(CASE WHEN songid IS NULL THEN 1 ELSE 0 END) FROM songs
    ""","""
        SELECT SUM(CASE WHEN artistid IS NULL THEN 1 ELSE 0 END) FROM artists
    ""","""
        SELECT SUM(CASE WHEN userid IS NULL THEN 1 ELSE 0 END) FROM users
    """])
