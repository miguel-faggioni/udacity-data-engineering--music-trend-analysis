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
                artist_id varchar(256) NOT NULL,
                "year" int4,
                duration numeric(18,0),
                CONSTRAINT songs_pkey PRIMARY KEY (song_id)
        );
        
        CREATE TABLE IF NOT EXISTS public.lyrics (
                lyrics_id varchar(256) NOT NULL,
                artist_id varchar(256) NOT NULL,
                song_id varchar(256) NOT NULL,
                count_words int4,
                count_no_stopwords int4,
                count_distinct_words int4,
                count_distinct_no_stopwords int4,
                count_distinct_words_used_once int4,
                distinct_most_common varchar(256),
                count_most_common_usage int4,
                lyrics_sentiment numeric(8,7),
                common_words_sentiment numeric(8,7),
                common_words_sentiment_with_weights numeric(8,7),
                CONSTRAINT lyrics_pkey PRIMARY KEY (lyrics_id)
        );
        
        CREATE TABLE IF NOT EXISTS public.song_features (
                song_features_id varchar(256) NOT NULL,
                artist_id varchar(256) NOT NULL,
                song_id varchar(256) NOT NULL,
                fade_in numeric(32,16),
                fade_out numeric(32,16),
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
                time_signature int4,
                CONSTRAINT song_features_pkey PRIMARY KEY (song_features_id)
        );
        
        CREATE TABLE IF NOT EXISTS public.staging_charts (
                chart_title varchar(256),
                chart_name varchar(256),
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
        
        CREATE TABLE IF NOT EXISTS public.staging_lyrics (
                artist_name varchar(256),
                song_name varchar(256),
                lyrics varchar(65535),
                count_words int4,
                count_no_stopwords int4,
                count_distinct_words int4,
                count_distinct_no_stopwords int4,
                count_distinct_words_used_once int4,
                distinct_most_common varchar(256),
                count_most_common_usage int4,
                lyrics_sentiment numeric(8,7),
                common_words_sentiment numeric(8,7),
                common_words_sentiment_with_weights numeric(8,7)
        );
    """)
    
    chart_table_insert = ("""
        SELECT
               md5( concat( song_name, concat(chart_title, chart_year) ) ) AS chart_song_id,
               md5( concat(song_name, artist_name) ) AS song_id,
               md5( artist_name ) AS artist_id,
               song_rank AS rank,
               chart_name,
               chart_year AS year
        FROM staging_charts
    """)

    song_table_insert = ("""
        SELECT
               md5( concat(title, artist_name) ) AS song_id,
               title,
               md5( artist_name ) AS artist_id,
               year,
               duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT
               md5( artist_name ) AS artist_id,
               artist_name,
               artist_location,
               artist_latitude,
               artist_longitude
        FROM staging_songs
    """)

    lyrics_table_insert = ("""
        SELECT
               md5( lyrics ) AS lyrics_id,
               md5( artist_name ) AS artist_id,
               md5( concat(song_name, artist_name) ) AS song_id,
               count_words,
               count_no_stopwords,
               count_distinct_words,
               count_distinct_no_stopwords,
               count_distinct_words_used_once,
               distinct_most_common,
               count_most_common_usage,
               lyrics_sentiment,
               common_words_sentiment,
               common_words_sentiment_with_weights
        FROM staging_lyrics
    """)

    song_feature_table_insert = ("""
        SELECT
               md5( spotify_id ) AS song_features_id,
               md5( artist_name ) AS artist_id,
               md5( concat(song_name, artist_name) ) AS song_id,
               end_of_fade_in AS fade_in,
               ( duration - start_of_fade_out ) AS fade_out,
               loudness,
               tempo,
               tempo_confidence,
               key,
               key_confidence,
               mode,
               mode_confidence,
               danceability,
               energy,
               speechiness,
               acousticness,
               instrumentalness,
               liveness,
               valence,
               time_signature
        FROM staging_features
    """)

    select_nulls_count = (["""
        SELECT SUM(CASE WHEN (song_id IS NULL OR artist_id IS NULL)THEN 1 ELSE 0 END) FROM charts
    ""","""
        SELECT COUNT(*)
        FROM (
            SELECT *
            FROM charts
            LEFT OUTER JOIN artists
             ON charts.artist_id = artists.artist_id
        ) missing_artists_on_charts
    ""","""
        SELECT COUNT(*)
        FROM (
            SELECT *
            FROM charts
            LEFT OUTER JOIN songs
             ON charts.song_id = songs.song_id
        ) missing_songs_on_charts
    ""","""
        SELECT COUNT(*)
        FROM (
            SELECT *
            FROM lyrics
            LEFT OUTER JOIN artists
             ON lyrics.artist_id = artists.artist_id
        ) missing_artists_on_lyrics
    ""","""
        SELECT COUNT(*)
        FROM (
            SELECT *
            FROM lyrics
            LEFT OUTER JOIN songs
             ON lyrics.song_id = songs.song_id
        ) missing_songs_on_lyrics
    ""","""
        SELECT COUNT(*)
        FROM (
            SELECT *
            FROM song_features
            LEFT OUTER JOIN artists
             ON song_features.artist_id = artists.artist_id
        ) missing_artists_on_song_features
    ""","""
        SELECT COUNT(*)
        FROM (
            SELECT *
            FROM song_features
            LEFT OUTER JOIN songs
             ON song_features.song_id = songs.song_id
        ) missing_songs_on_song_features
    """])
