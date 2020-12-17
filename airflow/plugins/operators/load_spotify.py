"""
The load Spotify operator receives a parameter defining an SQL query to get the list of song and artist names whose features will be queried.

There is also an optional parameter that allows switching between insert modes when loading the data. The default behaviour is append-only.
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

class LoadSpotifyOperator(BaseOperator):
    sql_query = """
        INSERT INTO "{}" (spotify_id,artist_name,song_name,loudness,tempo,key,mode,danceability,energy,speechiness,acousticness,instrumentalness,liveness,valence,duration,end_of_fade_in,start_of_fade_out,tempo_confidence,key_confidence,mode_confidence,time_signature)
        VALUES {}
    """
    ui_color = '#1DB954'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 delete_before_insert=False,
                 to_table="",
                 chart_name="",
                 skip=False,
                 spotify_client_id="",
                 spotify_client_secret="",
                 select_sql="",
                 *args, **kwargs):
        super(LoadSpotifyOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.delete_before_insert = delete_before_insert
        self.to_table = to_table
        self.chart_name = chart_name
        self.skip_task = skip
        self.spotify_client_id = spotify_client_id
        self.spotify_client_secret = spotify_client_secret
        self.select_sql = select_sql
        
    def execute(self, context):
        if self.skip_task == True:
            return
        
        sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
            client_id=self.spotify_client_id,
            client_secret=self.spotify_client_secret
        ))

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.year = context.get('execution_date').year
        
        if(self.delete_before_insert == True):
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.to_table))

        self.log.info("Getting songs to query their features")
        select_query = """
           SELECT
                  artist_name AS artist,
                  song_name AS song
             FROM staging_charts
            WHERE chart_year = '{}'
              AND chart_title = '{}'
        """.format(self.year,self.chart_name.replace("'","\\'"))
        song_list = redshift.get_records(select_query)
        self.log.info("Querying features for {} tracks".format(len(song_list)))
        
        spotify_ids = []
        for song in song_list:
            spotify_songs = sp.search(q='{} - {}'.format(song[1],song[0]), limit=1)
            for idx, track in enumerate(spotify_songs['tracks']['items']):
                self.log.info('Found {} ({}) : {} - {}'.format(track['id'],idx,track['name'],track['artists'][0]['name']))
                spotify_ids.append(track['uri'])

        def chunks(list, size):
            """Yield successive `size`-sized chunks from list."""
            for i in range(0, len(list), size):
                yield list[i:i + size]

        self.log.info("Getting features from Spotify")
        song_features = []
        for tracks_block in chunks(spotify_ids, 100):
            features = sp.audio_features(tracks_block)
            for feature in features:
                analysis = sp.audio_analysis(feature['uri'])
                song_features.append({
                    'spotify_id': feature['id'],
                    'loudness': feature['loudness'],
                    'tempo': feature['tempo'],
                    'key': feature['key'],
                    'mode': feature['mode'],
                    'danceability': feature['danceability'],
                    'energy': feature['energy'],
                    'speechiness': feature['speechiness'],
                    'acousticness': feature['acousticness'],
                    'instrumentalness': feature['instrumentalness'],
                    'liveness': feature['liveness'],
                    'valence': feature['valence'],
                    'duration': analysis['track']['duration'],
                    'end_of_fade_in': analysis['track']['end_of_fade_in'],
                    'start_of_fade_out': analysis['track']['start_of_fade_out'],
                    'tempo_confidence': analysis['track']['tempo_confidence'],
                    'key_confidence': analysis['track']['key_confidence'],
                    'mode_confidence': analysis['track']['mode_confidence'],
                    'time_signature': analysis['track']['time_signature'],
                })
            
        self.log.info("Copying data to table")
        data_to_insert = [ ("("+",".join((2+len(feature))*["'{}'"])+")").format(
            feature['spotify_id'],
            song[0].replace("'","\\'"),
            song[1].replace("'","\\'"),
            feature['loudness'],
            feature['tempo'],
            feature['key'],
            feature['mode'],
            feature['danceability'],
            feature['energy'],
            feature['speechiness'],
            feature['acousticness'],
            feature['instrumentalness'],
            feature['liveness'],
            feature['valence'],
            feature['duration'],
            feature['end_of_fade_in'],
            feature['start_of_fade_out'],
            feature['tempo_confidence'],
            feature['key_confidence'],
            feature['mode_confidence'],
            feature['time_signature'],
        ) for (feature,song) in zip(song_features,song_list) ]
        formatted_sql = self.sql_query.format(
            self.to_table,
            ','.join(data_to_insert)
        )
        redshift.run(formatted_sql)