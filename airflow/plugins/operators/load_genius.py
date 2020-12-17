"""
The load Genius operator receives a parameter defining an SQL query to get the list of song and artist names whose features will be queried.

There is also an optional parameter that allows switching between insert modes when loading the data. The default behaviour is append-only.
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from lyricsgenius import Genius
import nltk
import re

class LoadGeniusOperator(BaseOperator):
    sql_query = """
        INSERT INTO "{}" (artist_name,song_name,lyrics)
        VALUES {}
    """
    ui_color = '#FFFF64'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 delete_before_insert=False,
                 to_table="",
                 chart_name="",
                 skip=False,
                 genius_access_token="",
                 select_sql="",
                 *args, **kwargs):
        super(LoadGeniusOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.delete_before_insert = delete_before_insert
        self.to_table = to_table
        self.chart_name = chart_name
        self.skip_task = skip
        self.genius_access_token = genius_access_token
        self.select_sql = select_sql
        
    def execute(self, context):
        if self.skip_task == True:
            return

        genius = Genius(self.genius_access_token)
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.year = context.get('execution_date').year
        
        if(self.delete_before_insert == True):
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.to_table))

        self.log.info("Getting songs to query their lyrics")
        select_query = """
           SELECT
                  artist_name AS artist,
                  song_name AS song
             FROM staging_charts
            WHERE chart_year = '{}'
              AND chart_title = '{}'
LIMIT 10 -- TODO remover limit
        """.format(self.year,self.chart_name.replace("'","\\'"))
        song_list = redshift.get_records(select_query)
        self.log.info("Querying lyrics for {} tracks".format(len(song_list)))

        song_lyrics = []
        for song in song_list:
            try:
                genius_song = genius.search_song(song[1],song[0])
            except Exception as e:
                self.log.error(e)
            if genius_song is not None:
                self.log.info('Found : {} - {}'.format(genius_song.title,genius_song.artist))

                """
                sentence = genius_song.lyrics
                sentence = re.sub(r'\[.*\]','',sentence)
                sentence = re.sub(r'\(.*\)','',sentence)
                tokens = nltk.word_tokenize(sentence)
                tagged = nltk.pos_tag(tokens)
                entities = nltk.chunk.ne_chunk(tagged)
                """
                
                song_lyrics.append({
                    'artist': genius_song.artist,
                    'song': genius_song.title,
                    'lyrics': genius_song.lyrics.replace("'","\\'"),
                })

        self.log.info("Copying data to table")
        data_to_insert = [ ("("+",".join((len(song))*["'{}'"])+")").format(
            song['artist'],
            song['song'],
            song['lyrics'][0:65535],
        ) for song in song_lyrics ]
        formatted_sql = self.sql_query.format(
            self.to_table,
            ','.join(data_to_insert)
        )
        redshift.run(formatted_sql)
