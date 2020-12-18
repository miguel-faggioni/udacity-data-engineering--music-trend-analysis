"""
The load Genius operator receives a parameter defining an SQL query to get the list of song and artist names whose features will be queried.

There is also an optional parameter that allows switching between insert modes when loading the data. The default behaviour is append-only.
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from lyricsgenius import Genius
import re
import collections
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
nltk.download('stopwords')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
nltk.download('vader_lexicon')

class LoadGeniusOperator(BaseOperator):
    sql_query = """
        INSERT INTO "{}" (artist_name,song_name,lyrics,count_words,count_no_stopwords,count_distinct_words,count_distinct_no_stopwords,count_distinct_words_used_once,distinct_most_common,count_most_common_usage,lyrics_sentiment,common_words_sentiment,common_words_sentiment_with_weights)

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
                 most_common_count=1,
                 select_limit=None,
                 *args, **kwargs):
        super(LoadGeniusOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.delete_before_insert = delete_before_insert
        self.to_table = to_table
        self.chart_name = chart_name
        self.skip_task = skip
        self.genius_access_token = genius_access_token
        self.select_sql = select_sql
        self.most_common_count = most_common_count
        self.select_limit = select_limit
        
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
        if self.select_limit is not None:
            select_limit = "LIMIT {}".format(self.select_limit)
        else:
            select_limit = ""
        select_query = """
           SELECT
                  artist_name AS artist,
                  song_name AS song
             FROM staging_charts
            WHERE chart_year = '{}'
              AND chart_title = '{}'
            {}
        """.format(self.year,self.chart_name.replace("'","\\'"),select_limit)
        song_list = redshift.get_records(select_query)
        self.log.info("Querying lyrics for {} tracks".format(len(song_list)))

        sid = SentimentIntensityAnalyzer()
        song_lyrics = []
        for song in song_list:
            try:
                genius_song = genius.search_song(song[1],song[0])
            except Exception as e:
                self.log.error(e)
            if genius_song is not None:
                self.log.info('Found : {} - {}'.format(genius_song.title,genius_song.artist))

                # get the lyrics
                sentence = genius_song.lyrics
                # remove words between [] and ()
                sentence = re.sub(r'\[.*\]','',sentence)
                sentence = re.sub(r'\(.*\)','',sentence)
                # tokenize
                tokens = nltk.word_tokenize(sentence)
                # tokenize without punctuation and converting to lowercase
                lower_alpha_tokens = [ w for w in word_tokenize(sentence.lower()) if w.isalpha()]
                # remove stopwords
                no_stops = [t for t in lower_alpha_tokens if t not in stopwords.words('english')]
                # get stopwords removed
                only_stops = [ x for x in lower_alpha_tokens if x not in no_stops ]
                # count
                count_words = len(lower_alpha_tokens)
                count_stopwords = len(only_stops)
                count_no_stopwords = len(no_stops)
                count_distinct_words = len(collections.Counter(lower_alpha_tokens))
                count_distinct_no_stopwords = len(collections.Counter(no_stops))
                count_distinct_words_used_once = len([ x[1] for x in collections.Counter(no_stops).most_common() if x[1] == 1 ])
                # get most frequent words
                distinct_most_common = [ '{}:{}'.format(x[0],x[1]) for x in collections.Counter(no_stops).most_common(self.most_common_count) ]
                count_most_common_usage = sum([ x[1] for x in collections.Counter(no_stops).most_common(self.most_common_count) ])
                # analyse sentiment
                lyrics_sentiment = sid.polarity_scores(sentence)
                common_words_sentiment = sid.polarity_scores(' '.join(distinct_most_common))
                common_words_sentiment_with_weights = sid.polarity_scores(' '.join([ ' '.join([x[0]]*x[1]) for x in collections.Counter(no_stops).most_common(self.most_common_count) ]))
                
                song_lyrics.append({
                    'artist': genius_song.artist,
                    'song': genius_song.title,
                    'lyrics': genius_song.lyrics.replace("'","\\'"),
                    'count_words': count_words,
                    'count_no_stopwords': count_no_stopwords,
                    'count_distinct_words': count_distinct_words,
                    'count_distinct_no_stopwords': count_distinct_no_stopwords,
                    'count_distinct_words_used_once': count_distinct_words_used_once,
                    'distinct_most_common': ','.join(distinct_most_common),
                    'count_most_common_usage': count_most_common_usage,
                    'lyrics_sentiment': lyrics_sentiment['compound'],
                    'common_words_sentiment': common_words_sentiment['compound'],
                    'common_words_sentiment_with_weights': common_words_sentiment_with_weights['compound'],
                })

        self.log.info("Copying data to table")
        data_to_insert = [ ("("+",".join((len(song))*["'{}'"])+")").format(
            song['artist'],
            song['song'],
            song['lyrics'][0:65535],
            song['count_words'],
            song['count_no_stopwords'],
            song['count_distinct_words'],
            song['count_distinct_no_stopwords'],
            song['count_distinct_words_used_once'],
            song['distinct_most_common'],
            song['count_most_common_usage'],
            song['lyrics_sentiment'],
            song['common_words_sentiment'],
            song['common_words_sentiment_with_weights'],
        ) for song in song_lyrics ]
        formatted_sql = self.sql_query.format(
            self.to_table,
            ','.join(data_to_insert)
        )
        redshift.run(formatted_sql)
