"""
The LoadBillboardOperator gets the `execution_year` from the context, and a Billboard chart name from the parameters.

The operator then uses the `billboard` python lib to get the rankings for the given year and chart.

The rankings are inserted into a table whose name is received as a parameter.

There is also an optional parameter that allows switching between insert modes when loading the data. The default behaviour is append-only.
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import billboard

class LoadBillboardOperator(BaseOperator):
    sql_query = """
        INSERT INTO "{}" (song_rank,song_name,artist_name,chart_year,chart_title,chart_name)
        VALUES {}
    """
    ui_color = '#4F5980'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 delete_before_insert=False,
                 to_table="",
                 chart_name="",
                 skip=False,
                 *args, **kwargs):
        super(LoadBillboardOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.delete_before_insert = delete_before_insert
        self.to_table = to_table
        self.chart_name = chart_name
        self.skip_task = skip
        
    def execute(self, context):
        if self.skip_task == True:
            return

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.year = context.get('execution_date').year
        
        if(self.delete_before_insert == True):
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {} WHERE chart_year = '{}' AND chart_title = '{}'".format(self.to_table,self.year,chart.title.replace("'","\\'")))

        self.log.info("Getting chart from Billboard")
        chart = billboard.ChartData(self.chart_name,year=self.year)
        
        self.log.info("Copying data to table")
        data_to_insert = [ "('{}','{}','{}','{}','{}','{}')".format(
            entry.rank,
            entry.title.replace("'","\\'"),
            entry.artist.replace("'","\\'"),
            chart.year,
            chart.name.replace("'","\\'"),
            chart.title.replace("'","\\'")
        ) for entry in chart ]
        formatted_sql = self.sql_query.format(
            self.to_table,
            ','.join(data_to_insert)
        )
        redshift.run(formatted_sql)
