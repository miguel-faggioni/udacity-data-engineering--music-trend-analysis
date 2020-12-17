"""
The load Billboard operator receives a parameter defining which billboard chart to be loaded, as well as from which year to load.

There is also an optional parameter that allows switching between insert modes when loading the data. The default behaviour is append-only.
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import billboard

class LoadBillboardOperator(BaseOperator):
    sql_query = """
        INSERT INTO "{}" (song_rank,song_name,artist_name,chart_year,chart_title)
        VALUES {}
    """
    ui_color = '#B19FBD'

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
        data_to_insert = [ "('{}','{}','{}','{}','{}')".format(
            entry.rank,
            entry.title.replace("'","\\'"),
            entry.artist.replace("'","\\'"),
            chart.year,
            chart.name.replace("'","\\'")
        ) for entry in chart ]
        formatted_sql = self.sql_query.format(
            self.to_table,
            ','.join(data_to_insert)
        )
        redshift.run(formatted_sql)
