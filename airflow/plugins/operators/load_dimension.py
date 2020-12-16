"""
The load fact operator received an SQL query to run data transformations. Most of the logic is within the SQL transformations and the operator also received the target database on which to run the query against. The target table that will contain the results of the transformation is also passed as a parameter to the operator.

There is also an optional parameter that allows switching between insert modes when loading dimensions. The default behaviour is append-only.
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    sql_query = """
        INSERT INTO "{}"
        {}
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 delete_before_insert=False,
                 to_table="",
                 sql_select="",
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.delete_before_insert = delete_before_insert
        self.sql_select = sql_select
        self.to_table = to_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if(self.delete_before_insert == True):
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.to_table))

        self.log.info("Copying dimensions to table")
        formatted_sql = self.sql_query.format(
            self.to_table,
            self.sql_select
        )
        redshift.run(formatted_sql)
