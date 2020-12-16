"""
The load fact operator received an SQL query to run data transformations. Most of the logic is within the SQL transformations and the operator also received the target database on which to run the query against. The target table that will contain the results of the transformation is also passed as a parameter to the operator.
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    sql_query = """
        INSERT INTO "{}"
        {}
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 to_table="",
                 sql_select="",
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_select = sql_select
        self.to_table = to_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Copying facts to table")
        formatted_sql = self.sql_query.format(
            self.to_table,
            self.sql_select
        )
        redshift.run(formatted_sql)
