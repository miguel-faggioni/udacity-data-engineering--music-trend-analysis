"""
The data quality operator runs checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result are checked and if there is no match, the operator raises an exception.
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_queries=[],
                 expected_values=[],
                 continue_after_fail=False,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        if len(sql_queries) != len(expected_values):
            raise ValueError('Length of `sql_queries` and `expected_values` params must be equal')
        self.sql_queries = sql_queries
        self.expected_values = expected_values
        self.continue_after_fail = continue_after_fail
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        errors = []
        for check in zip(self.sql_queries,self.expected_values):
            self.log.info('Expecting {} as result from query: {}'.format(check[1], check[0]))
            result = redshift.get_records(check[0])
            self.log.info('Got: {}'.format(result))
            if result != check[1]:
                error = 'Data quality check failed. Expected {} but got {}'.format(check[1],result)
                if self.continue_after_fail == True:
                    errors.append(error)
                else:
                    raise ValueError(error)

        if errors:
            raise ValueError(errors)

        self.log.info('Data quality check passed.')
