from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # The DAG allows to switch between append-only and delete-load functionality
                 redshift_conn_id = '',
                 table = '',
                 sql_query = '',
                 truncate_table = '',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate_table = truncate_table

    def execute(self, context):
        self.log.info('LoadDimensionOperator is ready to go')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        if self.truncate_table: 
            self.log.info('Truncating table...')
            redshift.run(f"TRUNCATE TABLE {self.table}")     
        formatted_sql = self.sql_query.format(self.table)
        self.log.info('Running query...')
        redshift.run(formatted_sql)
        self.log.info(f"Success: {self.task_id}")
