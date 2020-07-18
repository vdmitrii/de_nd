from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'
    insert_sql_statement = """
        INSERT INTO {table} 
        {sql_statement}
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table = '',
                 sql_query = '', 
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        self.log.info('LoadFactOperator is ready to go')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info(f"Copying data into {self.table}")
        redshift.run(LoadFactOperator.insert_sql_statement.format(
            table=self.table,
            select_query=self.sql_query
        ))