from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import LoadDimensionOperator
from helpers import SqlQueries


def load_dim_to_redshift_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        table,
        truncate_table,
        sql_query,
        *args, **kwargs):
    dag = DAG(f"{parent_dag_name}.{task_id}", **kwargs)

    load_table = LoadDimensionOperator(
        task_id = f"Load_{table}_table",
        dag = dag,
        table = table,
        redshift_conn_id = redshift_conn_id,
        truncate_table = truncate_table,
        sql_query = sql_query
    )

    return dag