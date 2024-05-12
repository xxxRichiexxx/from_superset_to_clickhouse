from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from plugins.operator.clickhouse_operator import ClickHouseOperator
from plugins.hooks.clickhouse_hook import ClickHouseHook
from includes.scripts.python.on_callback_airflow import on_callback, on_callback_success
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
import os
import logging


superset_conn = BaseHook.get_connection('new_superset')

default_args = {
    'depends_on_past': False,
    # 'on_failure_callback': on_callback,
    # 'on_retry_callback': on_callback,
    # 'on_success_callback': on_callback_success,
    'start_date': datetime(2024, 1, 1),
    'owner': 'bi',
    'catchup': False,
}

dir_path = os.path.dirname(__file__)

with DAG(
        dag_id='upload_superset_logs_to_clickhouse_v1',
        description='Загрузка логов superset в clickhouse',
        schedule_interval='@daily',
        catchup=False,
        max_active_runs=1,
        default_args=default_args,
        tags=['superset', 'postgres', 'clickhouse'],
) as dag:

    start = EmptyOperator(task_id="start")

    create_table = ClickHouseOperator(
        task_id='create_table',
        clickhouse_conn_id='clickhouse-bi-cluster-production',
        sql_file=f'{dir_path}/sql/create_table.sql',
    )

    create_processed_table = ClickHouseOperator(
        task_id='create_processed_table',
        clickhouse_conn_id='clickhouse-bi-cluster-production',
        sql_file=f'{dir_path}/sql/create_processed_table.sql',
        params={
            'src_host': superset_conn.host,
            'src_port': superset_conn.port,
            'src_bd': superset_conn.schema,
            'src_table': 'logs',
            'src_login': superset_conn.login,
            'src_password': superset_conn.password,
        }
    )

    @task
    def shard_upload(con_id, src_bd, where_conditions):

        clickhouse_hook = ClickHouseHook(con_id)

        query = """
            SELECT DATE_TRUNC('MONTH', MAX(dttm))
            FROM bi.f_superset_logs_replicated;
        """

        batch_start_date = clickhouse_hook.get_records(query)[0][0]

        if not batch_start_date:
            batch_start_date = '2000-01-01'

        with open(f'{dir_path}/sql/upload_to_processed_table.sql') as file:
            query = file.read().\
                replace('{{ params.batch_start_date }}', str(batch_start_date)).\
                replace('{{ params.src_bd }}', src_bd).\
                replace('{{ params.where_conditions }}', where_conditions)
                
        clickhouse_hook.run(query)
        

    @task
    def change_partitions():

        clickhouse_hook = ClickHouseHook('clickhouse-bi-cluster-production')

        query = """
            SELECT "partition"
            FROM system.parts
            WHERE database = 'bi' AND table = 'f_superset_logs_processed'
            GROUP BY "partition";
        """
        partitions = clickhouse_hook.get_records(query)

        if partitions:
            for partition in partitions[0]:
                query = f"""
                    ALTER TABLE bi.f_superset_logs_replicated ON cluster  'ch-001'
                    REPLACE PARTITION '{partition}' FROM bi.f_superset_logs_processed;
                """           
                clickhouse_hook.run(query)
        else:
            logging.info('Партиции для обновленя отсутствуют')

    drop_processed_table = ClickHouseOperator(
        task_id='drop_processed_table',
        clickhouse_conn_id='clickhouse-bi-cluster-production',
        sql_file=f'{dir_path}/sql/drop_processed_table.sql',
    )

    end = EmptyOperator(task_id="end")

    start >> create_table >> create_processed_table
    create_processed_table >> [
        shard_upload(
            'clickhouse-bi-cluster-production_shard_1',
            superset_conn.schema,
            "id%2 = 0"
        ),
        shard_upload(
            'clickhouse-bi-cluster-production_shard_2',
            superset_conn.schema,
            "id%2 <> 0"
        ),
    ] >> change_partitions() >> drop_processed_table >> end