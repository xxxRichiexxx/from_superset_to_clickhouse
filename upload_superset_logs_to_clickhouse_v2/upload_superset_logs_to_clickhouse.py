from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from plugins.operator.clickhouse_operator import ClickHouseOperator
from plugins.hooks.clickhouse_hook import ClickHouseHook
from includes.scripts.python.on_callback_airflow import on_callback, on_callback_success
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from includes.scripts.python.functions.environment import get_environment_clickhouse
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
import os


dir_path = os.path.dirname(__file__)
_environment = Variable.get('airflow_environment').lower()
clickhouse_env = get_environment_clickhouse(_environment)
clickhouse_hook = ClickHouseHook(clickhouse_env['cluster_connection'])

doc_md = """
    Данный DAG загружает данные таблиц logs, ab_user, dashboards из Superset в Clickhouse.
    По умолчанию используется подключение superset. Для сбора данных из старого Superset'а
    необходимо при запуске дага указать:

        {"superset_connection": "superset_old"}

"""

dag_default_args = {
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'owner': 'bi',
    'catchup': False,
    'on_retry_callback': on_callback,
    'on_success_callback': on_callback_success,
}

tasks_default_args = {
    'clickhouse_conn_id': clickhouse_env['cluster_connection']
}

def get_superset_connection(context):
    """
    Достает из параметров запуска дага подключение к Superset или использует
    значение по умолчанию (новый Superset)
    """
    superset_connection = context['dag_run'].conf.get('superset_connection')
    if not superset_connection:
        return BaseHook.get_connection('superset')
    return BaseHook.get_connection(superset_connection)
    

with DAG(
        dag_id='upload_superset_logs_to_clickhouse',
        description='Загрузка логов superset в clickhouse',
        schedule_interval='@daily',
        catchup=False,
        max_active_runs=1,
        default_args=dag_default_args,
        tags=['superset', 'postgres', 'clickhouse'],
        doc_md=doc_md,
        on_failure_callback=on_callback,
) as dag:

    start = EmptyOperator(task_id="start")

    # Создание таблиц-приемников в Clickhouse
    create_table_tasks = ClickHouseOperator(
        task_id=f'create_tables',
        sql_file=f'{dir_path}/sql/create_tables.sql',
        default_args=tasks_default_args,
        params={
            'cluster': clickhouse_env['cluster_name'],
        }
    )

    @task
    def create_ext_table_tasks(**context):
        """
        Создание внешних таблиц в Clickhouse для получения данных
        из Superset
        """
        superset_connection = get_superset_connection(context)

        with open(f'{dir_path}/sql/create_external_tables.sql') as file:
            query = file.read().\
                replace('{{ params.src_host }}', str(superset_connection.host)).\
                replace('{{ params.src_port }}', str(superset_connection.port)).\
                replace('{{ params.src_db }}', str(superset_connection.schema)).\
                replace('{{ params.src_login }}', str(superset_connection.login)).\
                replace('{{ params.src_password }}', str(superset_connection.password)).\
                replace('{{ params.cluster }}', str(clickhouse_env['cluster_name']))

        clickhouse_hook.run([item for item in query.split(';') if item])
    

    @task
    def upload_data(table, refrash_field, **context):
        """
        Загрузка данных через внешние таблицы в таблицы-приемники
        """

        superset_connection = get_superset_connection(context)

        query = f"""
            SELECT MAX({refrash_field})
            FROM bi.{table}_distributed;
        """

        batch_start_date = clickhouse_hook.get_records(query)[0][0]

        if not batch_start_date:
            batch_start_date = '2000-01-01'

        with open(f'{dir_path}/sql/{table}_upload_data.sql') as file:
            query = file.read().\
                replace('{{ params.batch_start_date }}', str(batch_start_date)).\
                replace('{{ params.src_db }}', superset_connection.schema).\
                replace('{{ params.refrash_field }}', refrash_field)
                
        clickhouse_hook.run(query)

    # Удаление старых логов в источнике
    @task
    def delete_old_logs(**context):

        superset_connection = get_superset_connection(context)
        if superset_connection.conn_id == 'superset_old':
            raise AirflowSkipException
        
        query = """
            DELETE FROM public.logs
            WHERE dttm < DATE_TRUNC('MONTH', NOW() - INTERVAL '30 MONTH')
        """
    
        PostgresOperator(
            task_id='delete_old_logs',
            postgres_conn_id=superset_connection.conn_id,
            sql=query,
        ).execute(context)

    # Удаление внешних таблиц в Clickhouse
    drop_external_tables = ClickHouseOperator(
        task_id='drop_external_tables',
        sql_file=f'{dir_path}/sql/drop_external_tables.sql',
        default_args=tasks_default_args,
        params={
            'cluster': clickhouse_env['cluster_name'],
        }
    )

    end = EmptyOperator(task_id="end")

    # Последовательность запуска задач
    start >> create_table_tasks >> create_ext_table_tasks() >> [
        upload_data.override(task_id=f'logs_upload_data')(
            'f_superset_logs',
            'dttm'
        ),
        upload_data.override(task_id=f'ab_user_upload_data')(
            'f_superset_ab_user',
            'changed_on'
        ),
        upload_data.override(task_id=f'dashboards_upload_data')(
            'f_superset_dashboards',
            'changed_on'
        ),
    ] >> delete_old_logs() >> drop_external_tables >> end