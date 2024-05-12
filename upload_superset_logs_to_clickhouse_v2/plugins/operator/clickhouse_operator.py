import logging
from airflow.models import BaseOperator
from plugins.hooks.clickhouse_hook import ClickHouseHook
from typing import *


class ClickHouseOperator(BaseOperator):

    # Включение шаблонизации 
    # (включение шаблонизации для sql_file приведет к ошибке в существующих дагах,
    # jinja не найдет шаблон файла)
    template_fields: Sequence[str] = ("custom_template_fields",)

    def __init__(
        self,
        clickhouse_conn_id: str,
        sql: str = None,
        sql_file: str = None,
        multy_sql: str = None,
        params: Optional[Dict[str, Any]] = None,
        custom_template_fields: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs,
    ):
        if not sql and not sql_file and not multy_sql:
            raise Exception("sql or sql_file or sql_file_simple must be set in ClickhouseOperator")
        else:
            super().__init__(*args, **kwargs)
            self.conn_id = clickhouse_conn_id
            self.sql = sql
            self.multy_sql = multy_sql
            self.sql_file = sql_file
            self.params = params
            self.custom_template_fields = custom_template_fields

    def replace_params(self, sql):
        if self.params:
            for key, value in self.params.items():
                sql = sql.replace('{{ params.%s }}' % (str(key)), str(value))
        if self.custom_template_fields:
            for key, value in self.custom_template_fields.items():
                sql = sql.replace('{{ custom_template_fields.%s }}' % (str(key)), str(value))
        return sql

    def execute_multy_sql(self, clickhouse_hook):
        try:
            sql_file_str = self.replace_params(self.multy_sql)
            logging.info(sql_file_str)
            sql_list = sql_file_str.split(';')
            sql_list = [x for x in sql_list if x]
            for sql in sql_list:
                if sql.strip():
                    clickhouse_hook.run(sql)
        except FileNotFoundError:
            raise Exception(f"sql_file {self.sql_file} does not exist!")

    def execute_sql_from_file(self, clickhouse_hook):
        try:
            fd = open(self.sql_file, 'r', encoding='utf-8')
            self.multy_sql = fd.read()
            fd.close()
        except FileNotFoundError:
            raise Exception(f"sql_file {self.sql_file} does not exist!")
        self.execute_multy_sql(clickhouse_hook)

    def execute(self, context):
        clickhouse_hook = ClickHouseHook(self.conn_id)
        if self.sql_file:
            self.execute_sql_from_file(clickhouse_hook)
        elif self.sql:
            clickhouse_hook.run(self.replace_params(self.sql))
        elif self.multy_sql:
            self.execute_multy_sql(clickhouse_hook)