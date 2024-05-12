import logging
from itertools import islice
from typing import *
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.hooks.base import BaseHook
from clickhouse_driver import Client
import pandas as pd

class ClickHouseHook(BaseHook):
    conn_name_attr = 'clickhouse_conn_id'
    default_conn_name = 'clickhouse_default'

    def __init__(
            self,
            clickhouse_conn_id: str = default_conn_name,
            database: Optional[str] = None,
            shara_id: int = 1,
            mode: str = 'cluster',
            cluster_name: str = 'ch-001',
    ):
        self.clickhouse_conn_id = clickhouse_conn_id
        self.database = database
        self.shara_id = shara_id
        self.mode = mode
        self.cluster_name = cluster_name

    def get_conn(self) -> Client:
        conn = self.get_connection(self.clickhouse_conn_id)
        connection_kwargs = conn.extra_dejson.copy()
        if conn.port:
            connection_kwargs.update(port=int(conn.port))
        if conn.login:
            connection_kwargs.update(user=conn.login)
        if conn.password:
            connection_kwargs.update(password=conn.password)
        if self.database:
            connection_kwargs.update(database=self.database)
        elif conn.schema:
            connection_kwargs.update(database=conn.schema)
        return Client(conn.host or 'localhost', **connection_kwargs)

    def get_shara_ip(self):
        sql = "SELECT host_address FROM system.clusters WHERE shard_num = {shara_id} limit 1;".format(
            shara_id=self.shara_id
        )
        return self.get_first(sql)

    def get_records(self, sql: str, parameters: Optional[dict] = None) -> List[Tuple]:
        self._log_query(sql, parameters)
        with disconnecting(self.get_conn()) as client:
            return client.execute(sql, params=parameters)

    def get_first(self, sql: str, parameters: Optional[dict] = None) -> Optional[Tuple]:
        self._log_query(sql, parameters)
        with disconnecting(self.get_conn()) as client:
            try:
                return next(client.execute_iter(sql, params=parameters))
            except StopIteration:
                return None

    def get_pandas_df(self, sql: str):
        import pandas as pd
        rows, columns_defs = self.run(sql, with_column_types=True)
        columns = [column_name for column_name, _ in columns_defs]
        return pd.DataFrame(rows, columns=columns)

    def import_pandas_df(self, sql: str, df: pd.DataFrame):
        with disconnecting(self.get_conn()) as client:
            client.execute(sql, df.to_dict('records'))

    def execute_clickhouse_server_command(command: str):
        try:
            logging.info('export_command: ' + command)
            ssh = SSHHook(ssh_conn_id='clickhouse_bi_server', cmd_timeout=None)
            ssh_client = ssh.get_conn()
            ssh_client.load_system_host_keys()
            stdin, stdout, stderr = ssh_client.exec_command(command)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status == 0:
                logging.info('SSH finished')
                z = stdout.read()
                logging.info('stdout: ')
                logging.info(z)
                return z
            else:
                z = stderr.read()
                raise Exception('SSH error status code ' + str(z))
        finally:
            if ssh_client:
                ssh_client.close()

    def import_csv(self, csv: str, table: str):
        conn = self.get_connection(self.clickhouse_conn_id)
        suf_sql = ' FORMAT CSVWithNames '
        if self.mode == 'cluster':
            suf_sql = '_replicated ' + suf_sql + ' ON CLUSTER {cluster_name} '.format(cluster_name=self.cluster_name)
        logging.info(csv)
        command = 'clickhouse-client -h {host} --port 9000 -u {user} --password {password} -d {database} ' \
                  '--format_csv_delimiter=";" --format_csv_allow_double_quotes=0 --format_csv_allow_single_quotes=0  ' \
                  '--receive_timeout=3600 --input_format_tsv_empty_as_default=1 --format_csv_null_representation=NULL ' \
                  '--send_timeout=3600 --connect_timeout=3600 --query ' \
                  '"INSERT INTO bi.process_{table}{suf_sql} " < {csv}'.format(
            host=self.get_shara_ip(),
            user=conn.login,
            password=conn.password.replace('$', '\$'),
            database=conn.schema,
            table=table,
            suf_sql=suf_sql,
            csv=csv,
        )
        logging.info('Command: ' + command)
        self.execute_clickhouse_server_command(command)

    def run(
            self,
            sql: Union[str, Iterable[str]],
            parameters: Union[None, dict, list, tuple, Generator] = None,
            with_column_types: bool = False,
    ) -> Any:
        if isinstance(sql, str):
            sql = (sql,)
        with disconnecting(self.get_conn()) as conn:
            last_result = None
            for s in sql:
                self._log_query(s, parameters)
                last_result = conn.execute(
                    s,
                    params=parameters,
                    with_column_types=with_column_types,
                )

        return last_result

    def _log_query(
            self,
            sql: str,
            parameters: Union[None, dict, list, tuple, Generator],
    ) -> None:
        self.log.info(
            '%s%s', sql,
            f' with {self._log_params(parameters)}' if parameters else '',
        )

    @staticmethod
    def _log_params(
            parameters: Union[dict, list, tuple, Generator],
            limit: int = 10,
    ) -> str:
        if isinstance(parameters, Generator) or len(parameters) <= limit:
            return str(parameters)
        if isinstance(parameters, dict):
            head = dict(islice(parameters.items(), limit))
        else:
            head = parameters[:limit]
        head_str = str(head)
        closing_paren = head_str[-1]
        return f'{head_str[:-1]} … and {len(parameters) - limit} ' \
               f'more parameters{closing_paren}'


_InnerT = TypeVar('_InnerT')


class disconnecting(ContextManager, Generic[_InnerT]):
    """ Context to automatically disconnect something at the end of a block.
    Similar to contextlib.closing but calls .disconnect() method on exit.
    """

    def __init__(self, thing: _InnerT):
        self._thing = thing

    def __enter__(self) -> _InnerT:
        return self._thing

    def __exit__(self, *exc_info) -> None:
        self._thing.disconnect()
