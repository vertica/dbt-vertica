# Copyright (c) [2018-2023]  Micro Focus or one of its affiliates.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.




from contextlib import contextmanager
from dataclasses import dataclass
import ssl
import os
import requests
from typing import Optional
from dbt.contracts.connection import AdapterResponse
from typing import List, Optional, Tuple, Any, Iterable, Dict, Union
import dbt.clients.agate_helper
import agate
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.events import AdapterLogger
logger = AdapterLogger("vertica")
from dbt.contracts.connection import AdapterResponse


import dbt.exceptions
import vertica_python


@dataclass
class verticaCredentials(Credentials):
    host: str
    database: str
    schema: str
    username: str
    password: str
    ssl: bool = False
    port: int = 5433
    timeout: int = 3600
    oauth_access_token: str = ""
    withMaterialization: bool = False
    ssl_env_cafile: Optional[str] = None
    ssl_uri: Optional[str] = None
    connection_load_balance: Optional[bool]= True
    retries:int  =  1
    backup_server_node: Optional[List[str]] = None
    # backup_server_node: Optional[str] = None

    # additional_info = {
    # 'password': str, 
    # 'backup_server_node': list# invalid value to be set in a connection string
    # }

    @property
    def type(self):
        return 'vertica'

    @property
    def unique_field(self):
        """
        Hashed and included in anonymous telemetry to track adapter adoption.
        Pick a field that can uniquely identify one team/organization building with this adapter
        """
        return self.host

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'
        return ('host','port','database','username','schema', 'connection_load_balance')

class verticaConnectionManager(SQLConnectionManager):
    TYPE = 'vertica'

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug(':P Connection is already open')
            return connection

        credentials = connection.credentials

        try:
            conn_info = {
                'host': credentials.host,
                'port': credentials.port,
                'user': credentials.username,
                'password': credentials.password,
                'database': credentials.database,
                'connection_timeout': credentials.timeout,
                'connection_load_balance':credentials.connection_load_balance,
                'session_label': f'dbt_{credentials.username}',
                'retries': credentials.retries,
                'oauth_access_token': credentials.oauth_access_token,
                'backup_server_node':credentials.backup_server_node,
                
            }

            # if credentials.ssl.lower() in {'true', 'yes', 'please'}:
            if credentials.ssl:
                if credentials.ssl_env_cafile is not None:
                    context = ssl.create_default_context(
                        cafile=os.environ.get(credentials.ssl_env_cafile),
                    )
                elif credentials.ssl_uri is not None:
                    resp = requests.get(credentials.ssl_uri)
                    resp.raise_for_status()
                    ssl_data = resp.content
                    context = ssl.create_default_context(
                        cadata=ssl_data.decode("ascii", "ignore")
                    )
                else:
                    context = ssl.create_default_context()
                conn_info['ssl'] = context
                logger.debug(f'SSL is on')
            
            def connect():
                handle = vertica_python.connect(**conn_info)
                logger.debug(f':P Connection work {handle}')
                connection.state = 'open'
                connection.handle = handle
                logger.debug(f':P Connected to database: {credentials.database} at {credentials.host} at {handle}')
                return handle
        
               


        except Exception as exc:
            logger.debug(f':P Error connecting to database: {exc}')
            connection.state = 'fail'
            connection.handle = None
            raise dbt.exceptions.DbtFailedToConnectErroe(str(exc))

        # This is here mainly to support dbt-integration-tests.
        # It globally enables WITH materialization for every connection dbt
        # makes to Vertica. (Defaults to False)
        # Normal usage would be to use query HINT or declare session parameter in model or hook,
        # but tests do not support hooks and cannot change tests from dbt_utils
        # used in dbt-integration-tests
        if credentials.withMaterialization:
            try:
                logger.debug(f':P Set EnableWithClauseMaterialization')
                cur = connection.handle.cursor()
                cur.execute("ALTER SESSION SET PARAMETER EnableWithClauseMaterialization=1")
                cur.close()

            except Exception as exc:
                logger.debug(f':P Could not EnableWithClauseMaterialization: {exc}')
                pass

        retryable_exceptions = [
        Exception,
        dbt.exceptions.FailedToConnectError
        ]

        return cls.retry_connection(
        connection,
        connect=connect,
        logger=logger,
        retry_limit=credentials.retries,
        retryable_exceptions=retryable_exceptions,
        )

    @classmethod
    def get_response(cls, cursor):
        code = cursor.description
        rows = cursor.rowcount
        message = cursor._message
        arraysize = cursor.arraysize
        operation = cursor.operation
        return AdapterResponse(
            _message="Operation: {}, Message: {}, Code: {}, Rows: {}, Arraysize: {}".format(operation, message, str(code), rows, arraysize),
            rows_affected=rows,
            code=str(code)
        )

    def cancel(self, connection):
        logger.debug(':P Cancel query')
        connection.handle.cancel()

    @classmethod
    def get_result_from_cursor(cls, cursor: Any, limit: Optional[int]) -> agate.Table:
        data: List[Any] = []
        column_names: List[str] = []

        if cursor.description is not None:
            column_names = [col[0] for col in cursor.description]
            if limit:
                rows = cursor.fetchmany(limit)
            else:
                rows = cursor.fetchall()
            # rows = cursor.fetchall()
            # check result for every query if there are some queries with ; separator
            while cursor.nextset():
                check = cursor._message
                if isinstance(check, ErrorResponse):
                    logger.debug(f'Cursor message is: {check}')
                    self.release()
                    raise dbt.exceptions.DbtDatabaseError(str(check))

            data = cls.process_results(column_names, rows)

        return dbt.clients.agate_helper.table_from_data_flat(data, column_names)

    def execute(
        self, sql: str, auto_begin: bool = False, fetch: bool = False, limit: Optional[int] = None
    ) -> Tuple[AdapterResponse, agate.Table]:
        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(sql, auto_begin)
        response = self.get_response(cursor)
        if fetch:
            table = self.get_result_from_cursor(cursor,limit)
        else:
            table = dbt.clients.agate_helper.empty_table()
            while cursor.nextset():
                check = cursor._message
                if isinstance(check, vertica_python.vertica.messages.ErrorResponse):
                    logger.debug(f'Cursor message is: {check}')
                    self.release()
                    raise dbt.exceptions.DbtDatabaseError(str(check))
        return response, table

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except vertica_python.DatabaseError as exc:
            logger.debug(f':P Database error: {exc}')
            self.release()
            raise dbt.exceptions.DbtDatabaseError(str(exc))
        except Exception as exc:
            logger.debug(f':P Error: {exc}')
            self.release()
            raise dbt.exceptions.DbtRuntimeError(str(exc))

    @classmethod
    def data_type_code_to_name(cls, type_code: Union[int, str]) -> str:
        assert isinstance(type_code, int)
        return vertica.connector.constants.FIELD_ID_TO_NAME[type_code]