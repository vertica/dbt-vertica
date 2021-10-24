from contextlib import contextmanager
from dataclasses import dataclass
import ssl
import os
import requests
from typing import Optional


from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.logger import GLOBAL_LOGGER as logger
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
    withMaterialization: bool = False
    ssl_env_cafile: Optional[str] = None
    ssl_uri: Optional[str] = None

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
        return ('host','port','database','username','schema')


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
                'connection_load_balance': True,
                'session_label': f'dbt_{credentials.username}',
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

            handle = vertica_python.connect(**conn_info)
            connection.state = 'open'
            connection.handle = handle
            logger.debug(f':P Connected to database: {credentials.database} at {credentials.host}')

        except Exception as exc:
            logger.debug(f':P Error connecting to database: {exc}')
            connection.state = 'fail'
            connection.handle = None
            raise dbt.exceptions.FailedToConnectException(str(exc))

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

        return connection

    @classmethod
    def get_response(cls, cursor):
        code = cursor.description
        rows = cursor.rowcount

        return AdapterResponse(
            _message="{} {}".format(code, rows),
            rows_affected=rows,
            code=code
        )

    def cancel(self, connection):
        logger.debug(':P Cancel query')
        connection.handle.cancel()

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except vertica_python.DatabaseError as exc:
            logger.debug(f':P Database error: {exc}')
            self.release()
            raise dbt.exceptions.DatabaseException(str(exc))
        except Exception as exc:
            logger.debug(f':P Error: {exc}')
            self.release()
            raise dbt.exceptions.RuntimeException(str(exc))

