from contextlib import contextmanager
from dataclasses import dataclass

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.logger import GLOBAL_LOGGER as logger
import dbt.exceptions

import vertica_python


@dataclass
class verticaCredentials(Credentials):
    host: str
    database: str
    port: int
    schema: str
    username: str
    password: str


    @property
    def type(self):
        return 'vertica'

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'
        return ('host','port','database','username', 'schema')


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
                'connection_timeout': 10,
                'connection_load_balance': True,
                'session_label': f'dbt_{credentials.username}',
            }

            handle = vertica_python.connect(**conn_info)
            connection.state = 'open'
            connection.handle = handle
            logger.debug(f':P Connected to database: {credentials.database} at {credentials.host}')

        except Exception as exc:
            logger.debug(f':P Error connecting to database: {exc}')
            connection.state = 'fail'
            connection.handle = None
            raise dbt.exceptions.FailedToConnectException(str(exc))

        return connection

    @classmethod
    def get_status(cls, cursor):
        return str(cursor.rowcount)

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


