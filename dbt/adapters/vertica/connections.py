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
    schema: str
    username: str
    password: str
    port: int = 5433
    timeout: int = 3600
    withMaterialization: bool = False


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
                'connection_timeout': credentials.timeout,
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


