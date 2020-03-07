from dbt.adapters.sql import SQLAdapter
from dbt.adapters.vertica import verticaConnectionManager


class verticaAdapter(SQLAdapter):
    ConnectionManager = verticaConnectionManager

    @classmethod
    def date_function(cls):
        return 'sysdate'

