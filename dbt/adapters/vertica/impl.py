from dbt.adapters.sql import SQLAdapter
from dbt.adapters.vertica import verticaConnectionManager

import agate

class verticaAdapter(SQLAdapter):
    ConnectionManager = verticaConnectionManager

    @classmethod
    def date_function(cls):
        return 'sysdate'

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        column = agate_table.columns[col_idx]
        lens = [len(d.encode("utf-8")) for d in column.values_without_nulls()]
        max_len = max(lens) if lens else 64
        return "varchar({})".format(max_len)

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "numeric(18,{})".format(decimals) if decimals else "integer"

    def run_sql_for_tests(self, sql, fetch, conn):
        cursor = conn.handle.cursor()
        try:
            cursor.execute(sql)
            if fetch == "one":
                return cursor.fetchone()
            elif fetch == "all":
                return cursor.fetchall()
            else:
                return
        except BaseException as e:
            if conn.handle and not getattr(conn.handle, "closed", True):
                conn.handle.rollback()
            print(sql)
            print(e)
            raise
        finally:
            conn.transaction_open = False
