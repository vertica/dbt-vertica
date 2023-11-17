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


from dbt.adapters.sql import SQLAdapter
from dbt.adapters.vertica import verticaConnectionManager
#from dbt.adapters.vertica import VerticaRelation
#from dbt.adapters.vertica import VerticaColumn
from typing import Mapping, Any, Optional, List, Union, Dict
from dbt.adapters.base import available
from dbt.exceptions import (

    DbtRuntimeError
)

import agate
from dataclasses import dataclass
from dbt.adapters.base.meta import available
from dbt.adapters.sql import SQLAdapter  # type: ignore

from dbt.adapters.sql.impl import (
    LIST_SCHEMAS_MACRO_NAME,
    LIST_RELATIONS_MACRO_NAME,
)

from dbt.adapters.base.impl import AdapterConfig,ConstraintSupport
from dbt.contracts.graph.nodes import ConstraintType

@dataclass
class VerticaConfig(AdapterConfig):
    transient: Optional[bool] = None
    cluster_by: Optional[Union[str, List[str]]] = None
    automatic_clustering: Optional[bool] = None
    secure: Optional[bool] = None
    copy_grants: Optional[bool] = None
    vertica_warehouse: Optional[str] = None
    query_tag: Optional[str] = None
    merge_update_columns: Optional[str] = None




class verticaAdapter(SQLAdapter):
    ConnectionManager = verticaConnectionManager
   # Relation = VerticaRelation
    #Column = VerticaColumn
    
    AdapterSpecificConfigs = VerticaConfig
    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.primary_key: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_ENFORCED,
    }


    @classmethod
    def date_function(cls):
        return 'sysdate'

   

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        column = agate_table.columns[col_idx]
        lens = [len(d.encode("utf-8")) for d in column.values_without_nulls()]
        max_len = max(lens)+10 if lens else 64
        return "varchar({})".format(max_len)

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "numeric(18,{})".format(decimals) if decimals else "integer"

    @available
    def standardize_grants_dict(self, grants_table: agate.Table) -> dict:
        """
        :param grants_table: An agate table containing the query result of
            the SQL returned by get_show_grant_sql
        :return: A standardized dictionary matching the `grants` config
        :rtype: dict
        """
        grants_dict: Dict[str, List[str]] = {}
        for row in grants_table:
            grantee = row["grantee"]
            privilege = row["privilege_type"]
            if privilege in grants_dict.keys():
                grants_dict[privilege].append(grantee)
            else:
                grants_dict.update({privilege: [grantee]})
        return grants_dict


    def run_sql_for_tests(self, sql, fetch, conn):
        cursor = conn.handle.cursor()
        try:
            cursor.execute(sql)
            result = None                
            if fetch == "one":
                result = cursor.fetchone()
                conn.handle.commit()
                return result
            elif fetch == "all":
                result = cursor.fetchall()
                conn.handle.commit()
                return result
            else:
                conn.handle.commit()
                return
        except BaseException as e:
            if conn.handle and not getattr(conn.handle, "closed", True):
                conn.handle.rollback()
            print(sql)
            print(e)
            raise
        finally:
            conn.transaction_open = False
    
    def valid_incremental_strategies(self):
        """The set of standard builtin strategies which this adapter supports out-of-the-box.
        Not used to validate custom strategies defined by end users.
        """
        return ["append"]

    def builtin_incremental_strategies(self):
        return ["append", "delete+insert", "merge", "insert_overwrite"]

    @available.parse_none
    def get_incremental_strategy_macro(self, model_context, strategy: str):
        # Construct macro_name from strategy name
        if strategy is None:
            strategy = "default"

        # validate strategies for this adapter
        valid_strategies = self.valid_incremental_strategies()
        valid_strategies.append("default")
        builtin_strategies = self.builtin_incremental_strategies()
        if strategy in builtin_strategies and strategy not in valid_strategies:
            raise DbtRuntimeError(
                f"The incremental strategy '{strategy}' is not valid for this adapter"
            )

        strategy = strategy.replace("+", "_")
        macro_name = f"get_incremental_{strategy}_sql"
        # The model_context should have MacroGenerator callable objects for all macros
        if macro_name not in model_context:
            raise DbtRuntimeError(
                'dbt could not find an incremental strategy macro with the name "{}" in {}'.format(
                    macro_name, self.config.project_name
                )
            )

        # This returns a callable macro
        return model_context[macro_name]
    def debug_query(self) -> None:
        self.execute("select 1 as id")