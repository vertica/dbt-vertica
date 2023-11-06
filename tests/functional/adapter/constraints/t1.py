
from dbt.tests.adapter.constraints.test_constraints import BaseTableContractSqlHeader ,BaseContractSqlHeader
from dbt.tests.adapter.constraints.test_constraints import  BaseIncrementalContractSqlHeader
from dbt.tests.adapter.constraints.test_constraints import BaseModelConstraintsRuntimeEnforcement


import pytest
from dbt.tests.adapter.dbt_clone.test_dbt_clone import  BaseCloneNotPossible, TestPostgresCloneNotPossible,BaseClone

import pytest


import pytest
from dbt.tests.util import (
    run_dbt,
    get_manifest,
    run_dbt_and_capture,
    write_file,
    read_file,
    relation_from_name,
)






my_model_contract_sql_header_sql = """
{{
  config(
    materialized = "table"
  )
}}

{% call set_sql_header(config) %}
set session time zone 'Asia/Kolkata';
{%- endcall %}
select current_setting('timezone') as column_name;
"""

model_contract_header_schema_yml = """
version: 2
models:
  - name: my_model_contract_sql_header
    config:
      contract:
        enforced: true
    columns:
      - name: column_name
        data_type: text
"""



my_model_incremental_contract_sql_header_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change="append_new_columns"
  )
}}

{% call set_sql_header(config) %}
set session time zone 'Asia/Kolkata';
{%- endcall %}
select current_setting('timezone') as column_name
"""


class BaseContractSqlHeader:
    """Tests a contracted model with a sql header dependency."""

    def test__contract_sql_header(self, project):
        run_dbt(["run", "-s", "my_model_contract_sql_header"])

        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model_contract_sql_header"
        model_config = manifest.nodes[model_id].config

        assert model_config.contract.enforced


class BaseTableContractSqlHeader(BaseContractSqlHeader):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_contract_sql_header.sql": my_model_contract_sql_header_sql,
            "constraints_schema.yml": model_contract_header_schema_yml,
        }


class TestTableContractSqlHeader(BaseTableContractSqlHeader):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_contract_sql_header.sql": my_model_contract_sql_header_sql,
            "constraints_schema.yml": model_contract_header_schema_yml,
        }




class BaseIncrementalContractSqlHeader(BaseContractSqlHeader):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_contract_sql_header.sql": my_model_incremental_contract_sql_header_sql,
            "constraints_schema.yml": model_contract_header_schema_yml,
        }


class TestIncrementalContractSqlHeader(BaseIncrementalContractSqlHeader):
    pass
