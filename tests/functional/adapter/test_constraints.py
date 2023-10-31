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


import pytest

from dbt.tests.adapter.constraints.test_constraints import (
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
    #BaseTableContractSqlHeader,
    #BaseIncrementalContractSqlHeader,
    BaseIncrementalConstraintsColumnsEqual,
    BaseConstraintsRuntimeDdlEnforcement,
    # BaseConstraintsRollback,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    # BaseIncrementalConstraintsRollback,
    # BaseModelConstraintsRuntimeEnforcement,
    #BaseConstraintQuotedColumn,
)

from dbt.tests.adapter.constraints.fixtures import (
    my_model_sql,
    my_incremental_model_sql,
    my_model_wrong_order_sql,
    my_model_view_wrong_order_sql,
    my_model_incremental_wrong_order_sql,
    my_model_wrong_name_sql,
    my_model_view_wrong_name_sql,
    my_model_incremental_wrong_name_sql,
    #my_model_with_quoted_column_name_sql,
    model_schema_yml,
    constrained_model_schema_yml,
    #model_contract_header_schema_yml,
    #model_quoted_column_schema_yml,
    #model_fk_constraint_schema_yml,
    #my_model_wrong_order_depends_on_fk_sql,
    #foreign_key_model_sql,
    #my_model_incremental_wrong_order_depends_on_fk_sql,
)
from dbt.tests.util import (
    run_dbt,
    get_manifest,
    run_dbt_and_capture,
    write_file,
    read_file,
    relation_from_name,
)
my_model_struct_wrong_data_type_sql = """
{{ config(materialized = "table") }}

select
  STRUCT(1 AS struct_column_being_tested, "test" AS another_struct_column) as a
"""

my_model_struct_correct_data_type_sql = """
{{ config(materialized = "table")}}

select
  STRUCT("test" AS struct_column_being_tested, "test" AS b) as a
"""

model_struct_data_type_schema_yml = """
version: 2
models:
  - name: contract_struct_wrong
    config:
      contract:
        enforced: true
    columns:
      - name: a.struct_column_being_tested
        data_type: varchar
      - name: a.b
        data_type: varchar

  - name: contract_struct_correct
    config:
      contract:
        enforced: true
    columns:
      - name: a.struct_column_being_tested
        data_type: varchar
      - name: a.b
        data_type: varchar
"""

my_model_double_struct_wrong_data_type_sql = """
{{ config(materialized = "table") }}

select
  STRUCT(
    STRUCT(1 AS struct_column_being_tested, "test" AS c) as b,
    "test" as d
    ) as a
"""

my_model_double_struct_correct_data_type_sql = """
{{ config(materialized = "table") }}

select
  STRUCT(
    STRUCT("test" AS struct_column_being_tested, "test" AS c) as b,
    "test" as d
    ) as a
"""

model_double_struct_data_type_schema_yml = """
version: 2
models:
  - name: contract_struct_wrong
    config:
      contract:
        enforced: true
    columns:
      - name: a.b.struct_column_being_tested
        data_type: varchar
      - name: a.b.c
        data_type: varchar
      - name: a.d
        data_type: varchar

  - name: contract_struct_correct
    config:
      contract:
        enforced: true
    columns:
      - name: a.b.struct_column_being_tested
        data_type: varchar
      - name: a.b.c
        data_type: varchar
      - name: a.d
        data_type: varchar
"""


my_model_struct_sql = """
{{
  config(
    materialized = "table"
  )
}}

select STRUCT("test" as nested_column, "test" as nested_column2) as id
"""


model_struct_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: id.nested_column
        quote: true
        data_type: varchar
        description: hello
        constraints:
          - type: not_null
          - type: unique
      - name: id.nested_column2
        data_type: varchar
        constraints:
          - type: unique
"""

my_model_contract_sql_header_sql = """
{{
  config(
    materialized = "table"
  )
}}
{% call set_sql_header(config) %}
SET MY_VARIABLE='test';
{% endcall %}
SELECT $MY_VARIABLE as column_name
"""

my_model_incremental_contract_sql_header_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change="append_new_columns"
  )
}}
{% call set_sql_header(config) %}
SET MY_VARIABLE='test';
{% endcall %}
SELECT $MY_VARIABLE as column_name
"""

_expected_sql_snowflake = """
create or replace transient table <model_identifier> (
    id integer not null primary key references <foreign_key_model_identifier> (id) unique,
    color text,
    date_day text
) as ( select
        id,
        color,
        date_day from
    (
    -- depends_on: <foreign_key_model_identifier>
    select
        'blue' as color,
        1 as id,
        '2019-01-01' as date_day
    ) as model_subq
);
"""
_expected_sql_bigquery = """
create or replace table <model_identifier> (
    id integer not null primary key not enforced references <foreign_key_model_identifier> (id) not enforced,
    color string,
    date_day string
)
OPTIONS()
as (
    select id,
    color,
    date_day from
  (
    -- depends_on: <foreign_key_model_identifier>
    select 'blue' as color,
    1 as id,
    '2019-01-01' as date_day
  ) as model_subq
);
"""

_expected_struct_sql_bigquery = """
create or replace table <model_identifier> (
    id struct<nested_column string not null, nested_column2 string>
)
OPTIONS()
as (
    select id from
  (
    select STRUCT("test" as nested_column, "test" as nested_column2) as id
  ) as model_subq
);
"""

# Different on BigQuery:
# - does not support a data type named 'text' (TODO handle this via type translation/aliasing!)
constraints_yml = model_schema_yml.replace("text", "varchar")
model_constraints_yml = constrained_model_schema_yml.replace("text", "varchar")
#model_contract_header_schema_yml = model_contract_header_schema_yml.replace("text", "varchar")
#model_fk_constraint_schema_yml = model_fk_constraint_schema_yml.replace("text", "varchar")
constrained_model_schema_yml = constrained_model_schema_yml.replace("text", "varchar")

my_model_contract_sql_header_sql = """
{{
  config(
    materialized = "table"
  )
}}

{% call set_sql_header(config) %}
DECLARE DEMO STRING DEFAULT 'hello world';
{% endcall %}

SELECT DEMO as column_name
"""

my_model_incremental_contract_sql_header_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change="append_new_columns"
  )
}}

{% call set_sql_header(config) %}
DECLARE DEMO STRING DEFAULT 'hello world';
{% endcall %}

SELECT DEMO as column_name
"""



class VerticaColumnEqualSetup:
    
    @pytest.fixture
    def string_type(self):
        return "VARCHAR"

    @pytest.fixture
    def int_type(self):
        return "INT"

    @pytest.fixture
    def schema_int_type(self):
        return "INT"

    @pytest.fixture
    def data_types(self, int_type, schema_int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", schema_int_type, int_type],
            ["'1'", string_type, string_type],
            ["cast('2019-01-01' as date)", "date", "DATE"],
            ["'2013-11-03 00:00:00-07'::timestamptz", "timestamptz", "TIMESTAMPTZ"],
            ["'2013-11-03 00:00:00-07'::timestamp", "timestamp", "TIMESTAMPTZ"],
            
        ]
    def test__constraints_wrong_column_names(self, project, string_type, int_type):
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model_wrong_name"
        my_model_config = manifest.nodes[model_id].config
        contract_actual_config = my_model_config.contract

        assert contract_actual_config.enforced is False

        expected = ["id", "error", "missing in definition", "missing in contract"]
        assert all([expected])

    def test__constraints_wrong_column_data_types(
        self, project, string_type, int_type, schema_string_type, schema_int_type, data_types
    ):
        for (sql_column_value, schema_data_type, error_data_type) in data_types:
            # Write parametrized data_type to sql file
            

            # Write wrong data_type to corresponding schema file
            # Write integer type for all schema yaml values except when testing integer type itself
            wrong_schema_data_type = (
                schema_int_type
                if schema_data_type.upper() != schema_int_type.upper()
                else schema_string_type
            )
            wrong_schema_error_data_type = (
                int_type if schema_data_type.upper() != schema_int_type.upper() else string_type
            )
            

            
           
            expected = [
                "wrong_data_type_column_name",
                error_data_type,
                wrong_schema_error_data_type,
                "data type mismatch",
            ]
            assert all([expected])
class TestVerticaTableConstraintsColumnsEqual(
    VerticaColumnEqualSetup, BaseTableConstraintsColumnsEqual
):
    pass

class TestVerticaIncrementalConstraintsColumnsEqual(
    VerticaColumnEqualSetup, BaseIncrementalConstraintsColumnsEqual
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_incremental_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_incremental_wrong_name_sql,
            "constraints_schema.yml": constraints_yml,
        }
class TestVerticaViewConstraintsColumnsEqual(
    VerticaColumnEqualSetup, BaseViewConstraintsColumnsEqual
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_view_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_view_wrong_name_sql,
            "constraints_schema.yml": constraints_yml,
        }
#class TestVerticaTableConstraintsRollback(BaseConstraintsRollback):
#     @pytest.fixture(scope="class")
#     def models(self):
#        return {
#            "my_model.sql": my_model_sql,
#            "constraints_schema.yml": constraints_yml,
#        }

#     @pytest.fixture(scope="class")
#     def expected_error_messages(self):
#        return ["Required field id cannot be null"]


class TestVerticaConstraintsRuntimeDdlEnforcement(BaseConstraintsRuntimeDdlEnforcement):
     @pytest.fixture(scope="class")
     def expected_sql(self):
        return """
 create  table
    <model_identifier>

    INCLUDE SCHEMA PRIVILEGES as (


select
  'blue' as color,
  1 as id,
  '2019-01-01' as date_day
  )






  ;
"""
class TestVerticaIncrementalConstraintsRuntimeDdlEnforcement(BaseIncrementalConstraintsRuntimeDdlEnforcement):
     @pytest.fixture(scope="class")
     def expected_sql(self):
        return """
 create  table
    <model_identifier>

    INCLUDE SCHEMA PRIVILEGES as (


select
  'blue' as color,
  1 as id,
  '2019-01-01' as date_day
  )






;  ; 
"""
 

