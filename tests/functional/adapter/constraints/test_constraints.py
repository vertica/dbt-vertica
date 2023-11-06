from dbt.tests.adapter.utils.test_null_compare import BaseNullCompare
from dbt.tests.adapter.utils.test_null_compare import BaseMixedNullCompare

from dbt.tests.adapter.constraints.test_constraints import BaseIncrementalConstraintsRollback
from dbt.tests.adapter.utils.test_equals import  BaseEquals
from dbt.tests.adapter.utils.test_validate_sql import  BaseValidateSqlMethod

from dbt.tests.adapter.constraints.test_constraints import BaseModelConstraintsRuntimeEnforcement
from dbt.tests.adapter.constraints.test_constraints import BaseConstraintQuotedColumn

from dbt.tests.util import (
    run_dbt,
    get_manifest,
    run_dbt_and_capture,
    write_file,
    read_file,
    relation_from_name,
)

import pytest

SEEDS__DATA_EQUALS_CSV = """key_name,x,y,expected
1,1,1,same
2,1,2,different
3,1,,different
4,2,1,different
5,2,2,same
6,2,,different
7,,1,different
8,,2,different
9,,,same
"""

# model breaking constraints
my_model_with_nulls_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
 
  cast(null as {{ dbt.type_int() }}) as id,
 
  'red' as color,
  '2019-01-01' as date_day
"""


my_model_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  1 as id,
  'blue' as color,
  '2019-01-01' as date_day
"""



model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
          - type: check
            expression: id >= 1
        tests:
          - unique
      - name: color
        data_type: text
      - name: date_day
        data_type: text
  - name: my_model_error
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: text
      - name: date_day
        data_type: text
  - name: my_model_wrong_order
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: text
      - name: date_day
        data_type: text
  - name: my_model_wrong_name
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: text
      - name: date_day
        data_type: text
"""

class BaseConstraintsRollback:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def null_model_sql(self):
        return my_model_with_nulls_sql

    @pytest.fixture(scope="class")
    def expected_color(self):
        return "blue"

    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ['null value in column "id"', "violates not-null constraint"]

    def assert_expected_error_messages(self, error_message, expected_error_messages):
        print(msg in error_message for msg in expected_error_messages)
        assert all(msg in error_message for msg in expected_error_messages)

    def test__constraints_enforcement_rollback(
        self, project, expected_color, expected_error_messages, null_model_sql
    ):
        # print(expected_error_messages)
        results = run_dbt(["run", "-s", "my_model"])
        # print(results)
        
        assert len(results) == 1

#         # Make a contract-breaking change to the model
        write_file(null_model_sql, "models", "my_model.sql")
       
        failing_results = run_dbt(["run", "-s", "my_model"], expect_pass=True)
        # print("start",failing_results[0].message,"endhere", len(failing_results))
        assert len(failing_results) == 1

#         # Verify the previous table still exists
        relation = relation_from_name(project.adapter, "my_model")
        old_model_exists_sql = f"select * from {relation}"
        old_model_exists = project.run_sql(old_model_exists_sql, fetch="all")
        # print(old_model_exists[0][1],len(old_model_exists))
        assert len(old_model_exists) == 1
        assert old_model_exists[0][1] == expected_color
        
        # Confirm this model was contracted
        # TODO: is this step really necessary?
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        my_model_config = manifest.nodes[model_id].config
        contract_actual_config = my_model_config.contract
        
        assert contract_actual_config.enforced is True

        # # Its result includes the expected error messages
        # print(expected_error_messages)
        self.assert_expected_error_messages(failing_results[0].message, expected_error_messages)



# class TestIncrementalConstraintsRollback(BaseIncrementalConstraintsRollback):
  

#     @pytest.fixture(scope="class")
#     def models(self):
#         return {
#             "my_model.sql": my_model_sql,
#             "constraints_schema.yml": model_schema_yml,
#         }
#     @pytest.fixture(scope="class")
#     def expected_error_messages(self):
#         return  [""]
    
#     @pytest.fixture(scope="class")
#     def expected_color(self):
#         return "blue"
    
#     @pytest.fixture(scope="class")
#     def null_model_sql(self):
#         return my_model_with_nulls_sql


my_incremental_model_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change='append_new_columns'
  )
}}

select
  1 as id,
  'blue' as color,
  '2019-01-01' as date_day
"""

my_model_incremental_with_nulls_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change='append_new_columns'  )
}}

select
  -- null value for 'id'
  cast(null as {{ dbt.type_int() }}) as id,
  -- change the color as well (to test rollback)
  'red' as color,
  '2019-01-01' as date_day
"""

class BaseIncrementalConstraintsRollback(BaseConstraintsRollback):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_incremental_model_sql,
            "constraints_schema.yml": model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def null_model_sql(self):
        return my_model_incremental_with_nulls_sql



class TestIncrementalConstraintsRollback(BaseIncrementalConstraintsRollback):
    # pass

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": model_schema_yml,
        }
    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return  [""]
    
    @pytest.fixture(scope="class")
    def expected_color(self):
        return "red"
    
    @pytest.fixture(scope="class")
    def null_model_sql(self):
        return my_model_with_nulls_sql
    




class TestValidateSqlMethod(BaseValidateSqlMethod):
    pass

class TestNullCompare(BaseNullCompare):
    pass


class TestMixedNullCompare(BaseMixedNullCompare):
    pass


class TestEquals(BaseEquals):
    
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_equals.csv": SEEDS__DATA_EQUALS_CSV,
        }
    pass
    




class TestConstraintQuotedColumn(BaseConstraintQuotedColumn):
    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
create table <model_identifier> INCLUDE SCHEMA PRIVILEGES as ( select 'blue' as "from", 1 as id, '2019-01-01' as date_day ) ;       """
    pass

class TestModelConstraintsRuntimeEnforcement(BaseModelConstraintsRuntimeEnforcement):
    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
create table <model_identifier> INCLUDE SCHEMA PRIVILEGES as ( -- depends_on: <foreign_key_model_identifier> select 'blue' as color, 1 as id, '2019-01-01' as date_day ) ;
"""