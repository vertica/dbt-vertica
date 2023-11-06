from dbt.tests.adapter.utils.test_null_compare import BaseNullCompare
from dbt.tests.adapter.utils.test_null_compare import BaseMixedNullCompare

from dbt.tests.adapter.constraints.test_constraints import BaseIncrementalConstraintsRollback
from dbt.tests.adapter.utils.test_equals import  BaseEquals
from dbt.tests.adapter.utils.test_validate_sql import  BaseValidateSqlMethod

from dbt.tests.adapter.constraints.test_constraints import BaseModelConstraintsRuntimeEnforcement
from dbt.tests.adapter.constraints.test_constraints import BaseConstraintQuotedColumn



import pytest



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
#         return "red"
    
#     @pytest.fixture(scope="class")
#     def null_model_sql(self):
#         return my_model_with_nulls_sql
    




# class TestValidateSqlMethod(BaseValidateSqlMethod):
#     pass

# class TestNullCompare(BaseNullCompare):
#     pass


# class TestMixedNullCompare(BaseMixedNullCompare):
#     pass


# class TestEquals(BaseEquals):
#     pass




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