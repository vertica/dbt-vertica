import pytest

from dbt.tests.adapter.basic.files import (
    seeds_base_csv,
    seeds_added_csv,
    seeds_newcolumns_csv,
    base_view_sql,
    base_table_sql,
    schema_base_yml,
    model_base
)

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod, models__upstream_sql, models__expected_sql

schema_seed_added_yml = """version: 2
seeds:
  - name: base
    config:
      column_types:
        name: varchar(50)
  - name: added
    config:
      column_types:
        name: varchar(50)
"""

config_materialized_var = """
  {% set materialized_var = "table" %}
  {% if var("materialized_var", "table") == "view" %}
    {% set materialized_var = "view" %}
  {% endif %}
  {{ config(materialized=materialized_var) }}
"""

base_materialized_var_sql = config_materialized_var + model_base

class TestSimpleMaterializationsVertica(BaseSimpleMaterializations):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_model.sql": base_view_sql,
            "table_model.sql": base_table_sql,
            "swappable.sql": base_materialized_var_sql,
            "schema.yml": schema_base_yml,
        }


class TestSingularTestsVertica(BaseSingularTests):
    pass


class TestSingularTestsEphemeralVertica(BaseSingularTestsEphemeral):
    pass


class TestEmptyVertica(BaseEmpty):
    pass


class TestEphemeralVertica(BaseEphemeral):
    pass


class TestIncrementalVertica(BaseIncremental):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "added.csv": seeds_added_csv,
            "seeds.yml": schema_seed_added_yml,
        }


class TestGenericTestsVertica(BaseGenericTests):
    pass


class TestSnapshotCheckColsVertica(BaseSnapshotCheckCols):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "added.csv": seeds_added_csv,
            "seeds.yml": schema_seed_added_yml,
        }


class TestSnapshotTimestampVertica(BaseSnapshotTimestamp):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base.csv": seeds_base_csv,
            "added.csv": seeds_added_csv,
            "seeds.yml": schema_seed_added_yml,
        }


class TestBaseAdapterMethod(BaseAdapterMethod):
    pass


# TODO: convert this into a pytest
#   test_vertica_dbt_incremental_delete_insert:
#     project: text_vertica_dbt_delete_insert
#     sequence:
#       - type: dbt
#         cmd: seed
#       - type: run_results
#         length: fact.seed.length
#       - type: dbt
#         cmd: run
#         vars:
#           seed_name: base
#       - type: relation_rows
#         name: base
#         length: fact.base.rowcount
#       - type: run_results
#         length: fact.run.length
#       - type: relations_equal
#         relations:
#           - base
#           - incremental
#       - type: dbt
#         cmd: run
#         vars:
#           seed_name: added
#       - type: relation_rows
#         name: added
#         length: fact.added.rowcount
#       - type: run_results
#         length: fact.run.length
#       - type: relations_equal
#         relations:
#           - added
#           - incremental
#       - type: dbt
#         cmd: docs generate
#       - type: catalog
#         exists: True
#         nodes:
#           length: fact.catalog.nodes.length
#         sources:
#           length: fact.catalog.sources.length


# TODO: convert this into a pytest
#   - name: text_vertica_dbt_delete_insert
#     paths:
#       seeds/base.csv: files.seeds.base
#       seeds/added.csv: files.seeds.added
#       seeds/properties.yml: |
#         version: 2
#         seeds:
#           - name: base
#             config:
#               column_types:
#                 name: varchar(50)
#           - name: added
#             config:
#               column_types:
#                 name: varchar(50)
#       models/schema.yml: files.schemas.base
#       models/incremental.sql:
#         materialized: incremental
#         # Specifying which column to use, in this case, we will use the id
#         # Wrap in a CTE due to added seed name varchar being size 9 and seed base name varchar being size 8
#         body: |
#             {{
#               config(
#                 materialized = 'incremental',
#                 incremental_strategy = 'delete+insert',
#                 unique_key = 'id'
#               )
#             }}
#             with incremental_data as
#             (
#               select * from {{ source('raw', 'seed') }}
#               {% if is_incremental() %}
#               where id > (select max(id) from {{ this }})
#               {% endif %}
#             )
#             select * from incremental_data
#     facts:
#       seed:
#         length: 2
#         names:
#           - base
#           - added
#       run:
#         length: 1
#         names:
#           - incremental
#       catalog:
#         nodes:
#           length: 3
#         sources:
#           length: 1
#       persisted_relations:
#         - base
#         - added
#         - incremental
#       base:
#         rowcount: 10
#       added:
#         rowcount: 20