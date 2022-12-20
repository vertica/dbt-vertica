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


# TODO: consider BaseIncrementalUniqueKey test for both strategies as well
class TestIncrementalMergeVertica(BaseIncremental):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "added.csv": seeds_added_csv,
            "seeds.yml": schema_seed_added_yml,
        }

class TestIncrementalDeleteInsertVertica(BaseIncremental):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "added.csv": seeds_added_csv,
            "seeds.yml": schema_seed_added_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": { "+incremental_strategy": "delete+insert" }
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
