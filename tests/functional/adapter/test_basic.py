import pytest

from dbt.tests.adapter.basic.files import seeds_base_csv, seeds_added_csv, seeds_newcolumns_csv

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod

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

class TestSimpleMaterializationsVertica(BaseSimpleMaterializations):
    pass


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
