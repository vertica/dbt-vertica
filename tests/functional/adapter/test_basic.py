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
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)

from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental,BaseIncrementalNotSchemaChange
from dbt.tests.adapter.incremental.test_incremental_unique_id import BaseIncrementalUniqueKey
from dbt.tests.adapter.incremental.test_incremental_predicates import BaseIncrementalPredicates
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection

incremental_sql = """
{{ config(materialized="incremental",incremental_strategy="merge",unique_key='id') }}
select * from {{ source('raw', 'seed') }}
{% if is_incremental() %}
where id > (select max(id) from {{ this }})
{% endif %}
""".strip()

schema_base_yml = """
version: 2
sources:
  - name: raw
    schema: "{{ target.schema }}"
    tables:
      - name: seed
        identifier: "{{ var('seed_name', 'base') }}"
"""

#class TestIncrementalPredicatesDeleteInsert(BaseIncrementalPredicates):
#    pass


#class TestPredicatesDeleteInsert(BaseIncrementalPredicates):
#    pass

#class TestIncrementalUniqueKey(BaseIncrementalUniqueKey):
#    pass

class TestEmptyVertica(BaseEmpty):
    pass

class TestGenericTestsVertica(BaseGenericTests):
    pass

class TestBaseAdapterMethodVertica(BaseAdapterMethod):
    pass

class TestVerticaValidateConnection(BaseValidateConnection):
    pass

class TestSingularTestsEphemeralVertica(BaseSingularTestsEphemeral):
    pass

class TestSimpleMaterializationsVertica(BaseSimpleMaterializations):
    pass


class TestEphemeralVertica(BaseEphemeral):
    pass

class TestIncrementalVertica(BaseIncremental):
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental.sql": incremental_sql, "schema.yml": schema_base_yml}
    

class TestBaseIncrementalNotSchemaChangeVertica(BaseIncrementalNotSchemaChange):
    pass

# @pytest.mark.skip_profile('vertica')
class TestSnapshotCheckColsVertica(BaseSnapshotCheckCols):
    pass

class TestSnapshotTimestampVertica(BaseSnapshotTimestamp):
    pass


