# Copyright (c) [2018-2025]  Micro Focus or one of its affiliates.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import dbt.tests.adapter.simple_snapshot.new_record_check_mode as _check_module
import dbt.tests.adapter.simple_snapshot.new_record_dbt_valid_to_current as _v2c_module
import dbt.tests.adapter.simple_snapshot.new_record_timestamp_mode as _ts_module
from dbt.tests.adapter.simple_snapshot.new_record_check_mode import (
    BaseSnapshotNewRecordCheckMode,
)
from dbt.tests.adapter.simple_snapshot.new_record_dbt_valid_to_current import (
    BaseSnapshotNewRecordDbtValidToCurrent,
)
from dbt.tests.adapter.simple_snapshot.new_record_timestamp_mode import (
    BaseSnapshotNewRecordTimestampMode,
)


def _verticafy_sql(sql: str) -> str:
    """Vertica has no TEXT type; rewrite the base suite's DDL."""
    return sql.replace(" TEXT", " VARCHAR(200)").replace("::text", "::varchar")


def _verticafy(module):
    for name in dir(module):
        value = getattr(module, name)
        if isinstance(value, str) and (" TEXT" in value or "::text" in value):
            setattr(module, name, _verticafy_sql(value))
        elif isinstance(value, list) and all(isinstance(item, str) for item in value):
            setattr(module, name, [_verticafy_sql(item) for item in value])


for _module in (_ts_module, _check_module, _v2c_module):
    _verticafy(_module)


class TestVerticaSnapshotNewRecordTimestampMode(BaseSnapshotNewRecordTimestampMode):
    pass


class TestVerticaSnapshotNewRecordCheckMode(BaseSnapshotNewRecordCheckMode):
    pass


class TestVerticaSnapshotNewRecordDbtValidToCurrent(BaseSnapshotNewRecordDbtValidToCurrent):
    pass
