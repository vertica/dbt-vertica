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
from dbt.tests.adapter.utils.data_types.base_data_type_macro import BaseDataTypeMacro

seeds__expected_csv = """boolean_col
True
""".lstrip()

models__actual_sql = """
select cast('True' as {{ type_boolean() }}) as boolean_col
"""


class BaseTypeBoolean(BaseDataTypeMacro):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected.csv": seeds__expected_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"actual.sql": self.interpolate_macro_namespace(models__actual_sql, "type_boolean")}


class TestTypeBoolean(BaseTypeBoolean):
    pass
