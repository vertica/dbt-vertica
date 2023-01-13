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



# import pytest
from dbt.tests.adapter.grants.base_grants import BaseGrants
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.context.base import BaseContext  # diff_of_two_dicts only
import os
import pytest
from dbt.tests.util import (
    run_dbt_and_capture,
    write_file,
    relation_from_name,
    get_connection,
)

my_invalid_model_sql = """
  select 1 as fun
"""

invalid_user_table_model_schema_yml = """
version: 2
models:
  - name: my_invalid_model
    config:
      materialized: table
      grants:
        select: ['invalid_user']
"""

invalid_privilege_table_model_schema_yml = """
version: 2
models:
  - name: my_invalid_model
    config:
      materialized: table
      grants:
        fake_privilege: ["{{ env_var('DBT_TEST_USER_2') }}"]
"""

from dotenv import load_dotenv
load_dotenv()
TEST_USER_ENV_VARS = ["DBT_TEST_USER_1", "DBT_TEST_USER_2", "DBT_TEST_USER_3"]


def replace_all(text, dic):
    for i, j in dic.items():
        text = text.replace(i, j)
    return text
class BaseGrantsVertica(BaseGrants):
    def privilege_grantee_name_overrides(self):
        # these privilege and grantee names are valid on most databases, but not all!
        # looking at you, BigQuery
        # optionally use this to map from "select" --> "other_select_name", "insert" --> ...
        return {
            "select": "select",
            "insert": "insert",
            "fake_privilege": "fake_privilege",
            "invalid_user": "invalid_user",
        }

    def interpolate_name_overrides(self, yaml_text):
        return replace_all(yaml_text, self.privilege_grantee_name_overrides())

    @pytest.fixture(scope="class", autouse=True)
    def get_test_users(self, project):
        test_users = []
        for env_var in TEST_USER_ENV_VARS:
            user_name = os.environ[env_var]
            if user_name:
                test_users.append(user_name)
        return test_users

    def get_grants_on_relation(self, project, relation_name):
        relation = relation_from_name(project.adapter, relation_name)
        adapter = project.adapter
        with get_connection(adapter):
            kwargs = {"relation": relation}
            show_grant_sql = adapter.execute_macro("get_show_grant_sql", kwargs=kwargs)
            _, grant_table = adapter.execute(show_grant_sql, fetch=True)
            actual_grants = adapter.standardize_grants_dict(grant_table)
        return actual_grants

    def assert_expected_grants_match_actual(self, project, relation_name, expected_grants):
        actual_grants = self.get_grants_on_relation(project, relation_name)
        # need a case-insensitive comparison
        # so just a simple "assert expected == actual_grants" won't work
        diff_a = BaseContext.diff_of_two_dicts(actual_grants, expected_grants)
        diff_b = BaseContext.diff_of_two_dicts(expected_grants, actual_grants)
        assert diff_a == diff_b == {}


class BaseInvalidGrantsVertica(BaseGrantsVertica):

    def test_invalid_grants(self, project, get_test_users, logs_dir):
        
        # failure when grant to a user/role that doesn't exist
        yaml_file = self.interpolate_name_overrides(invalid_user_table_model_schema_yml)
        write_file(yaml_file, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert results,self.grantee_does_not_exist_error() in log_output

        # failure when grant to a privilege that doesn't exist
        yaml_file = self.interpolate_name_overrides(invalid_privilege_table_model_schema_yml)
        write_file(yaml_file, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert results,self.privilege_does_not_exist_error() in log_output

class TestInvalidGrantsVertica(BaseInvalidGrantsVertica,BaseInvalidGrants):
    pass

