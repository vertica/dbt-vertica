
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
import os
from typing import Dict, Any, Set
# Import the standard functional fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = "dbt.tests.fixtures.project"

def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="vertica", type=str)



def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "skip_profile(profile): skip test for the given profile",
    )
    config.addinivalue_line(
        "markers",
        "only_profile(profile): only test the given profile",
    )

# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here
@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
         'type': 'vertica',
        'threads': 1,
        'host': '159.65.150.255',
        'username': 'dbadmin',
        'password': '',
        'database': 'VMart',
        'port': 5433,
        
    }


#  return {
#         "type": "vertica",
#         "threads": 1,
#         "host": "159.65.150.255",
#         "port": int(os.getenv("VERTICA_TEST_PORT", 5433)),
#         "username": os.getenv("VERTICA_TEST_USER", "dbadmin"),
#         "password": os.getenv("VERTICA_TEST_PASS", ""),
#         "database": os.getenv("VERTICA_TEST_DATABASE","VMart"),
        
#     }




# def dbt_profile_target():
#     return {
#         'type': 'vertica',
#         'threads': 1,
#         'host': 'localhost',
#         'username': 'dbadmin',
#         'password': '',
#         'database': 'docker',
#         'port': 5433,
#     }

@pytest.fixture(scope="session")
def dbt_profile_target(request):
    profile_type = request.config.getoption("--profile")
    if profile_type =="vertica":
        target = vertica_target()
    elif profile_type == "databricks_sql_endpoint":
        target = databricks_sql_endpoint_target()
    elif profile_type == "apache_spark":
        target = apache_spark_target()
    else:
        raise ValueError(f"Invalid profile type '{profile_type}'")
    return target

def apache_spark_target():
    return {
        "type": "spark",
        "host": "159.65.150.255",
    }

def databricks_sql_endpoint_target():
    return {
        "type": "spark",
        "host": "159.65.150.255",
    }

def vertica_target():
    return {
       'type': 'vertica',
        'threads': 1,
        'host': '159.65.150.255',
        'username': 'dbadmin',
        'password': '',
        'database': 'VMart',
        'port': 5433,
    }


@pytest.fixture(autouse=True)
def skip_by_profile_type(request):
    profile_type = request.config.getoption("--profile")
    if request.node.get_closest_marker("skip_profile"):
        for skip_profile_type in request.node.get_closest_marker("skip_profile").args:
            if skip_profile_type == profile_type:
                pytest.skip("skipped on '{profile_type}' profile")

@pytest.fixture(autouse=True)
def only_profile_type(request):
    profile_type = request.config.getoption("--profile")
    if request.node.get_closest_marker("only_profile"):
        for only_profile_type in request.node.get_closest_marker("only_profile").args:
            if only_profile_type != profile_type:
                pytest.skip("skipped on '{profile_type}' profile")