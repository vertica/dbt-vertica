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

#!/usr/bin/env python
from setuptools import find_packages
from setuptools import setup
import pathlib
import os
import re


HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()


import os
import sys
import re

# require python 3.7 or newer
if sys.version_info < (3, 7):
    print("Error: dbt does not support this version of Python.")
    print("Please upgrade to Python 3.7 or higher.")
    sys.exit(1)


# require version of setuptools that supports find_namespace_packages
from setuptools import setup

try:
    from setuptools import find_namespace_packages
except ImportError:
    # the user has a downlevel version of setuptools.
    print("Error: dbt requires setuptools v40.1.0 or higher.")
    print('Please upgrade setuptools with "pip install --upgrade setuptools" ' "and try again")
    sys.exit(1)


# pull long description from README
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md")) as f:
    long_description = f.read()


# get this package's version from dbt/adapters/<name>/__version__.py

# require a compatible minor version (~=), prerelease if this is a prerelease


def _get_plugin_version_dict():
    _version_path = os.path.join(HERE, "dbt", "adapters", "vertica", "__version__.py")
    _semver = r"""(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)"""
    _pre = r"""((?P<prekind>a|b|rc)(?P<pre>\d+))?"""
    _version_pattern = fr"""version\s*=\s*["']{_semver}{_pre}["']"""
    with open(_version_path) as f:
        match = re.search(_version_pattern, f.read().strip())
        if match is None:
            raise ValueError(f"invalid version at {_version_path}")
        return match.groupdict()


def _get_dbt_core_version():
    parts = _get_plugin_version_dict()
    minor = "{major}.{minor}.0".format(**parts)
    pre = parts["prekind"] + "1" if parts["prekind"] else ""
    return f"{minor}{pre}"


package_name = "dbt-vertica"
package_version = "1.4.4"
description = """Official vertica adapter plugin for dbt (data build tool)"""
dbt_core_version = _get_dbt_core_version()

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=README,
    long_description_content_type='text/markdown',
    license='Apache License 2.0', 
    author='Vertica (Former authors: Matthew Carter, Andy Regan, Andrew Hedengren)',
    author_email='os_dbt_vertica@microfocus.com',
    url='https://github.com/ajay.abrol2/dbt-vertica/',
    packages=find_packages(include=["dbt","dbt.*"]),
    
    package_data={
        'dbt': [
            'include/vertica/dbt_project.yml',
            'include/vertica/profile_template.yml',
            'include/vertica/sample_profiles.yml',
            'include/vertica/macros/*.sql',
            'include/vertica/macros/adapters/*.sql',
            'include/vertica/macros/materializations/*.sql',
            'include/vertica/macros/materializations/models/incremental/*.sql',
            'include/vertica/macros/materializations/models/table/*.sql',
            'include/vertica/macros/materializations/models/view/*.sql',
            'include/vertica/macros/materializations/seeds/*.sql',
            'include/vertica/macros/materializations/snapshots/*.sql',
        ]
    },
    install_requires=[
        'dbt-core==1.4.4',
        # "dbt-core~={}".format(dbt_core_version),
        'vertica-python>=1.1.0',
        'dbt-tests-adapter==1.4.4',
        'python-dotenv==0.21.1',
    ],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Topic :: Database",
        "Topic :: Database :: Database Engines/Servers",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Operating System :: OS Independent"
    ],
    python_requires=">=3.7.2",
)
