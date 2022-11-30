#!/usr/bin/env python
from setuptools import find_packages
from setuptools import setup
import pathlib

package_name = "dbt-vertica"
package_version = "1.0.4"
description = """The vertica adapter plugin for dbt (data build tool)"""

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=README,
    long_description_content_type='text/markdown',
    license='MIT',
    author='Matthew Carter (original), Andrew Hedengren, Andy Reagan',
    author_email='arosychuk@gmail.com, andy@andyreagan.com',
    url='https://github.com/mpcarter/dbt-vertica',
    packages=find_packages(),
    package_data={
        'dbt': [
            'include/vertica/dbt_project.yml',
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
        'dbt-core>=1.0.0',
        'vertica-python>=0.10.0',
    ],
    extras_require={
        'dev': [
            'pytest-dbt-adapter==0.6.0',
        ]
    }
)
