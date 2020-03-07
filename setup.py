#!/usr/bin/env python
from setuptools import find_packages
from setuptools import setup

package_name = "dbt-vertica"
package_version = "0.0.1"
description = """The vertica adapter plugin for dbt (data build tool)"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author='Matthew Carter',
    author_email='carter.matt.p@gmail.com',
    url='github.com/MCarter/dbt-vertica',
    packages=find_packages(),
    package_data={
        'dbt': [
            'include/vertica/dbt_project.yml',
            'include/vertica/macros/*.sql',
        ]
    },
    install_requires=[
        'dbt-core>=0.15.0',
        'vertica-python>=0.10.0',
    ]
)
