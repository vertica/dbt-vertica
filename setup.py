#!/usr/bin/env python
from setuptools import find_packages
from setuptools import setup
import pathlib

package_name = "dbt-vertica"
package_version = "0.21.0"
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
    url='https://github.com/andyreagan/dbt-vertica',
    packages=find_packages(),
    package_data={
        'dbt': [
            'include/vertica/dbt_project.yml',
            'include/vertica/macros/*.sql',
            'include/vertica/macros/materializations/*.sql',
        ]
    },
    install_requires=[
        'dbt-core>=0.21.0',
        'vertica-python>=0.10.0',
    ]
)
