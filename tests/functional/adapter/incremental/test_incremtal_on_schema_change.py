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
from dbt.tests.util import (
    check_relations_equal,
    run_dbt,
)
from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
 
    BaseIncrementalOnSchemaChange,
    BaseIncrementalOnSchemaChangeSetup,
)

_MODELS__INCREMENTAL_SYNC_REMOVE_ONLY = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns'

    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% set string_type = dbt.type_string() %}

{% if is_incremental() %}

SELECT id,
       cast(field1 as VARCHAR) as field1

FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

select id,
       cast(field1 as VARCHAR) as field1,
       cast(field2 as VARCHAR) as field2

from source_data where id <= 3

{% endif %}
"""

_MODELS__INCREMENTAL_IGNORE = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='ignore'
    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% if is_incremental() %}

SELECT id, field1, field2, field3, field4 FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

SELECT id, field1, field2 FROM source_data LIMIT 3

{% endif %}
"""

_MODELS__INCREMENTAL_SYNC_REMOVE_ONLY_TARGET = """
{{
    config(materialized='table')
}}

with source_data as (

    select * from {{ ref('model_a') }}

)

{% set string_type = dbt.type_string() %}

select id
       ,cast(field1 as VARCHAR) as field1

from source_data
order by id
"""

_MODELS__INCREMENTAL_IGNORE_TARGET = """
{{
    config(materialized='table')
}}

with source_data as (

    select * from {{ ref('model_a') }}

)

select id
       ,field1
       ,field2

from source_data
"""

_MODELS__INCREMENTAL_FAIL = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail'
    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% if is_incremental()  %}

SELECT id, field1, field2 FROM source_data

{% else %}

SELECT id, field1, field3 FROm source_data

{% endif %}
"""

_MODELS__INCREMENTAL_SYNC_ALL_COLUMNS = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns'

    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% set string_type = 'VARCHAR' %}

{% if is_incremental() %}

SELECT id,
       cast(field1 as VARCHAR) as field1,
       cast(field3 as VARCHAR) as field3, -- to validate new fields
       cast(field4 as VARCHAR) AS field4 -- to validate new fields

FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

select id,
       cast(field1 as VARCHAR) as field1,
       cast(field2 as VARCHAR) as field2

from source_data where id <= 3

{% endif %}
"""

_MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns'
    )
}}

{% set string_type = dbt.type_string() %}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% if is_incremental()  %}

SELECT id,
       cast(field1 as VARCHAR) as field1,
       cast(field3 as VARCHAR) as field3,
       cast(field4 as VARCHAR) as field4
FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

SELECT id,
       cast(field1 as VARCHAR) as field1,
       cast(field2 as VARCHAR) as field2
FROM source_data where id <= 3

{% endif %}
"""

_MODELS__A = """
{{
    config(materialized='table')
}}

with source_data as (

    select 1 as id, 'aaa' as field1, 'bbb' as field2, 111 as field3, 'TTT' as field4
    union all select 2 as id, 'ccc' as field1, 'ddd' as field2, 222 as field3, 'UUU' as field4
    union all select 3 as id, 'eee' as field1, 'fff' as field2, 333 as field3, 'VVV' as field4
    union all select 4 as id, 'ggg' as field1, 'hhh' as field2, 444 as field3, 'WWW' as field4
    union all select 5 as id, 'iii' as field1, 'jjj' as field2, 555 as field3, 'XXX' as field4
    union all select 6 as id, 'kkk' as field1, 'lll' as field2, 666 as field3, 'YYY' as field4

)

select id
       ,field1
       ,field2
       ,field3
       ,field4

from source_data
"""

_MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_TARGET = """
{{
    config(materialized='table')
}}

{% set string_type = dbt.type_string() %}

with source_data as (

    select * from {{ ref('model_a') }}

)

select id
       ,cast(field1 as VARCHAR) as field1
       ,cast(field2 as VARCHAR) as field2
       ,cast(CASE WHEN id <= 3 THEN NULL ELSE field3 END as VARCHAR) AS field3
       ,cast(CASE WHEN id <= 3 THEN NULL ELSE field4 END as VARCHAR) AS field4

from source_data
"""

_MODELS__INCREMENTAL_APPEND_NEW_COLUMNS = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns'
    )
}}

{% set string_type = 'VARCHAR' %}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% if is_incremental()  %}

SELECT id,
       cast(field1 as VARCHAR) as field1,
       cast(field2 as VARCHAR) as field2,
       cast(field3 as VARCHAR) as field3,
       cast(field4 as VARCHAR) as field4
FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

SELECT id,
       cast(field1 as VARCHAR) as field1,
       cast(field2 as VARCHAR) as field2
FROM source_data where id <= 3

{% endif %}
"""

_MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET = """
{{
    config(materialized='table')
}}

with source_data as (

    select * from {{ ref('model_a') }}

)

{% set string_type = 'VARCHAR' %}

select id
       ,cast(field1 as VARCHAR) as field1
       --,cast(case when id <= 3 then null else field2 end as VARCHAR) as field2
       ,cast(case when id <= 3 then null else field3 end as VARCHAR) as field3
       ,cast(case when id <= 3 then null else field4 end as VARCHAR) as field4

from source_data
order by id
"""

_MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE_TARGET = """
{{
    config(materialized='table')
}}

{% set string_type = dbt.type_string() %}

with source_data as (

    select * from {{ ref('model_a') }}

)

select id,
       cast(field1 as VARCHAR) as field1,
       cast(CASE WHEN id >  3 THEN NULL ELSE field2 END as VARCHAR) AS field2,
       cast(CASE WHEN id <= 3 THEN NULL ELSE field3 END as VARCHAR) AS field3,
       cast(CASE WHEN id <= 3 THEN NULL ELSE field4 END as VARCHAR) AS field4

from source_data
"""

class BaseIncrementalOnSchemaChangeSetup:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_sync_remove_only.sql": _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY,
            "incremental_ignore.sql": _MODELS__INCREMENTAL_IGNORE,
            "incremental_sync_remove_only_target.sql": _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY_TARGET,
            "incremental_ignore_target.sql": _MODELS__INCREMENTAL_IGNORE_TARGET,
            "incremental_fail.sql": _MODELS__INCREMENTAL_FAIL,
            "incremental_sync_all_columns.sql": _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS,
            "incremental_append_new_columns_remove_one.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE,
            "model_a.sql": _MODELS__A,
            "incremental_append_new_columns_target.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_TARGET,
            "incremental_append_new_columns.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS,
            "incremental_sync_all_columns_target.sql": _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET,
            "incremental_append_new_columns_remove_one_target.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE_TARGET,
        }


    def run_twice_and_assert(self, include, compare_source, compare_target, project):

        # dbt run (twice)
        run_args = ["run"]
        if include:
            run_args.extend(("--select", include))
        results_one = run_dbt(run_args)
        assert len(results_one) == 3

        results_two = run_dbt(run_args)
        assert len(results_two) == 3

        check_relations_equal(project.adapter, [compare_source, compare_target])

    def run_incremental_append_new_columns(self, project):
        select = "model_a incremental_append_new_columns incremental_append_new_columns_target"
        compare_source = "incremental_append_new_columns"
        compare_target = "incremental_append_new_columns_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)

    def run_incremental_append_new_columns_remove_one(self, project):
        select = "model_a incremental_append_new_columns_remove_one incremental_append_new_columns_remove_one_target"
        compare_source = "incremental_append_new_columns_remove_one"
        compare_target = "incremental_append_new_columns_remove_one_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)

    def run_incremental_sync_all_columns(self, project):
        select = "model_a incremental_sync_all_columns incremental_sync_all_columns_target"
        compare_source = "incremental_sync_all_columns"
        compare_target = "incremental_sync_all_columns_target"
        #self.run_twice_and_assert(select, compare_source, compare_target, project)

    def run_incremental_sync_remove_only(self, project):
        select = "model_a incremental_sync_remove_only incremental_sync_remove_only_target"
        compare_source = "incremental_sync_remove_only"
        compare_target = "incremental_sync_remove_only_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)


class BaseIncrementalOnSchemaChange(BaseIncrementalOnSchemaChangeSetup):
    def test_run_incremental_ignore(self, project):
        select = "model_a incremental_ignore incremental_ignore_target"
        compare_source = "incremental_ignore"
        compare_target = "incremental_ignore_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)

    def test_run_incremental_append_new_columns(self, project):
        self.run_incremental_append_new_columns(project)
        self.run_incremental_append_new_columns_remove_one(project)

    def test_run_incremental_sync_all_columns(self, project):
        self.run_incremental_sync_all_columns(project)
       # self.run_incremental_sync_remove_only(project)

    def test_run_incremental_fail_on_schema_change(self, project):
        select = "model_a incremental_fail"
        run_dbt(["run", "--models", select, "--full-refresh"])
        results_two = run_dbt(["run", "--models", select], expect_pass=False)
        assert "Compilation Error" in results_two[1].message




class TestIncrementalOnSchemaChange(BaseIncrementalOnSchemaChange):
   pass

