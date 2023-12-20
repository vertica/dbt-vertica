


from dbt.tests.adapter.utils.test_date_spine import BaseDateSpine

# from dbt.tests.adapter.utils.test_get_intervals_between import BaseGetIntervalsBetween 
from dbt.tests.adapter.utils.test_generate_series import BaseGenerateSeries 

from dbt.tests.adapter.utils.test_get_powers_of_two import BaseGetPowersOfTwo



import pytest
from dbt.tests.adapter.utils.base_utils import BaseUtils

models__test_get_intervals_between_sql = """
SELECT
  {% if target.type == 'postgres' %}
    {{ get_intervals_between('day',"'09/01/2023'::date", "'09/12/2023'::date" ) }} as intervals,
  {% else %}
      {{ get_intervals_between( "'09/01/2023'", "'09/12/2023'",'day'  ) }} as intervals,
  {% endif %}
  11 as expected

"""

models__test_get_intervals_between_yml = """
version: 2
models:
  - name: test_get_intervals_between
    tests:
      - assert_equal:
          actual: intervals
          expected: expected
"""
models__test_date_spine_sql = """
with generated_dates as (
    {% if target.type == 'postgres' %}
        {{ date_spine("day", "'2023-09-01'::date", "'2023-09-10'::date") }}
 
    {% elif target.type == 'bigquery' or target.type == 'redshift' %}
        select cast(date_day as date) as date_day
        from ({{ date_spine("day", "'2023-09-01'", "'2023-09-10'") }})
 
    {% else %}
        {{ date_spine("day", "'2023-09-01'", "'2023-09-10'") }}
    {% endif %}
), expected_dates as (
    {% if target.type == 'vertica' %}
        select '2023-09-01'::date as expected
        union all
        select '2023-09-02'::date as expected
        union all
        select '2023-09-03'::date as expected
        union all
        select '2023-09-04'::date as expected
        union all
        select '2023-09-05'::date as expected
        union all
        select '2023-09-06'::date as expected
        union all
        select '2023-09-07'::date as expected
        union all
        select '2023-09-08'::date as expected
        union all
        select '2023-09-09'::date as expected
 
    {% elif target.type == 'bigquery' or target.type == 'redshift' %}
        select cast('2023-09-01' as date) as expected
        union all
        select cast('2023-09-02' as date) as expected
        union all
        select cast('2023-09-03' as date) as expected
        union all
        select cast('2023-09-04' as date) as expected
        union all
        select cast('2023-09-05' as date) as expected
        union all
        select cast('2023-09-06' as date) as expected
        union all
        select cast('2023-09-07' as date) as expected
        union all
        select cast('2023-09-08' as date) as expected
        union all
        select cast('2023-09-09' as date) as expected
 
    {% else %}
        select '2023-09-01' as expected
        union all
        select '2023-09-02' as expected
        union all
        select '2023-09-03' as expected
        union all
        select '2023-09-04' as expected
        union all
        select '2023-09-05' as expected
        union all
        select '2023-09-06' as expected
        union all
        select '2023-09-07' as expected
        union all
        select '2023-09-08' as expected
        union all
        select '2023-09-09' as expected
    {% endif %}
), joined as (
    select
        generated_dates.date_day,
        expected_dates.expected
    from generated_dates
    left join expected_dates on generated_dates.date_day = expected_dates.expected
)
 
SELECT * from joined
"""
 
models__test_date_spine_yml = """
version: 2
models:
  - name: test_date_spine
    tests:
      - assert_equal:
          actual: date_day
          expected: expected
"""
 


class BaseGetIntervalsBetween(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_get_intervals_between.yml": models__test_get_intervals_between_yml,
            "test_get_intervals_between.sql": self.interpolate_macro_namespace(
                models__test_get_intervals_between_sql, "get_intervals_between"
            ),
        }






class BaseDateSpine(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_date_spine.yml": models__test_date_spine_yml,
            "test_date_spine.sql": self.interpolate_macro_namespace(
                models__test_date_spine_sql, "date_spine"
            ),
        }


class TestDateSpine(BaseDateSpine):
    pass

# pass
class TestGenerateSeries(BaseGenerateSeries):
    pass


# #pass
class TestGetPowersOfTwo(BaseGetPowersOfTwo):
    pass



class TestGetIntervalsBeteween(BaseGetIntervalsBetween):
    pass
