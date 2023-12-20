


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


class BaseGetIntervalsBetween(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_get_intervals_between.yml": models__test_get_intervals_between_yml,
            "test_get_intervals_between.sql": self.interpolate_macro_namespace(
                models__test_get_intervals_between_sql, "get_intervals_between"
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
