import pytest

from dbt.tests.adapter.sample_mode.test_sample_mode import BaseSampleModeTest

# Vertica-friendly timestamp literals (no trailing UTC offset)
_input_model_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, TIMESTAMP '2025-01-01 01:25:00' as event_time
UNION ALL
select 2 as id, TIMESTAMP '2025-01-02 13:47:00' as event_time
UNION ALL
select 3 as id, TIMESTAMP '2025-01-03 01:32:00' as event_time
"""


class TestVerticaSampleMode(BaseSampleModeTest):
    @pytest.fixture(scope="class")
    def input_model_sql(self) -> str:
        return _input_model_sql
