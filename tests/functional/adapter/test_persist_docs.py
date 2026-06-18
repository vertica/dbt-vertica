import pytest

from dbt.tests.util import run_dbt_and_capture


comment_model_sql = """
{{ config(materialized='table', persist_docs={'relation': true, 'columns': true}) }}

select 1 as id, 'alice' as first_name
"""


comment_schema_yml = """
version: 2

models:
  - name: comment_model
    description: "table comment from dbt"
    columns:
      - name: id
        description: "id column comment from dbt"
      - name: first_name
        description: "first_name column comment from dbt"
"""


class TestPersistDocsVertica:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "comment_model.sql": comment_model_sql,
            "schema.yml": comment_schema_yml,
        }

    def test_emits_table_and_column_comments(self, project):
        results, log_output = run_dbt_and_capture(["--debug", "run"])

        assert len(results) == 1

        normalized_log = log_output.lower()
        assert "comment on table" in normalized_log
        assert "comment on column" in normalized_log
        assert "table comment from dbt" in normalized_log
        assert "id column comment from dbt" in normalized_log
        assert "first_name column comment from dbt" in normalized_log
