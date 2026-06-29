import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture, relation_from_name


model_sql = """
{{ config(materialized='table') }}

select
    1 as id,
    'alpha' as category
"""

schema_yml = """
version: 2

models:
  - name: model_with_comments
    description: "Model-level table comment from dbt"
    columns:
      - name: id
        description: "Primary identifier comment"
      - name: category
        description: "Category column comment"
"""

schema_yml_invalid_table_comment = """
version: 2

models:
  - name: model_with_comments
    description: "Invalid table comment contains $dbt_comment_literal_block$"
    columns:
      - name: id
        description: "Primary identifier comment"
      - name: category
        description: "Category column comment"
"""

schema_yml_invalid_column_comment = """
version: 2

models:
  - name: model_with_comments
    description: "Valid table comment"
    columns:
      - name: id
        description: "Invalid column comment contains $dbt_comment_literal_block$"
      - name: category
        description: "Category column comment"
"""

forbidden_comment_message = "The string $dbt_comment_literal_block$ is not allowed in comments."


class TestPersistDocsVertica:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_with_comments.sql": model_sql,
            "schema.yml": schema_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                }
            }
        }

    def test_table_and_column_comments(self, project):
        run_dbt(["run"])

        relation = relation_from_name(project.adapter, "model_with_comments")

        table_comment_sql = f"""
            select comment
            from v_catalog.comments
            where lower(object_type) = 'table'
              and lower(object_schema) = lower('{relation.schema}')
              and lower(object_name) = lower('{relation.identifier}')
        """
        table_comment = project.run_sql(table_comment_sql, fetch="one")
        assert table_comment is not None
        assert table_comment[0] == "Model-level table comment from dbt"

        column_comments_sql = f"""
            select lower(child_object), comment
            from v_catalog.comments
            where lower(object_type) = 'column'
              and lower(object_schema) = lower('{relation.schema}')
              and lower(object_name) = lower('{relation.identifier}')
              and lower(child_object) in ('id', 'category')
            order by 1
        """
        column_comments = project.run_sql(column_comments_sql, fetch="all")
        assert column_comments is not None

        comments_by_column = {row[0]: row[1] for row in column_comments}
        assert comments_by_column["id"] == "Primary identifier comment"
        assert comments_by_column["category"] == "Category column comment"


class TestPersistDocsVerticaInvalidTableComment:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_with_comments.sql": model_sql,
            "schema.yml": schema_yml_invalid_table_comment,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                }
            }
        }

    def test_invalid_table_comment_fails(self, project):
        _, output = run_dbt_and_capture(["run"], expect_pass=False)
        assert forbidden_comment_message in output


class TestPersistDocsVerticaInvalidColumnComment:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_with_comments.sql": model_sql,
            "schema.yml": schema_yml_invalid_column_comment,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                }
            }
        }

    def test_invalid_column_comment_fails(self, project):
        _, output = run_dbt_and_capture(["run"], expect_pass=False)
        assert forbidden_comment_message in output
