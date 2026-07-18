import datetime

import dbt.tests.adapter.simple_snapshot.test_various_configs as _base_module
from dbt.tests.util import run_dbt, run_sql_with_adapter
from dbt.tests.adapter.simple_snapshot.test_various_configs import (
    BaseSnapshotColumnNames,
    BaseSnapshotColumnNamesFromDbtProject,
    BaseSnapshotDbtValidToCurrent,
    BaseSnapshotInvalidColumnNames,
    BaseSnapshotMultiUniqueKey,
)


def _verticafy(module):
    """Vertica has no TEXT type; rewrite the base suite's DDL constants."""
    for name in dir(module):
        value = getattr(module, name)
        if isinstance(value, str) and (" TEXT" in value or "::text" in value):
            setattr(
                module,
                name,
                value.replace(" TEXT", " VARCHAR(200)").replace("::text", "::varchar"),
            )


_verticafy(_base_module)


class TestVerticaSnapshotColumnNames(BaseSnapshotColumnNames):
    pass


class TestVerticaSnapshotColumnNamesFromDbtProject(BaseSnapshotColumnNamesFromDbtProject):
    pass


class TestVerticaSnapshotInvalidColumnNames(BaseSnapshotInvalidColumnNames):
    pass


class TestVerticaSnapshotDbtValidToCurrent(BaseSnapshotDbtValidToCurrent):
    def test_valid_to_current(self, project):
        """Same as the base test, but with deterministic ordering.

        The base test indexes rows of an un-ordered SELECT, relying on
        Postgres heap order; Vertica returns rows in projection sort order,
        so assert on (id, valid_to) groups instead.
        """
        project.run_sql(_base_module.create_seed_sql)
        project.run_sql(_base_module.create_snapshot_expected_sql)
        project.run_sql(_base_module.seed_insert_sql)
        project.run_sql(_base_module.populate_snapshot_expected_valid_to_current_sql)

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        original_snapshot = run_sql_with_adapter(
            project.adapter,
            "select id, test_scd_id, test_valid_to from {schema}.snapshot_actual"
            " order by id, test_valid_to",
            "all",
        )
        assert len(original_snapshot) == 20
        assert all(
            row[2] == datetime.datetime(2099, 12, 31, 0, 0) for row in original_snapshot
        )

        project.run_sql(_base_module.invalidate_sql)
        project.run_sql(_base_module.update_with_current_sql)

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        updated_snapshot = run_sql_with_adapter(
            project.adapter,
            "select id, test_scd_id, test_valid_to from {schema}.snapshot_actual"
            " order by id, test_valid_to",
            "all",
        )
        valid_tos = {}
        for row in updated_snapshot:
            valid_tos.setdefault(row[0], []).append(row[2])

        current = datetime.datetime(2099, 12, 31, 0, 0)
        for row_id, row_valid_tos in valid_tos.items():
            if 10 <= row_id <= 20:
                # updated ids: one closed-out row and one current row
                assert len(row_valid_tos) == 2, row_id
                assert row_valid_tos[0] != current, row_id
                assert row_valid_tos[1] == current, row_id
            else:
                assert row_valid_tos == [current], row_id


class TestVerticaSnapshotMultiUniqueKey(BaseSnapshotMultiUniqueKey):
    pass
