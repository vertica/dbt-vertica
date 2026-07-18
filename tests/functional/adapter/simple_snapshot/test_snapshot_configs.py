from dbt.tests.adapter.simple_snapshot.test_various_configs import (
    BaseSnapshotColumnNames,
    BaseSnapshotColumnNamesFromDbtProject,
    BaseSnapshotDbtValidToCurrent,
    BaseSnapshotInvalidColumnNames,
    BaseSnapshotMultiUniqueKey,
)


class TestVerticaSnapshotColumnNames(BaseSnapshotColumnNames):
    pass


class TestVerticaSnapshotColumnNamesFromDbtProject(BaseSnapshotColumnNamesFromDbtProject):
    pass


class TestVerticaSnapshotInvalidColumnNames(BaseSnapshotInvalidColumnNames):
    pass


class TestVerticaSnapshotDbtValidToCurrent(BaseSnapshotDbtValidToCurrent):
    pass


class TestVerticaSnapshotMultiUniqueKey(BaseSnapshotMultiUniqueKey):
    pass
