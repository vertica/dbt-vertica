from dbt.tests.adapter.simple_snapshot.new_record_check_mode import (
    BaseSnapshotNewRecordCheckMode,
)
from dbt.tests.adapter.simple_snapshot.new_record_dbt_valid_to_current import (
    BaseSnapshotNewRecordDbtValidToCurrent,
)
from dbt.tests.adapter.simple_snapshot.new_record_timestamp_mode import (
    BaseSnapshotNewRecordTimestampMode,
)


class TestVerticaSnapshotNewRecordTimestampMode(BaseSnapshotNewRecordTimestampMode):
    pass


class TestVerticaSnapshotNewRecordCheckMode(BaseSnapshotNewRecordCheckMode):
    pass


class TestVerticaSnapshotNewRecordDbtValidToCurrent(BaseSnapshotNewRecordDbtValidToCurrent):
    pass
