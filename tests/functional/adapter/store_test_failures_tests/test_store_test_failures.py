

from dbt.tests.adapter.store_test_failures_tests import basic
from dbt.tests.adapter.store_test_failures_tests.test_store_test_failures import (
    TestStoreTestFailures,
)

#pass
class TestSnowflakeStoreTestFailures(TestStoreTestFailures):
    pass

#pass
class TestStoreTestFailuresAsInteractions(basic.StoreTestFailuresAsInteractions):
    pass

#pass
class TestStoreTestFailuresAsProjectLevelOff(basic.StoreTestFailuresAsProjectLevelOff):
    pass

#pass
class TestStoreTestFailuresAsProjectLevelView(basic.StoreTestFailuresAsProjectLevelView):
    pass

#pass
class TestStoreTestFailuresAsGeneric(basic.StoreTestFailuresAsGeneric):
    pass

#pass
class TestStoreTestFailuresAsProjectLevelEphemeral(basic.StoreTestFailuresAsProjectLevelEphemeral):
    pass

#pass
class TestStoreTestFailuresAsExceptions(basic.StoreTestFailuresAsExceptions):
    pass
