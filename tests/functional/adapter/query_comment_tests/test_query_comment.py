import pytest

from dbt.tests.adapter.query_comment.test_query_comment import (
    BaseQueryComments,
    BaseMacroQueryComments,
    BaseMacroArgsQueryComments,
    BaseMacroInvalidQueryComments,
    BaseNullQueryComments,
    BaseEmptyQueryComments,
)


class TestQueryCommentsVertica(BaseQueryComments):
    pass

class TestMacroQueryCommentsVertica(BaseMacroQueryComments):
    pass

class TestMacroArgsQueryCommentsVertica(BaseMacroArgsQueryComments):
    pass

class TestMacroInvalidQueryCommentsVertica(BaseMacroInvalidQueryComments):
    pass

class TestNullQueryCommentsVertica(BaseNullQueryComments):
    pass

class TestEmptyQueryCommentsVertica(BaseEmptyQueryComments):
    pass