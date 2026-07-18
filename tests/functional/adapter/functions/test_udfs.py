from dbt.tests.adapter.functions.test_udfs import UDFsBasic


class TestVerticaSqlUdfs(UDFsBasic):
    """Vertica SQL (scalar) UDFs created via dbt function models.

    Vertica SQL functions do not accept a volatility specifier, so only the
    base scenario (no volatility config) is exercised; the Deterministic /
    Stable / NonDeterministic variants from the base suite do not apply.
    """

    pass
