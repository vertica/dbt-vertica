from dbt.adapters.vertica.connections import verticaConnectionManager
from dbt.adapters.vertica.connections import verticaCredentials
from dbt.adapters.vertica.impl import verticaAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import vertica


Plugin = AdapterPlugin(
    adapter=verticaAdapter,
    credentials=verticaCredentials,
    include_path=vertica.PACKAGE_PATH)
