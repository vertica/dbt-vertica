# dbt-vertica

[![PyPI version](https://badge.fury.io/py/dbt-vertica.svg)](https://badge.fury.io/py/dbt-vertica)
[![License](https://img.shields.io/badge/License-Apache%202.0-orange.svg)](https://opensource.org/licenses/Apache-2.0)

[dbt](https://www.getdbt.com/) adapter for [Vertica](https://www.vertica.com/). The adapter uses [vertica-python](https://github.com/vertica/vertica-python) to connect to your Vertica database.

For more information on using dbt with Vertica, consult the [Vertica-Setup](https://docs.getdbt.com/reference/warehouse-setups/vertica-setup) and [Configuration](https://docs.getdbt.com/reference/resource-configs/vertica-configs) pages.


## dbt-vertica Versions Tested 
dbt-vertica has been developed using the following software and versions: 
* Vertica Server 12.0.3-0
* Python 3.11
* vertica-python client 1.3.1
* dbt-core 1.4.4
* dbt-tests-adapter 1.4.4

## Supported Features
### dbt Core Features
Below is a table for what features the current Vertica adapter supports for dbt. This is constantly improving and changing as both dbt adds new functionality, as well as the dbt-vertica driver improves. This list is based upon dbt 1.3.0
|                dbt Core Features                  | Supported   |
| ------------------------------------------------- | ----------- |
| Table Materializations                            | Yes         |
| Ephemeral Materializations                        | Yes         |
| View Materializations                             | Yes         |
| Incremental Materializations - Append             | Yes         |
| Incremental Materailizations - Merge              | Yes         |
| Incremental Materializations - Delete+Insert      | Yes         |
| Incremental Materializations - Insert_Overwrite   | Yes         |
| Snapshots - Timestamp                             | Yes         |
| Snapshots - Check Cols                            | No  |
| Seeds                                             | Yes         |
| Tests                                             | Yes         |
| Documentation                                     | Yes         |
| External Tables                                   | Untested    |
* **Yes** - Supported, and tests pass.
* **No** - Not supported or implemented.
* **Untested** - May support out of the box, though hasn't been tested.
* **Passes Test** -The testes have passed, though haven't tested in a production like environment

## Installation
```
$ pip install dbt-vertica
```
You don't need to install dbt separately. Installing `dbt-vertica` will also install `dbt-core` and `vertica-python`.
## Sample Profile Configuration
```profiles.yml

your-profile:
  outputs:
    dev:
      type: vertica # Don't change this!
      host: [hostname]
      port: [port] # or your custom port (optional)
      username: [your username] 
      password: [your password] 
      database: [database name] 
      schema: [dbt schema] 
      connection_load_balance: True
      backup_server_node: [list of backup hostnames or IPs]
      retries: [1 or more]
      threads: [1 or more] 
  target: dev

```
### Description of Profile Fields:

| Property | Description | Required? | Default Value | Example |
| -------- | ----------- | --------- | ------------- | ------- |
|  type	   | The specific adapter to use. |	Yes	| None | vertica |
| host	| The host name or IP address of any active node in the Vertica Server. |	Yes |	None |	127.0.0.1 |
| port |	The port to use, default or custom. |	Yes	| 5433 | 5433 |
| username | The username to use to connect to the server. | Yes | None	| dbadmin |
| password | The password to use for authenticating to the server. | Yes | None | my_password |
| database | The name of the database running on the server. | Yes | None | my_db |
| schema | The schema to build models into. | No | None | VMart |
| connection_load_balance | A Boolean value that indicates whether the connection can be redirected to a host in the database other than host. | No | true | true |
| backup_server_node | List of hosts to connect to if the primary host specified in the connection (host, port) is unreachable. Each item in the list should be either a host string (using default port 5433) or a (host, port) tuple. A host can be a host name or an IP address. | No | none | ['123.123.123.123','www.abc.com',('123.123.123.124',5433)]
| retries | The retry times after an unsuccessful connection. | No | 2 | 3 |
| threads | The number of threads the dbt project will run on. | No | 1 | 3 |
| label | A session label to identify the connection. | No | An auto-generated label with format of: dbt_username	| dbt_dbadmin |

For more information on Vertica’s connection properties please refer to [Vertica-Python](https://github.com/vertica/vertica-python#create-a-connection) Connection Properties.




## Changelog

See the [changelog](https://github.com/vertica/dbt-vertica/blob/master/CHANGELOG.md)


## Contributing guidelines

Have a bug or an idea? Please see [CONTRIBUTING.md](https://github.com/vertica/dbt-vertica/blob/master/CONTRIBUTING.md) for details

## Develop

Run a local Vertica instance like:

    docker run -p 5433:5433 \
               -p 5444:5444 \
               -e VERTICA_DB_NAME=docker \
               -e VMART_ETL_SCRIPT="" \
               -e VMART_ETL_SQL="" \
               vertica/vertica-ce

Access the local Vertica instance like:

    docker exec -it <docker_image_name> /opt/vertica/bin/vsql


You need the pytest dbt adapter:

    pip3 install  dbt-tests-adapter==1.4.4

Run tests via:
  
    pytest tests/functional/adapter/
    # run an individual test 
    pytest tests/functional/adapter/test_basic.py

    
