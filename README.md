# dbt-vertica

Your [dbt](https://www.getdbt.com/) adapter for [Vertica](https://www.vertica.com/).

Uses [vertica-python](https://github.com/vertica/vertica-python) to connect to Vertica database.

## Changes

### 1.0.1

- Fixed the Incremental method implementation (was buggy/incomplete)
   - Removed the `unique_id` as it wasn't implemented
   - Fixed when no fields were added - full table merge
- Added testing for Incremental materialization
  - Testing for dbt Incremental full table
  - Testing for dbt Incremental specified merged columns
- Added more logging to the connector to help understand why tests were failing
- Using the official [Vertica CE 11.0.x docker image](https://hub.docker.com/r/vertica/vertica-ce) now for tests

### 1.0.0

- Add support for DBT version 1.0.0

### 0.21.1

- Add testing, fix schema drop.

### 0.21.0

- Add `unique_field` property on connection, supporting 0.21.x.

### 0.20.2

- Added SSL options.

### 0.20.1
- Added the required changes from dbt 0.19.0. [Details found here](https://docs.getdbt.com/docs/guides/migration-guide/upgrading-to-0-19-0#for-dbt-plugin-maintainers).
- Added support for the MERGE command for incremental loading isntead of DELETE+INSERT

## Install

```
pip install dbt-vertica
```

You don't need to install dbt separately. Installing `dbt-vertica` will also install `dbt-core` and `vertica-python`.

## Sample Profile Configuration

```yaml
your-profile:
  outputs:
    dev:
      type: vertica # Don't change this!
      host: vertica-host-name
      port: 5433 # or your custom port (optional)
      username: your-username
      password: your-password
      database: vertica-database-name
      schema: your-default-schema
  target: dev
```

By default, `dbt-vertica` will request `ConnectionLoadBalance=true` (which is generally a good thing), and set a session label of `dbt_your-username`.

There are three options for SSL: `ssl`, `ssl_env_cafile`, and `ssl_uri`.
See their use in the code [here](https://github.com/mpcarter/dbt-vertica/blob/d15f925049dabd2833b4d88304edd216e3f654ed/dbt/adapters/vertica/connections.py#L72-L87).

## Reach out!

First off, I would not have been able to make this adapater if the smart folks at dbt labs didn't make it so easy. That said, it seems every database has its own little quirks. I ran into several different issues when adapting the macros to Vertica. If you find something not working right, please open an issue (assuming it has to do with the adapter and not dbt itself).

Also, I would be excited to hear about anyone who is able to benefit from using dbt with Vertica. (Just open an issue to leave me a comment.)

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

    pip3 install pytest-dbt-adapter==0.6.0

Run tests via:

    pytest tests/integration.dbtspec
    # run an individual test with increased logging:
    pytest tests/integration.dbtspec::test_dbt_base -xs --ff
