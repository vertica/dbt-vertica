# dbt-vertica

Your [dbt](https://www.getdbt.com/) adapter for [Vertica](https://www.vertica.com/).

Built on dbt 0.21.0

Uses [vertica-python](https://github.com/vertica/vertica-python) to connect to Vertica database.

## Changes

### 0.21.0

- Add `unique_field` property on connection.

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

First off, I would not have been able to make this adapater if the smart folks at Fishtown Analytics didn't make it so easy. That said, it seems every database has its own little quirks. I ran into several different issues when adapting the macros to Vertica. If you find something not working right, please open an issue (assuming it has to do with the adapter and not dbt itself).

Also, I would be excited to hear about anyone who is able to benefit from using dbt with Vertica. (Just open an issue to leave me a comment.)

## Develop

Run a local Vertica instance like:

    docker run -p 5433:5433 jbfavre/vertica:9.2.0-7_centos-7

You need the pytest dbt adapter:

    pip3 install pytest-dbt-adapter==0.5.0

Run tests via:

    pytest tests/integration.dbtspec
    # run an individual test with increased logging:
    pytest tests/integration.dbtspec::test_dbt_base -xs --ff