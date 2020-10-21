# dbt-vertica

Your [dbt](https://www.getdbt.com/) adapter for [Vertica](https://www.vertica.com/).

Built on dbt 0.18.x

Uses [vertica-python](https://github.com/vertica/vertica-python) to connect to Vertica database. 

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

## Reach out!

First off, I would not have been able to make this adapater if the smart folks at Fishtown Analytics didn't make it so easy. That said, it seems every database has its own little quirks. I ran into several different issues when adapting the macros to Vertica. If you find something not working right, please open an issue (assuming it has to do with the adapter and not dbt itself). 

Also, I would be excited to hear about anyone who is able to benefit from using dbt with Vertica. (Just open an issue to leave me a comment.)
