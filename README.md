# dbt-vertica

Your [dbt](https://www.getdbt.com/) adapter for [Vertica](https://www.vertica.com/).

Built on dbt 0.15.x

Uses [vertica-python](https://github.com/vertica/vertica-python) to connect to Vertica database. 

## Install

> No pip install yet available

## Profile Configuration

```
      type: vertica
      host: vertica-host-name
      port: 5433 or your custom port
      username: your-username
      password: your-password
      database: vertica-database-name
      schema: your-default-schema
```

