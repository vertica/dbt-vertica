## Changelog
- This file provides a full account of all changes to dbt-vertica.
- "Breaking changes" listed under a version may require action from end users.



### 1.7.13

#### Fixes:
- Enable connection autocommit parameter(https://github.com/vertica/dbt-vertica/issues/128)
- Fixed- SQLParse with a high vulnerability(https://github.com/vertica/dbt-vertica/security/dependabot/1)
- Update collect_freshness(https://github.com/vertica/dbt-vertica/issues/129)
- Incremental strategy, On schema change- Add or remove columns from table(https://github.com/vertica/dbt-vertica/issues/122)

### 1.7.3

#### Features:
- New capability support structure for adapters
- Metadata freshness checks
- Catalog fetch performance improvements
- Behavior of dbt show's --limit flag
- Migrate date_spine() Macro from dbt-utils to Core
- Data Spine Tests
- Storing Test Failures as View
- Additional Tests

#### Fixes:

- Metadata freshness checks Tests
  - TestGetLastRelationModified
  - TestListRelationsWithoutCachingSingle
  - TestListRelationsWithoutCachingFull
- Behavior of dbt show's --limit flag Tests
  - BaseShowSqlHeader
  - BaseShowLimit
- Data Spine Tests
  - TestDateSpine
  - TestGenerateSeries
  - TestGetIntervalsBetween
  - TestGetPowersOfTwo
- Storing Test Failures as View
  - TestStoreTestFailuresAsInteractions
  - TestStoreTestFailuresAsProjectLevelOff
  - TestStoreTestFailuresAsProjectLevelView
  - TestStoreTestFailuresAsGeneric
  - TestStoreTestFailuresAsProjectLevelEphemeral
  - TestStoreTestFailuresAsExceptions
- Additional Tests
  - TestCloneSameTargetAndState
  - SeedUniqueDelimiterTestBase 
  - TestSeedWithWrongDelimiter
  - TestSeedWithEmptyDelimiter

### 1.6.0

#### Features:
- Added support for [`dbt-core version 1.6.0`](https://github.com/dbt-labs/dbt-core/discussions/7958) according to DBT guidelines.
- Added support of oAuth authentication.
- New `clone` command.
- Droped support for Python 3.7. 

#### Fixes:
- Ensure support for revamped `dbt debug`.
- New limit arg for `adapter.execute()`
- Configuring composite unique key for incremental merge or insert+update strategy
- Added new functional tests and parameterize them by overriding fixtures:
  - TestIncrementalConstraintsRollback 
  - TestTableContractSqlHeader 
  - TestIncrementalContractSqlHeader 
  - TestModelConstraintsRuntimeEnforcement 
  - TestConstraintQuotedColumn 
  - TestEquals 
  - TestMixedNullCompare 
  - TestNullCompare 
  - TestVerticaCloneNotPossible 
  - TestValidateSqlMethod 
  

### 1.5.0
#### Features:
- Added support for [`dbt-core version 1.5.0`](https://github.com/dbt-labs/dbt-core/discussions/7213) according to DBT guidelines. 
- Support for Python 3.11.
#### Fixes:
- Added support for `constraints` data structure. 
- Implemented `data_type_code_to_name` to convert Python connector return types to strings.
- In both `create_table_as` and `create_view_as` macros, raised an explicit warning if a model is configured with an enforced contract
- Added new functional tests and parameterize them by overriding fixtures:
  - TestTableConstraintsColumnsEqual
  - TestViewConstraintsColumnsEqual
  - TestIncrementalConstraintsColumnsEqual
  - TestTableConstraintsRuntimeDdlEnforcement
  - TestIncrementalConstraintsRuntimeDdlEnforcement
  - TestModelConstraintsRuntimeEnforcement  

### 1.4.4
#### Features:
- Added support for [`dbt-core version 1.4.0`](https://github.com/dbt-labs/dbt-core/discussions/6624) according to DBT guidelines. 
- Support for Python 3.11.
#### Fixes:
- Merge strategy config parameter `merge_update_columns` is now working as intended. 
- The incremental flag `--full-refresh` is now working as intended.
### 1.3.0
#### Features:
- Added support for [`dbt-core version 1.3.0`](https://github.com/dbt-labs/dbt-core/discussions/6011) and migrated testing framework to new testing framework according to DBT guidelines. 
- Support for incremental model strategy ‘Append’. 
- Support for incremental model strategy ‘insert_overwrite’.
- Support for multiple optimization parameters for table materialization:
  - order_by
  - segmented_by_string
  - segmented_by_all_nodes
  - no_segmentation
  - ksafe
  - partition_by_string
  - partition_by_group_by_string
  - partition_by_active_count
- Support for enabling privileges inheritance for tables/views using INCLUDE SCHEMA PRIVILEGES by default in model materialization. If not required, can be disabled using EXCLUDE in the Vertica Server.
- Defined profile_template which helps user to configure profile while creating the project.
- Support for Python 3.10.
#### Fixes:
- Incremental materialization refactoring and cleanup.
- Updates to correctly handle errors for multi-statement queries.
#### Breaking Changes
##### Change description:
- Refactored `merge_columns` config parameter to `unique_key`.
- Support for the `merge_update_columns` to only merge the columns specified.
##### Impact:
- For the incremental model strategy ‘delete+insert’ and ‘merge’, `unique_key` is now a required parameter and it fails if not provided. 
- Existing applications using config parameter `merge_columns` will give an error because `merge_columns` as been removed.
##### Workaround/Solution:
- When using the incremental model strategy ‘delete+insert’ and ‘merge’ pass the required parameter `unique_key` instead of `merge_columns` in config and `merge_update_columns` is used to only merge the columns specified.
### 1.0.3
- Refactored the adapter to model after dbt's global_project macros
- Unimplemented functions should throw an exception that it's not implemented. If you stumble across this, please open an Issue or PR so we can investigate.
### 1.0.2
- Added support for snapshot timestamp with passing tests
- Added support for snapshot check cols with passing tests
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
