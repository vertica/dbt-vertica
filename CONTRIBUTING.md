Firstly, we appreciate you thinking about contributing to dbt-vertica so that it can continue to evolve.
This document will guide you through the contribution process. There are several ways you can help:

 - [Bug Reports](#bug-reports)
 - [Feature Requests](#feature-requests)
 - [Code Contributions](#code-contributions)

# Bug Reports

If you find a bug, submit an [issue](https://github.com/vertica/dbt-vertica/issues) with a complete and reproducible bug report. If the issue can't be reproduced, it will be closed. If you opened an issue, but figured out the answer later on your own, comment on the issue to let people know, then close the issue.

For issues (e.g. security related issues) that are **not suitable** to be reported publicly on the GitHub issue system, report your issues to [Vertica open source team](mailto:vertica-opensrc@microfocus.com) directly or file a case with Vertica support if you have a support account.

# Feature Requests

Feel free to share your ideas for how to improve *dbt-vertica*. We’re always open to suggestions.
You can open an [issue](https://github.com/vertica/dbt-vertica/issues)
with details describing what feature(s) you'd like added or changed.

If you would like to implement the feature yourself, open an issue to ask before working on it. Once approved, please refer to the [Code Contributions](#code-contributions) section.

# Code Contributions

## Step 1: Fork
Fork the project [on Github](https://github.com/vertica/dbt-vertica)  and check out your copy locally.
```shell
git clone git@github.com:YOURUSERNAME/dbt-vertica.git
cd dbt-vertica
```
Your GitHub repository **YOURUSERNAME/dbt-vertica** will be called "origin" in Git. 
You should also set up **vertica/dbt-vertica** as an "upstream" remote.

```shell
git remote add upstream git@github.com:vertica/dbt-vertica.git
git fetch upstream
```
### Configure Git for the first time

Make sure git knows your [name](https://help.github.com/articles/setting-your-username-in-git/ "Set commit username in Git") and [email address](https://help.github.com/articles/setting-your-commit-email-address-in-git/ "Set commit email address in Git"):

```shell
git config --global user.name "John Smith"
git config --global user.email "email@example.com"
```

## Step 2: Branch

Create a new branch for the work with a descriptive name:
```shell
git checkout -b my-fix-branch
```

## Step 3: Install dependencies

Install the dbt-vertica dependencies for development:
```shell

pip install dbt-vertica

```

## Step 4: Get the test suite running

*dbt-vertica* comes with a test suite of its own, in the Tests directory of the code base. It’s our policy to make sure all tests always pass.
We appreciate any and all contributions to the test suite! These tests use a Pytest framework: testing. You might want to check out the testing documentation for more details.

Syntax tests do simple syntax testing of individual metrics and configs, which do not require database connection. Examples of running tests:

Run all test :
```shell
$ cd /tests
```
```shell
$ pytest functional/adapter/
```

The Github Actions CI workflow committed as part of the project will automatically run test suite through different pytest versions. These CI tests must pass before any PR will be considered. This CI workflow can be run on your forked repository after you are enabling Github Actions on your fork.

## Step 5: Implement your fix or feature

At this point, you're ready to make your changes! Feel free to ask for help; everyone is a beginner at first.
## License Headers
Every file in this project must use the following Apache 2.0 header (with the appropriate year or years in the "[yyyy]" box; if a copyright statement from another party is already present in the code, you may add the statement on top of the existing copyright statement):

```
Copyright (c) [yyyy] Micro Focus or one of its affiliates.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
   http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

### Commits
Make some changes on your branch, then stage and commit as often as necessary:
```shell
git add.
git commit -m 'Added two more tests for #166'
```
When writing the commit message, try to describe precisely what the commit does. The commit message should be in lines of 72 chars maximum. Include the issue number #N if the commit is related to an issue.

### Tests
Add appropriate tests for the bug’s or feature's behavior, run the test suite again and ensure that all tests pass. Here is the guideline for writing test:
 - Tests should be easy for any contributor to run. Contributors may not get complete access to their Vertica database, for example, they may only have a non-admin user with write privileges to a single schema, and the database may not be the latest version. We encourage tests to use only what they need and nothing more.
 - If there are requirements to the database for running a test, the test should adapt to different situations and never report a failure. For example, if a test depends on a multi-node database, it should check the number of DB nodes first, and skip itself when it connects to a single-node database.

## Step 6: Push and Rebase

You can publish your work on GitHub just by doing:
```shell
$ git push origin my-fix-branch
```
When you go to your GitHub page, you will notice commits made on your local branch is pushed to the remote repository.
When upstream (vertica/dbt-vertica) has changed, you should **rebase** your work. The rebase command creates a linear history by moving your local commits onto the tip of the upstream commits.

You can rebase your branch locally and force-push to your GitHub repository by doing:

```shell
git checkout my-fix-branch
git fetch upstream
git rebase upstream/master
git push -f origin my-fix-branch
```

## Step 7: Make a Pull Request

When you think your work is ready to be pulled into dbt-vertica, you should create a pull request (PR) at GitHub.
A good pull request means:

- commits with one logical change in each
- well-formed messages for each commit
- documentation and tests, if needed
- Go to https://github.com/YOURUSERNAME/dbt-vertica  and [make a Pull Request](https://help.github.com/articles/creating-a-pull-request/) to vertica:master.

**Note: When you initate a contribution some background tests run to verify you proposed changes pass. These tests can take 5 to 10 minutes to execute, so be patient. It is out there doing something when you see the pause in activity.**

### Sign the CLA

Before we can accept a pull request, we first ask people to sign a Contributor License Agreement (or CLA). We ask this so that we know that contributors have the right to donate the code. You should notice a comment from **CLAassistant**  on your pull request page, follow this comment to sign the CLA electronically.

### Review

Pull requests are usually reviewed within a few days. If there are comments to address, apply your changes in new commits, rebase your branch and force-push to the same branch, re-run the test suite to ensure tests are still passing. We care about quality, Vertica has internal test suites to run as well, so your pull request won't be merged until all internal tests pass. To produce a clean commit history, our maintainers would do squash merging once your PR is approved, which means combining all commits of your PR into a single commit in the master branch.

You are done! Thank you for your code contribution!

After your pull request is merged, you can safely delete your branch and pull the changes from the upstream repository.