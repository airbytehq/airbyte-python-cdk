# Airbyte Python CDK - Contributing Guide

Learn how you can become a contributor to the Airbyte Python CDK.

Thank you for being interested in contributing to Airbyte Python CDK! Here are some guidelines to get you started:

- We adhere to the Airbyte [code of conduct](https://docs.airbyte.com/community/code-of-conduct).
- You can contribute by reporting bugs, posting GitHub discussions, opening issues, improving docs, and submitting pull requests with bug fixes and new features.
- If you're changing the code, please add unit tests for your change.
- When submitting issues or PRs, please add a small reproduction project. Using the changes in your connector and providing that connector code as an example (or a satellite PR) helps!

## First Time Setup

Here are some tips to get started using the project dependencies and development tools:

1. Make sure your Python version is 3.11

Fedora 41:

```bash
sudo dnf install python3.11
```

2. [Install pip](https://pip.pypa.io/en/stable/installation/)

Fedora 41:

```bash
sudo dnf install pip
```

3. [Install Poetry](https://python-poetry.org/docs/#) 2.0 or higher:

```bash
pip install poetry
```

or

```bash
curl -sSL https://install.python-poetry.org | python3 -
```
Fedora 41:

```bash
sudo dnf install poetry
```
Note: You can use "Poe" tasks to perform common actions such as lint checks (`poetry run poe lint`), autoformatting (`poetry run poe format-fix`), etc. For a list of tasks you can run, try `poetry run poe list`. See [Poe the Poet](https://poethepoet.natn.io/)

4 Use the Python 3.11 environment:

```bash
poetry env info
# validate 3.11 is active

# otherwise:
poetry env list

# Use the proper version:
poetry env use /usr/bin/python3.11
poetry env info
# validate 3.11 referred
```

5. Clone the CDK repo. If you will be testing connectors, you should clone the CDK into the same parent directory as `airbytehq/airbyte`, which contains the connector definitions.
6. In the **airbyte-python-cdk project** install the unit tests' prerequisites:

```bash
poetry install --all-extras
```
Note: By default in Poetry 2.0, `poetry lock` only refreshes the lockfile without pulling new versions. This is the same behavior as the previous `poetry lock --no-update` command.
1. You can use "Poe" tasks to perform common actions such as lint checks (`poetry run poe lint`), autoformatting (`poetry run poe format-fix`), etc. For a list of tasks you can run, try `poetry run poe list`.

7 If your operating system is RHEL or compatible, execute:

```bash
# just for the current session, until restart
sudo modprobe iptable_nat

Fedora 41:

```bash
# just for the current session, until restart
sudo modprobe iptable_nat

# include the nat module to survive restart
echo iptable_nat | sudo tee -a /etc/modules-load.d/modules
```

See also:

- [Dager-Podman Integration](https://blog.playgroundtech.io/introduction-to-dagger-6ab55ee28723)
- [CDK Issue 197](https://github.com/airbytehq/airbyte-python-cdk/issues/197)

8. Make sure Docker is installed locally, instead of Podman
   See also:

- [CDK Issue 197](https://github.com/airbytehq/airbyte-python-cdk/issues/197)

9. Edit airbyte/airbyte-integrations/connectors/source-shopify/acceptance-test-config.yml and change:

```
   connector_image: airbyte/source-shopify:dev    to    connector_image: airbyte/source-shopify:<version>
```

where the **version** comes from airbyte/airbyte-integrations/connectors/source-shopify/metadata.yaml, as
the value of the **dockerImageTag** parameter.

## Local development

Iterate on the CDK code locally.

### Run Unit Tests
To see all available `ruff` options, run `poetry run ruff`.

- `poetry run pytest` to run all unit tests.
- `poetry run pytest -k <suite or test name>` to run specific unit tests.
- `poetry run pytest-fast` to run the subset of PyTest tests, which are not flagged as `slow`. (It should take <5 min for fast tests only.)
- `poetry run pytest -s unit_tests` if you want to pass pytest options.

### Run Code Formatting

- `poetry run ruff check .` to report the formatting issues.
- `poetry run ruff check --fix` to fix the formatting issues.
- `poetry run ruff format` to format your Python code.

### Run Code Linting

- `poetry run poe lint` for lint checks.
- `poetry run poe check-local` to lint all code, type-check modified code, and run unit tests with coverage in one command.
- `poetry run mypy --config-file mypy.ini airbyte_cdk` to validate the code. Resolve the reported issues.

### More tools and options

To see all available scripts and options, run:

- `poetry run ruff`
- `poetry run pytest --help`
- `poetry run poe`

### Test locally CDK Changes against Connectors (integration tests)

When developing a new feature in the CDK, you may find it necessary:

- to run a connector that uses that new feature
- or use an existing connector to validate a new feature
- or fix it in the CDK.

In this project, the [GitHub pipelines](.github/workflows/connector-tests.yml) run such tests against the Shopify source as **integration tests**.

**Assumptions:**

- The test connector is in the [Airbyte project](https://github.com/airbytehq/airbyte).
- The [Airbyte project](https://github.com/airbytehq/airbyte) is checked out in `airbyte` directory.
- The [CDK development](https://github.com/airbytehq/airbyte-python-cdk) project is checked out in the `airbyte-python-cdk` directory - a sibling of the `airbyte` directory.

**Preparation steps**

- In the `airbyte` project run:

```bash
cd airbyte/airbyte-integrations/bases/connector-acceptance-test/
poetry install
```

- Edit the `airbyte/airbyte-integrations/connectors/<test connector>/pyproject.toml` file.
  Replace the line with `airbyte_cdk` with the following (see the assumptions above):

```toml
airbyte_cdk = { path = "../../../../airbyte-python-cdk", develop = true }
```

- In `airbyte/airbyte-integrations/connectors/<test connector>` reinstall `airbyte_cdk` from your local working directory:

```bash
cd airbyte/airbyte-integrations/connectors/<test connector>
poetry install
```

- In `airbyte/airbyte-integrations/connectors/<test connector>/` create the `secrets/config.json` file.
  For example, use the Shopify connector for the integration test:
  - Register in shopify.com as of [Shopify test connector](https://docs.airbyte.com/integrations/sources/shopify):
  - Use the generated **Admin API Access Token** as the API **password** in the configuration file.
  - On the **Settings / Domains** page find the subdomain of _myshopify.com_ and use it as the **shop** in the configuration file.
    Example:
    `     domain: nw0ipt-vr.myshopify.com
    shop: nw0ipt-vr
    `
    Example contents:

```json
{
  "shop": "nw0ipt-vr",
  "start_date": "2020-11-01",
  "credentials": {
    "auth_method": "api_password",
    "api_password": "shpat_XXXXXXXXXXX"
  }
}
```

- See also:
  - [Acceptance Tests Reference](https://docs.airbyte.com/connector-development/testing-connectors/connector-acceptance-tests-reference)
  - [Connector Acceptance Tests](https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/bases/connector-acceptance-test/README.md)

**Steps:**

- Run the connector's tests (see the connector's README.md)

```bash
cd airbyte/airbyte-integrations/connectors/<connector name>

poetry run <connector name> spec
poetry run <connector name> check --config secrets/config.json
poetry run <connector name> discover --config secrets/config.json
poetry run <connector name> read --config secrets/config.json --catalog integration_tests/<connector name>.json
poetry run pytest
```

Example:

```bash
cd airbyte/airbyte-integrations/connectors/source-shopify

poetry run source-shopify spec
poetry run source-shopify check --config secrets/config.json
poetry run source-shopify discover --config secrets/config.json
poetry run source-shopify read --config secrets/config.json --catalog integration_tests/configured_catalog.json
poetry run pytest
```

- Run the acceptance tests locally:

```bash
cd airbyte/airbyte-integrations/bases/connector-acceptance-test

poetry run pytest -p connector_acceptance_test.plugin --acceptance-test-config=../../connectors/<connector name>
```

Example:

```bash
poetry run pytest -p connector_acceptance_test.plugin --acceptance-test-config=../../connectors/source-shopify

# or with debug option:
poetry run pytest -p connector_acceptance_test.plugin --acceptance-test-config=../../connectors/source-shopify --pdb

# or with timeout option:
poetry run pytest -p connector_acceptance_test.plugin --acceptance-test-config=../../connectors/source-shopify --timeout=30
```

- When testing is complete, revert your test changes.

### Test CDK Changes against Connectors

**Preparation steps**

- Install the [`airbyte-ci` CLI](https://github.com/airbytehq/airbyte/blob/master/airbyte-ci/connectors/pipelines/README.md)

**Steps:**

- Build your connector image with the local CDK using:

```bash
cd airbyte
airbyte-ci connectors --use-local-cdk --name=<CONNECTOR> build
```

- Use the `test` command with `--use-local-cdk` to run the full set of connector tests, including connector acceptance tests (CAT) and the connector's unit tests:

```bash
cd airbyte
airbyte-ci connectors --use-local-cdk --name=<CONNECTOR> test
```

Note that the local CDK is injected at build time, so if you make changes, you must rerun the build command to see them reflected.

## Working with Poe Tasks

The Airbyte CDK defines common development tasks using [Poe the Poet](https://poethepoet.natn.io/). You can run `poetry run poe list` to see all available tasks. This will work after `poetry install --all-extras` without additional installations.

Optionally, if you can [pre-install Poe](https://poethepoet.natn.io/installation.html) with `pipx install poethepoet` and then you will be able to run Poe tasks with the shorter `poe TASKNAME` syntax instead of `poetry run poe TASKNAME`.
The Ruff configuration is stored in `ruff.toml` at the root of the repository. This file contains settings for line length, target Python version, and linting rules.

## Auto-Generating the Declarative Schema File

Low-code CDK models are generated from `sources/declarative/declarative_component_schema.yaml`. If the iteration you are working on includes changes to the models or the connector generator, you may need to regenerate them. To do that, you can run:

```bash
poetry run poe build
```

This will generate the code generator docker image and the component manifest files based on the schemas and templates.

## Generating API Reference Docs

Documentation auto-gen code lives in the `/docs` folder. Based on the doc strings of public methods, we generate API documentation using [pdoc](https://pdoc.dev).

To generate the documentation, run `poe docs-generate` or to build and open the docs preview in one step, run `poe docs-preview`.

The `docs-generate` Poe task is mapped to the `run()` function of `docs/generate.py`. Documentation pages will be generated in the `docs/generated` folder (ignored by git). You can also download auto-generated API docs for any GitHub push by navigating to the "Summary" tab of the docs generation check-in GitHub Actions.

## Release Management

Please have a look at the [Release Management](./RELEASES.md) guide for information on performing releases and pre-releases.

## FAQ

### Q: Who are "maintainers"?

For this documentation, "maintainers" are those who have write permissions (or higher) on the repo. Generally, these are Airbyte team members.

### Q: Where should connectors put integration tests?

Only tests within the `unit_tests` directory will be run by `airbyte-ci`. If you have integration tests that should also run, the common convention is to place these in the `unit_tests/integration` directory. This ensures they will be run automatically in CI and before each new release.

### Q: What GitHub slash commands are available and who can run them?

Only Airbyte CDK maintainers can run slash commands. The most common slash commands are as follows:

- `/autofix`- Corrects any linting or formatting issues and commits the change back to the repo.
- `/poetry-lock` - Re-locks dependencies and updates the `poetry.lock` file, then commits the changes back to the repo. This is helpful after merging in updates from main or when creating a PR in the browser - such as for version bumps or dependency updates directly in the PR.

The full list of available slash commands can be found in the [slash command dispatch file](https://github.com/airbytehq/airbyte-python-cdk/blob/main/.github/workflows/slash_command_dispatch.yml#L21-L25).

# Appendix: Advanced Topics

## Using MockServer in Place of Direct API Access

There may be a time when you do not have access to the API (either because you don't have the credentials, network access, etc...) You will probably still want to do end-to-end testing at least once. To do so, you can emulate the server you would be reaching using a server stubbing tool.

For example, using [MockServer](https://www.mock-server.com/), you can set up an expectation file like this:

```json
{
  "httpRequest": {
    "method": "GET",
    "path": "/data"
  },
  "httpResponse": {
    "body": "{\"data\": [{\"record_key\": 1}, {\"record_key\": 2}]}"
  }
}
```

Assuming this file has been created at `secrets/mock_server_config/expectations.json`, running the following command will allow matching any requests on path `/data` to return the response defined in the expectation file:

```bash
docker run -d --rm -v $(pwd)/secrets/mock_server_config:/config -p 8113:8113 --env MOCKSERVER_LOG_LEVEL=TRACE --env MOCKSERVER_SERVER_PORT=8113 --env MOCKSERVER_WATCH_INITIALIZATION_JSON=true --env MOCKSERVER_PERSISTED_EXPECTATIONS_PATH=/config/expectations.json --env MOCKSERVER_INITIALIZATION_JSON_PATH=/config/expectations.json mockserver/mockserver:5.15.0
```

HTTP requests to `localhost:8113/data` should now return the body defined in the expectations file. To test this, the implementer either has to change the code that defines the base URL for the Python source or update the `url_base` from low-code. With the Connector Builder running in docker, you will have to use domain `host.docker.internal` instead of `localhost` as the requests are executed within docker.

## Running Connector Acceptance Tests for a single connector in Docker with your local CDK installed

_Pre-requisite: Install the
[`airbyte-ci` CLI](https://github.com/airbytehq/airbyte/blob/master/airbyte-ci/connectors/pipelines/README.md)_

To run acceptance tests for a single connectors using the local CDK, from the connector directory, run:

```bash
airbyte-ci connectors --use-local-cdk --name=<CONNECTOR> test
```
