# Airbyte Python CDK - Contributing Guide

Learn how you can become a contributor to the Airbyte Python CDK.

Thank you for being interested in contributing to Airbyte Python CDK! Here are some guidelines to get you started:

- We adhere to the Airbyte [code of conduct](https://docs.airbyte.com/community/code-of-conduct).
- You can contribute by reporting bugs, posting github discussions, opening issues, improving docs, and submitting pull requests with bugfixes and new features alike.
- If you're changing the code, please add unit tests for your change.
- When submitting issues or PRs, please add a small reproduction project. Using the changes in your connector and providing that connector code as an example (or a satellite PR) helps!

## First Time Setup

Here are some tips to get started using the project dependencies and development tools:

1. Clone the CDK repo. If you will be testing connectors, you should clone the CDK into the same parent directory as `airbytehq/airbyte`, which contains the connector definitions.
1. Make sure [Poetry is installed](https://python-poetry.org/docs/#).
1. Run `poetry install --all-extras`.
1. Unit tests can be run via `poetry run pytest`.
1. You can use "Poe" tasks to perform common actions such as lint checks (`poetry run poe lint`), autoformatting (`poetry run poe format-fix`), etc. For a list of tasks you can run, try `poetry run poe list`.

Note that installing all extras is required to run the full suite of unit tests.

## Working with Poe Tasks

The Airbyte CDK uses [Poe the Poet](https://poethepoet.natn.io/) to define common development task. You can run `poetry run poe list` to see all available tasks. This will work after `poetry install --all-extras` without any additional installations.

Optionally, if you can [pre-install Poe](https://poethepoet.natn.io/installation.html) with `pipx install poethepoet` and then you will be able to run Poe tasks with the shorter `poe TASKNAME` syntax instead of `poetry run poe TASKNAME`.

## Running tests locally

- Iterate on the CDK code locally.
- Run tests via `poetry run poe pytest`, or `python -m pytest -s unit_tests` if you want to pass pytest options.
- Run `poetry run pytest-fast` to run the subset of PyTest tests which are not flagged as `slow`. (Should take <5 min for fast tests only.)
- Run `poetry run poe check-local` to lint all code, type-check modified code, and run unit tests with coverage in one command.

To see all available scripts, run `poetry run poe`.

## Formatting Code

- Iterate on the CDK code locally.
- Run `poetry run poe format-fix` to auto-fix formatting issues.
- If you only want to format Python code (excluding markdown, yaml, etc.), you can use `poetry run ruff format` to autoformat your Python code.

To see all available `ruff` options, run `poetry run ruff`.

The Ruff configuration is stored in `ruff.toml` at the root of the repository. This file contains settings for line length, target Python version, and linting rules.

## Auto-Generating the Declarative Schema File

Low-code CDK models are generated from `sources/declarative/declarative_component_schema.yaml`. If the iteration you are working on includes changes to the models or the connector generator, you may need to regenerate them. In order to do that, you can run:

```bash
poetry run poe build
```

This will generate the code generator docker image and the component manifest files based on the schemas and templates.

## Generating API Reference Docs

Documentation auto-gen code lives in the `/docs` folder. Based on the doc strings of public methods, we generate API documentation using [pdoc](https://pdoc.dev).

To generate the documentation, run `poe docs-generate` or to build and open the docs preview in one step, run `poe docs-preview`.

The `docs-generate` Poe task is mapped to the `run()` function of `docs/generate.py`. Documentation pages will be generated in the `docs/generated` folder (ignored by git). You can also download auto-generated API docs for any GitHub push by navigating to the "Summary" tab of the docs generation check in GitHub Actions.

## Release Management

Please see the [Release Management](./RELEASES.md) guide for information on how to perform releases and pre-releases.

## FAQ

### Q: Who are "maintainers"?

For the purpose of this documentation, "maintainers" are those who have write permissions (or higher) on the repo. Generally these are Airbyte team members.

### Q: Where should connectors put integration tests?

Only tests within the `unit_tests` directory will be run by `airbyte-ci`. If you have integration tests that should also run, the common convention is to place these in the `unit_tests/integration` directory. This ensures they will be run automatically in CI and before each new release.

### Q: What GitHub slash commands are available and who can run them?

Only Airbyte CDK maintainers can run slash commands. The most common slash commands are as follows:

- `/autofix`- Corrects any linting or formatting issues and commits the change back to the repo.
- `/poetry-lock` - Re-locks dependencies and updates the `poetry.lock` file, then commits the changes back to the repo. This is helpful after merging in updates from main, or when creating a PR in the browser - such as for version bumps or dependency updates directly in the PR.

The full list of available slash commands can be found in the [slash command dispatch file](https://github.com/airbytehq/airbyte-python-cdk/blob/main/.github/workflows/slash_command_dispatch.yml#L21-L25).

# Appendix: Advanced Topics

## Using MockServer in Place of Direct API Access

There may be a time when you do not have access to the API (either because you don't have the credentials, network access, etc...) You will probably still want to do end-to-end testing at least once. In order to do so, you can emulate the server you would be reaching using a server stubbing tool.

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

Assuming this file has been created at `secrets/mock_server_config/expectations.json`, running the following command will allow to match any requests on path `/data` to return the response defined in the expectation file:

```bash
docker run -d --rm -v $(pwd)/secrets/mock_server_config:/config -p 8113:8113 --env MOCKSERVER_LOG_LEVEL=TRACE --env MOCKSERVER_SERVER_PORT=8113 --env MOCKSERVER_WATCH_INITIALIZATION_JSON=true --env MOCKSERVER_PERSISTED_EXPECTATIONS_PATH=/config/expectations.json --env MOCKSERVER_INITIALIZATION_JSON_PATH=/config/expectations.json mockserver/mockserver:5.15.0
```

HTTP requests to `localhost:8113/data` should now return the body defined in the expectations file. To test this, the implementer either has to change the code which defines the base URL for Python source or update the `url_base` from low-code. With the Connector Builder running in docker, you will have to use domain `host.docker.internal` instead of `localhost` as the requests are executed within docker.

## Testing Connectors against local CDK Changes

When developing a new feature in the CDK, you will sometimes find it necessary to run a connector that uses that new feature, or to use an existing connector to validate some new feature or fix in the CDK.

### Option 1: Installing your local CDK into a local Python connector

Open the connector's `pyproject.toml` file and replace the line with `airbyte_cdk` with the following:

```toml
airbyte_cdk = { path = "../../../../airbyte-python-cdk", develop = true }
```

Then, running `poetry update` should reinstall `airbyte_cdk` from your local working directory. When testing is complete and you've published the CDK update, remember to revert your change and bump to the latest CDK version before re-publishing the connector.

### Option 2: Build and Test Connectors Using `airbyte-ci --use-local-cdk`

_Pre-requisite: Install the [`airbyte-ci` CLI](https://github.com/airbytehq/airbyte/blob/master/airbyte-ci/connectors/pipelines/README.md)_

You can build your connector image with the local CDK using

```bash
# from the airbytehq/airbyte base directory
airbyte-ci connectors --use-local-cdk --name=<CONNECTOR> build
```

Or use the `test` command with `--use-local-cdk` to run the full set of connector tests, including connector acceptance tests (CAT) and the connector's own unit tests:

```bash
# from the airbytehq/airbyte base directory
airbyte-ci connectors --use-local-cdk --name=<CONNECTOR> build
```

Note that the local CDK is injected at build time, so if you make changes, you will have to run the build command again to see them reflected.

#### Running Connector Acceptance Tests for a single connector in Docker with your local CDK installed

_Pre-requisite: Install the
[`airbyte-ci` CLI](https://github.com/airbytehq/airbyte/blob/master/airbyte-ci/connectors/pipelines/README.md)_

To run acceptance tests for a single connectors using the local CDK, from the connector directory, run:

```bash
airbyte-ci connectors --use-local-cdk --name=<CONNECTOR> test
```
