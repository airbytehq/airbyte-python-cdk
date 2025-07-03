## 👋 Greetings, Airbyte Team Member!

Here are some helpful tips and reminders for your convenience.

### Testing This CDK Version

You can test this version of the CDK using the following:

```bash
# Run the CLI from this branch:
uvx 'git+https://github.com/airbytehq/airbyte-python-cdk.git@{{ .branch_name }}#egg=airbyte-python-cdk[dev]' --help

# Update a connector to use the CDK from this branch ref:
cd airbyte-integrations/connectors/source-example
poe use-cdk-branch {{ .branch_name }}
```

### Helpful Resources

- [CDK Documentation](https://docs.airbyte.com/connector-development/cdk-python/)
- [Connector Development Guide](https://docs.airbyte.com/connector-development/)
- [Testing Connectors](https://docs.airbyte.com/connector-development/testing-connectors/)
- [CDK API Reference](https://airbytehq.github.io/airbyte-python-cdk/)

### PR Slash Commands

Airbyte Maintainers can execute the following slash commands on your PR:

- `/autofix` - Fixes most formatting and linting issues
- `/poetry-lock` - Updates poetry.lock file
- `/test` - Runs connector tests with the updated CDK
- `/poe <command>` - Runs any poe command in the CDK environment

[📝 _Edit this welcome message._](https://github.com/airbytehq/airbyte-python-cdk/blob/main/.github/pr-welcome-internal.md)
