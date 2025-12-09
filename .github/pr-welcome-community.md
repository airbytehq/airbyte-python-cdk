## 👋 Welcome to the Airbyte Python CDK!

Thank you for your contribution from **{{ .repo_name }}**! We're excited to have you in the Airbyte community.

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

- [Contributing Guidelines](https://docs.airbyte.com/contributing-to-airbyte/)
- [CDK API Reference](https://airbytehq.github.io/airbyte-python-cdk/)

### PR Slash Commands

As needed or by request, Airbyte Maintainers can execute the following slash commands on your PR:

- `/autofix` - Fixes most formatting and linting issues
- `/poetry-lock` - Updates poetry.lock file
- `/test` - Runs connector tests with the updated CDK
- `/prerelease` - Triggers a prerelease publish with default arguments

If you have any questions, feel free to ask in the PR comments or join our [Slack community](https://airbytehq.slack.com/).
