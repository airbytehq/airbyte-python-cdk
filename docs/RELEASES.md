# Airbyte Python CDK - Release Management Guide

## Publishing stable releases of the CDK and SDM

A few seconds after any PR is merged to `main` , a release draft will be created or updated on the releases page here: https://github.com/airbytehq/airbyte-python-cdk/releases. Here are the steps to publish a CDK release:

1. Click “Edit” next to the release.
2. Optionally change the version if you want a minor or major release version. When changing the version, you should modify both the tag name and the release title so the two match. The format for release tags is `vX.Y.Z` and GitHub will prevent you from creating the tag if you forget the “v” prefix.
3. Optionally tweak the text in the release notes - for instance to call out contributors, to make a specific change more intuitive for readers to understand, or to move updates into a different category than they were assigned by default. (Note: You can also do this retroactively after publishing the release.)
4. Publish the release by pressing the “Publish release” button.

*Note:*

- *Only maintainers can see release drafts. Non-maintainers will only see published releases.*
- If you create a tag on accident that you need to remove, contact a maintainer to delete the tag and the release.
- You can monitor the PyPI release process here in the GitHub Actions view: https://github.com/airbytehq/airbyte-python-cdk/actions/workflows/pypi_publish.yml

- **_[▶️ Loom Walkthrough](https://www.loom.com/share/ceddbbfc625141e382fd41c4f609dc51?sid=78e13ef7-16c8-478a-af47-4978b3ff3fad)_**

## Publishing Pre-Release Versions of the CDK and/or SDM (Internal)

This process is slightly different from the above, since we don't necessarily want public release notes to be published for internal testing releases. The same underlying workflow will be run, but we'll kick it off directly:

1. Navigate to the "Packaging and Publishing" workflow in GitHub Actions.
2. Type the version number - including a valid pre-release suffix. Examples: `1.2.3dev0`, `1.2.3rc1`, `1.2.3b0`, etc.
3. Select `main` or your dev branch from the "Use workflow from" dropdown.
4. Select your options and click "Run workflow".
5. Monitor the workflow to ensure the process has succeeded.
