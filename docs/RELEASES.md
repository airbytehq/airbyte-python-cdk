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

## Understanding and Debugging Builder and SDM Releases

### How Connector Builder uses SDM/CDK

The Connector Builder (written in Java) calls the CDK Python package directly, executing the CDK's Source Declarative Manfiest code via Python processes on the Builder container. (The Connector Builder does not directly invoke the SDM image, but there is an open project to change this in the future.)

Our publish flow sends a PR to the Builder repo (`airbyte-platform-internal`) to bump the version used in Builder. The Marketplace Contributions team (aka Connector Builder maintainers) will review and merge the PR.

### How the SDM Image is used in Platform

The platform scans DockerHub at an [every 10 minutes cadence](https://github.com/airbytehq/airbyte-platform-internal/blob/d744174c0f3ca8fa70f3e05cca6728f067219752/oss/airbyte-cron/src/main/java/io/airbyte/cron/jobs/DeclarativeSourcesUpdater.java) as of 2024-12-09. Based on that DockerHub scan, the platform bumps the default SDM version that is stored in the `declarative_manifest_image_version` table in prod.

Note: Currently we don't pre-test images in Platform so manual testing is needed.

### How to confirm what SDM version is used on the Platform

Currently there are two ways to do this.

The first option is to look in the `declarative_manifest_image_version` database table in Prod.

If that is not available as an option, you can run an Builder-created connector in Cloud and note the version number printed in the logs. Warning: this may not be indicative if that connector instance has been manually pinned to a specific version.

TODO: Would be great to find a way to inspect directly without requiring direct prod DB access. 

### How to pretest changes to SDM images manually

TODO: ...
