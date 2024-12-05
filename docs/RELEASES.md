# Airbyte Python CDK - Release Management Guide

## Publishing stable releases of the CDK

A few seconds after any PR is merged to `main` , a release draft will be created or updated on the releases page here: https://github.com/airbytehq/airbyte-python-cdk/releases. Here are the steps to publish a CDK release:

1. Click “Edit” next to the release.
2. Optionally change the version if you want a minor or major release version. When changing the version, you should modify both the tag name and the release title so the two match. The format for release tags is `vX.Y.Z` and GitHub will prevent you from creating the tag if you forget the “v” prefix.
3. Optionally tweak the text in the release notes - for instance to call out contributors, to make a specific change more intuitive for readers to understand, or to move updates into a different category than they were assigned by default. (Note: You can also do this retroactively after publishing the release.)
4. Publish the release by pressing the “Publish release” button.

*Note:*

- *Only maintainers can see release drafts. Non-maintainers will only see published releases.*
- If you create a tag on accident that you need to remove, contact a maintainer to delete the tag and the release.
- You can monitor the PyPi release process here in the GitHub Actions view: https://github.com/airbytehq/airbyte-python-cdk/actions/workflows/pypi_publish.yml

- **_[▶️ Loom Walkthrough](https://www.loom.com/share/ceddbbfc625141e382fd41c4f609dc51?sid=78e13ef7-16c8-478a-af47-4978b3ff3fad)_**

## Publishing Pre-Release Versions of the CDK

Publishing a pre-release version is similar to publishing a stable version. However, instead of using the auto-generated release draft, you’ll create a new release draft.

1. Navigate to the releases page: https://github.com/airbytehq/airbyte-python-cdk/releases
2. Click “Draft a new release”.
3. In the tag selector, type the version number of the prerelease you’d like to create and copy-past the same into the Release name box.
    - The release should be like `vX.Y.Zsuffix` where `suffix` is something like `dev0`, `dev1` , `alpha0`, `beta1`, etc.

## Publishing new versions of SDM (source-declarative-manifest)

Prereqs:

1. The SDM publish process assumes you have already published the CDK. If you have not already done so, you’ll want to first publish the CDK using the steps above. While this prereq is not technically *required*, it is highly recommended.

Publish steps:

1. Navigate to the GitHub action page here: https://github.com/airbytehq/airbyte-python-cdk/actions/workflows/publish_sdm_connector.yml
2. Click “Run workflow” to start the process of invoking a new manual workflow.
3. Click the drop-down for “Run workflow from” and then select the “tags” tab to browse already-created tags. Select the tag of the published CDK version you want to use for the SDM publish process. Notes:
    1. Optionally you can type part of the version number to filter down the list.
    2. You can ignore the version prompt box (aka leave blank) when publishing from a release tag. The version will be detected from the git tag.
    3. You can optionally click the box for “Dry run” if you want to observe the process before running the real thing. The dry run option will perform all steps *except* for the DockerHub publish step.
4. Without changing any other options, you can click “Run workflow” to run the workflow.
5. Watch the GitHub Action run. If successful, you should see it publish to DockerHub and a URL will appear on the “Summary” view once it has completed.

- **_[▶️ Loom Walkthrough](https://www.loom.com/share/bc8ddffba9384fcfacaf535608360ee1)_**
