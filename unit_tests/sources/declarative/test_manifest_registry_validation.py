"""
Unit tests for validating manifest.yaml files from the connector registry against the CDK schema.

This test suite fetches all manifest-only connectors from the Airbyte connector registry,
downloads their manifest.yaml files from public endpoints, and validates them against
the current declarative component schema defined in the CDK.
"""

import json
import logging
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Tuple
from unittest.mock import patch

import pytest
import requests
import yaml

from airbyte_cdk.sources.declarative.parsers.manifest_component_transformer import (
    ManifestComponentTransformer,
)
from airbyte_cdk.sources.declarative.parsers.manifest_reference_resolver import (
    ManifestReferenceResolver,
)
from airbyte_cdk.sources.declarative.validators.validate_adheres_to_schema import (
    ValidateAdheresToSchema,
)

logger = logging.getLogger(__name__)

# List of connectors to exclude from validation.
EXCLUDED_CONNECTORS: List[Tuple[str, str]] = [
    ("source-100ms", "6.44.0"),
    ("source-7shifts", "4.6.2"),
    ("source-activecampaign", "0.78.5"),
    ("source-adobe-commerce-magento", "6.48.15"),
    ("source-agilecrm", "6.4.0"),
    ("source-airbyte", "6.44.0"),
    ("source-airtable", "6.51.0"),
    ("source-amazon-ads", "6.45.10"),
    ("source-amazon-seller-partner", "6.44.0"),
    ("source-amazon-sqs", "6.44.0"),
    ("source-apify-dataset", "6.44.0"),
    ("source-appfollow", "6.44.0"),
    ("source-appsflyer", "6.44.0"),
    ("source-asana", "6.44.0"),
    ("source-ashby", "6.44.0"),
    ("source-aws-cloudtrail", "6.44.0"),
    ("source-azure-blob-storage", "6.44.0"),
    ("source-azure-table", "6.44.0"),
    ("source-bamboo-hr", "6.44.0"),
    ("source-baton", "6.44.0"),
    ("source-bigcommerce", "6.44.0"),
    ("source-bigquery", "6.44.0"),
    ("source-bing-ads", "6.44.0"),
    ("source-braintree", "6.44.0"),
    ("source-braze", "6.44.0"),
    ("source-breezometer", "6.44.0"),
    ("source-buildkite", "6.44.0"),
    ("source-callrail", "6.44.0"),
    ("source-chargebee", "6.44.0"),
    ("source-chartmogul", "6.44.0"),
    ("source-chargify", "6.44.0"),
    ("source-clickhouse", "6.44.0"),
    ("source-clickup-api", "6.44.0"),
    ("source-close-com", "6.44.0"),
    ("source-coda", "6.44.0"),
    ("source-coin-api", "6.44.0"),
    ("source-coinmarketcap", "6.44.0"),
    ("source-commercetools", "6.44.0"),
    ("source-convex", "6.44.0"),
    ("source-convertkit", "6.44.0"),
    ("source-courier", "6.44.0"),
    ("source-customerio", "6.44.0"),
    ("source-datadog", "6.44.0"),
    ("source-datascope", "6.44.0"),
    ("source-delighted", "6.44.0"),
    ("source-dixa", "6.44.0"),
    ("source-dockerhub", "6.44.0"),
    ("source-drift", "6.44.0"),
    ("source-duckdb", "6.44.0"),
    ("source-e2e-test", "6.44.0"),
    ("source-emailoctopus", "6.44.0"),
    ("source-everhour", "6.44.0"),
    ("source-facebook-marketing", "6.44.0"),
    ("source-facebook-pages", "6.44.0"),
    ("source-faker", "6.44.0"),
    ("source-fastbill", "6.44.0"),
    ("source-fauna", "6.44.0"),
    ("source-file", "6.44.0"),
    ("source-firebolt", "6.44.0"),
    ("source-flexport", "6.44.0"),
    ("source-freshcaller", "6.44.0"),
    ("source-freshdesk", "6.44.0"),
    ("source-freshsales", "6.44.0"),
    ("source-freshservice", "6.44.0"),
    ("source-freshworks-crm", "6.44.0"),
    ("source-gainsight-px", "6.44.0"),
    ("source-gcs", "6.44.0"),
    ("source-getlago", "6.44.0"),
    ("source-github", "6.44.0"),
    ("source-gitlab", "6.44.0"),
    ("source-glassfrog", "6.44.0"),
    ("source-gocardless", "6.44.0"),
    ("source-google-ads", "6.44.0"),
    ("source-google-analytics-data-api", "6.44.0"),
    ("source-google-analytics-v4", "6.44.0"),
    ("source-google-directory", "6.44.0"),
    ("source-google-drive", "6.44.0"),
    ("source-google-pagespeed-insights", "6.44.0"),
    ("source-google-search-console", "6.44.0"),
    ("source-google-sheets", "6.44.0"),
    ("source-google-workspace-admin-reports", "6.44.0"),
    ("source-greenhouse", "6.44.0"),
    ("source-gridly", "6.44.0"),
    ("source-harvest", "6.44.0"),
    ("source-hellobaton", "6.44.0"),
    ("source-helpscout", "6.44.0"),
    ("source-hubspot", "6.44.0"),
    ("source-hubplanner", "6.44.0"),
    ("source-insightly", "6.44.0"),
    ("source-instagram", "6.44.0"),
    ("source-instatus", "6.44.0"),
    ("source-intercom", "6.44.0"),
    ("source-ip2whois", "6.44.0"),
    ("source-iterable", "6.44.0"),
    ("source-jira", "6.44.0"),
    ("source-k6-cloud", "6.44.0"),
    ("source-klaviyo", "6.44.0"),
    ("source-kustomer-singer", "6.44.0"),
    ("source-kyve", "6.44.0"),
    ("source-launchdarkly", "6.44.0"),
    ("source-lemlist", "6.44.0"),
    ("source-lever-hiring", "6.44.0"),
    ("source-linkedin-ads", "6.44.0"),
    ("source-linkedin-pages", "6.44.0"),
    ("source-lokalise", "6.44.0"),
    ("source-looker", "6.44.0"),
    ("source-mailchimp", "6.44.0"),
    ("source-mailgun", "6.44.0"),
    ("source-mailjet-mail", "6.44.0"),
    ("source-mailjet-sms", "6.44.0"),
    ("source-marketo", "6.44.0"),
    ("source-metabase", "6.44.0"),
    ("source-microsoft-teams", "6.44.0"),
    ("source-mixpanel", "6.44.0"),
    ("source-monday", "6.44.0"),
    ("source-mux", "6.44.0"),
    ("source-my-hours", "6.44.0"),
    ("source-mysql", "6.44.0"),
    ("source-n8n", "6.44.0"),
    ("source-netsuite", "6.44.0"),
    ("source-news-api", "6.44.0"),
    ("source-newsdata", "6.44.0"),
    ("source-notion", "6.44.0"),
    ("source-nytimes", "6.44.0"),
    ("source-okta", "6.44.0"),
    ("source-omnisend", "6.44.0"),
    ("source-one-signal", "6.44.0"),
    ("source-openweather", "6.44.0"),
    ("source-orbit", "6.44.0"),
    ("source-outreach", "6.44.0"),
    ("source-pardot", "6.44.0"),
    ("source-partnerstack", "6.44.0"),
    ("source-paypal-transaction", "6.44.0"),
    ("source-paystack", "6.44.0"),
    ("source-pinterest", "6.44.0"),
    ("source-pipedrive", "6.44.0"),
    ("source-posthog", "6.44.0"),
    ("source-postgres", "6.44.0"),
    ("source-postmarkapp", "6.44.0"),
    ("source-prestashop", "6.44.0"),
    ("source-public-apis", "6.44.0"),
    ("source-punk-api", "6.44.0"),
    ("source-pypi", "6.44.0"),
    ("source-qualaroo", "6.44.0"),
    ("source-quickbooks", "6.44.0"),
    ("source-railz", "6.44.0"),
    ("source-rd-station-marketing", "6.44.0"),
    ("source-recreation", "6.44.0"),
    ("source-recurly", "6.44.0"),
    ("source-redshift", "6.44.0"),
    ("source-retently", "6.44.0"),
    ("source-rki-covid", "6.44.0"),
    ("source-s3", "6.44.0"),
    ("source-salesforce", "6.44.0"),
    ("source-salesloft", "6.44.0"),
    ("source-secoda", "6.44.0"),
    ("source-sendgrid", "6.44.0"),
    ("source-sendinblue", "6.44.0"),
    ("source-sentry", "6.44.0"),
    ("source-sftp", "6.44.0"),
    ("source-sftp-bulk", "6.44.0"),
    ("source-shopify", "6.44.0"),
    ("source-shortio", "6.44.0"),
    ("source-slack", "6.44.0"),
    ("source-smartengage", "6.44.0"),
    ("source-smaily", "6.44.0"),
    ("source-snapchat-marketing", "6.44.0"),
    ("source-snowflake", "6.44.0"),
    ("source-sonar-cloud", "6.44.0"),
    ("source-spacex-api", "6.44.0"),
    ("source-square", "6.44.0"),
    ("source-strava", "6.44.0"),
    ("source-stripe", "6.44.0"),
    ("source-surveymonkey", "6.44.0"),
    ("source-surveysparrow", "6.44.0"),
    ("source-talkdesk-explore", "6.44.0"),
    ("source-tempo", "6.44.0"),
    ("source-the-guardian-api", "6.44.0"),
    ("source-ticketmaster", "6.44.0"),
    ("source-tiktok-marketing", "6.44.0"),
    ("source-timely", "6.44.0"),
    ("source-toggl", "6.44.0"),
    ("source-trello", "6.44.0"),
    ("source-trustpilot", "6.44.0"),
    ("source-tvmaze-schedule", "6.44.0"),
    ("source-twilio", "6.44.0"),
    ("source-twilio-taskrouter", "6.44.0"),
    ("source-twitter", "6.44.0"),
    ("source-typeform", "6.44.0"),
    ("source-us-census", "6.44.0"),
    ("source-vantage", "6.44.0"),
    ("source-visma-economic", "6.44.0"),
    ("source-waiteraid", "6.44.0"),
    ("source-weatherstack", "6.44.0"),
    ("source-webflow", "6.44.0"),
    ("source-whisky-hunter", "6.44.0"),
    ("source-woocommerce", "6.44.0"),
    ("source-workable", "6.44.0"),
    ("source-workramp", "6.44.0"),
    ("source-xero", "6.44.0"),
    ("source-yandex-metrica", "6.44.0"),
    ("source-youtube-analytics", "6.44.0"),
    ("source-zendesk-chat", "6.44.0"),
    ("source-zendesk-sell", "6.44.0"),
    ("source-zendesk-sunshine", "6.44.0"),
    ("source-zendesk-support", "6.44.0"),
    ("source-zendesk-talk", "6.44.0"),
    ("source-zenloop", "6.44.0"),
    ("source-zoho-crm", "6.44.0"),
    ("source-zoom", "6.44.0"),
    ("source-zuora", "6.44.0"),
    ("source-ahrefs", "4.6.2"),
    ("source-aircall", "4.5.4"),
    ("source-akeneo", "5.16.0"),
    ("source-alpha-vantage", "4.6.2"),
    ("source-appcues", "4.6.2"),
    ("source-appstore-singer", "4.6.2"),
    ("source-auth0", "4.6.2"),
    ("source-aws-cloudtrail", "4.6.2"),
    ("source-babelforce", "4.6.2"),
    ("source-bigcommerce", "4.6.2"),
    ("source-bing-ads", "4.6.2"),
    ("source-braintree", "4.6.2"),
    ("source-cart", "4.6.2"),
    ("source-chargebee", "4.6.2"),
    ("source-chartmogul", "4.6.2"),
    ("source-chargify", "4.6.2"),
    ("source-clickup-api", "4.6.2"),
    ("source-close-com", "4.6.2"),
    ("source-cockroachdb", "4.6.2"),
    ("source-coin-api", "4.6.2"),
    ("source-coinmarketcap", "4.6.2"),
    ("source-commercetools", "4.6.2"),
    ("source-convertkit", "4.6.2"),
    ("source-customerio", "4.6.2"),
    ("source-datadog", "4.6.2"),
    ("source-datascope", "4.6.2"),
    ("source-delighted", "4.6.2"),
    ("source-dixa", "4.6.2"),
    ("source-dockerhub", "4.6.2"),
    ("source-drift", "4.6.2"),
    ("source-emailoctopus", "4.6.2"),
    ("source-everhour", "4.6.2"),
    ("source-facebook-marketing", "4.6.2"),
    ("source-facebook-pages", "4.6.2"),
    ("source-fastbill", "4.6.2"),
    ("source-fauna", "4.6.2"),
    ("source-firebolt", "4.6.2"),
    ("source-flexport", "4.6.2"),
    ("source-freshcaller", "4.6.2"),
    ("source-freshdesk", "4.6.2"),
    ("source-freshsales", "4.6.2"),
    ("source-freshservice", "4.6.2"),
    ("source-freshworks-crm", "4.6.2"),
    ("source-gainsight-px", "4.6.2"),
    ("source-getlago", "4.6.2"),
    ("source-github", "4.6.2"),
    ("source-gitlab", "4.6.2"),
    ("source-glassfrog", "4.6.2"),
    ("source-gocardless", "4.6.2"),
    ("source-google-ads", "4.6.2"),
    ("source-google-analytics-data-api", "4.6.2"),
    ("source-google-analytics-v4", "4.6.2"),
    ("source-google-directory", "4.6.2"),
    ("source-google-drive", "4.6.2"),
    ("source-google-pagespeed-insights", "4.6.2"),
    ("source-google-search-console", "4.6.2"),
    ("source-google-sheets", "4.6.2"),
    ("source-google-workspace-admin-reports", "4.6.2"),
    ("source-greenhouse", "4.6.2"),
    ("source-gridly", "4.6.2"),
    ("source-harvest", "4.6.2"),
    ("source-hellobaton", "4.6.2"),
    ("source-helpscout", "4.6.2"),
    ("source-hubspot", "4.6.2"),
    ("source-hubplanner", "4.6.2"),
    ("source-insightly", "4.6.2"),
    ("source-instagram", "4.6.2"),
    ("source-instatus", "4.6.2"),
    ("source-intercom", "4.6.2"),
    ("source-ip2whois", "4.6.2"),
    ("source-iterable", "4.6.2"),
    ("source-jira", "4.6.2"),
    ("source-k6-cloud", "4.6.2"),
    ("source-klaviyo", "4.6.2"),
    ("source-kustomer-singer", "4.6.2"),
    ("source-kyve", "4.6.2"),
    ("source-launchdarkly", "4.6.2"),
    ("source-lemlist", "4.6.2"),
    ("source-lever-hiring", "4.6.2"),
    ("source-linkedin-ads", "4.6.2"),
    ("source-linkedin-pages", "4.6.2"),
    ("source-lokalise", "4.6.2"),
    ("source-looker", "4.6.2"),
    ("source-mailchimp", "4.6.2"),
    ("source-mailgun", "4.6.2"),
    ("source-mailjet-mail", "4.6.2"),
    ("source-mailjet-sms", "4.6.2"),
    ("source-marketo", "4.6.2"),
    ("source-metabase", "4.6.2"),
    ("source-microsoft-teams", "4.6.2"),
    ("source-mixpanel", "4.6.2"),
    ("source-monday", "4.6.2"),
    ("source-mux", "4.6.2"),
    ("source-my-hours", "4.6.2"),
    ("source-mysql", "4.6.2"),
    ("source-n8n", "4.6.2"),
    ("source-netsuite", "4.6.2"),
    ("source-news-api", "4.6.2"),
    ("source-newsdata", "4.6.2"),
    ("source-notion", "4.6.2"),
    ("source-nytimes", "4.6.2"),
    ("source-okta", "4.6.2"),
    ("source-omnisend", "4.6.2"),
    ("source-one-signal", "4.6.2"),
    ("source-openweather", "4.6.2"),
    ("source-orbit", "4.6.2"),
    ("source-outreach", "4.6.2"),
    ("source-pardot", "4.6.2"),
    ("source-partnerstack", "4.6.2"),
    ("source-paypal-transaction", "4.6.2"),
    ("source-paystack", "4.6.2"),
    ("source-pinterest", "4.6.2"),
    ("source-pipedrive", "4.6.2"),
    ("source-posthog", "4.6.2"),
    ("source-postgres", "4.6.2"),
    ("source-postmarkapp", "4.6.2"),
    ("source-prestashop", "4.6.2"),
    ("source-public-apis", "4.6.2"),
    ("source-punk-api", "4.6.2"),
    ("source-pypi", "4.6.2"),
    ("source-qualaroo", "4.6.2"),
    ("source-quickbooks", "4.6.2"),
    ("source-railz", "4.6.2"),
    ("source-rd-station-marketing", "4.6.2"),
    ("source-recreation", "4.6.2"),
    ("source-recurly", "4.6.2"),
    ("source-redshift", "4.6.2"),
    ("source-retently", "4.6.2"),
    ("source-rki-covid", "4.6.2"),
    ("source-s3", "4.6.2"),
    ("source-salesforce", "4.6.2"),
    ("source-salesloft", "4.6.2"),
    ("source-secoda", "4.6.2"),
    ("source-sendgrid", "4.6.2"),
    ("source-sendinblue", "4.6.2"),
    ("source-sentry", "4.6.2"),
    ("source-sftp", "4.6.2"),
    ("source-sftp-bulk", "4.6.2"),
    ("source-shopify", "4.6.2"),
    ("source-shortio", "4.6.2"),
    ("source-slack", "4.6.2"),
    ("source-smartengage", "4.6.2"),
    ("source-smaily", "4.6.2"),
    ("source-snapchat-marketing", "4.6.2"),
    ("source-snowflake", "4.6.2"),
    ("source-sonar-cloud", "4.6.2"),
    ("source-spacex-api", "4.6.2"),
    ("source-square", "4.6.2"),
    ("source-strava", "4.6.2"),
    ("source-stripe", "4.6.2"),
    ("source-surveymonkey", "4.6.2"),
    ("source-surveysparrow", "4.6.2"),
    ("source-talkdesk-explore", "4.6.2"),
    ("source-tempo", "4.6.2"),
    ("source-the-guardian-api", "4.6.2"),
    ("source-ticketmaster", "4.6.2"),
    ("source-tiktok-marketing", "4.6.2"),
    ("source-timely", "4.6.2"),
    ("source-toggl", "4.6.2"),
    ("source-trello", "4.6.2"),
    ("source-trustpilot", "4.6.2"),
    ("source-tvmaze-schedule", "4.6.2"),
    ("source-twilio", "4.6.2"),
    ("source-twilio-taskrouter", "4.6.2"),
    ("source-twitter", "4.6.2"),
    ("source-typeform", "4.6.2"),
    ("source-us-census", "4.6.2"),
    ("source-vantage", "4.6.2"),
    ("source-visma-economic", "4.6.2"),
    ("source-waiteraid", "4.6.2"),
    ("source-weatherstack", "4.6.2"),
    ("source-webflow", "4.6.2"),
    ("source-whisky-hunter", "4.6.2"),
    ("source-woocommerce", "4.6.2"),
    ("source-workable", "4.6.2"),
    ("source-workramp", "4.6.2"),
    ("source-xero", "4.6.2"),
    ("source-yandex-metrica", "4.6.2"),
    ("source-youtube-analytics", "4.6.2"),
    ("source-zendesk-chat", "4.6.2"),
    ("source-zendesk-sell", "4.6.2"),
    ("source-zendesk-sunshine", "4.6.2"),
    ("source-zendesk-support", "4.6.2"),
    ("source-zendesk-talk", "4.6.2"),
    ("source-zenloop", "4.6.2"),
    ("source-zoho-crm", "4.6.2"),
    ("source-zoom", "4.6.2"),
    ("source-zuora", "4.6.2"),
    ("source-zoho-invoice", "6.1.0"),
    ("source-zonka-feedback", "5.17.0"),
]

RECHECK_EXCLUSION_LIST = False

USE_GIT_SPARSE_CHECKOUT = False

CONNECTOR_REGISTRY_URL = "https://connectors.airbyte.com/files/registries/v0/oss_registry.json"
MANIFEST_URL_TEMPLATE = (
    "https://connectors.airbyte.com/files/metadata/airbyte/{connector_name}/latest/manifest.yaml"
)


@pytest.fixture(scope="session")
def validation_successes() -> List[Tuple[str, str]]:
    """Thread-safe list for tracking validation successes."""
    return []


@pytest.fixture(scope="session")
def validation_failures() -> List[Tuple[str, str, str]]:
    """Thread-safe list for tracking validation failures."""
    return []


@pytest.fixture(scope="session")
def download_failures() -> List[Tuple[str, str]]:
    """Thread-safe list for tracking download failures."""
    return []


@pytest.fixture(scope="session")
def schema_validator() -> ValidateAdheresToSchema:
    """Cached schema validator to avoid repeated loading."""
    schema = load_declarative_component_schema()
    return ValidateAdheresToSchema(schema=schema)


@pytest.fixture(scope="session")
def manifest_connector_names() -> List[str]:
    """Cached list of manifest-only connector names to avoid repeated registry calls."""
    if USE_GIT_SPARSE_CHECKOUT:
        # Use git sparse-checkout to get all available manifest connectors
        try:
            manifests = download_manifests_via_git()
            return list(manifests.keys())
        except Exception as e:
            logger.warning(f"Git sparse-checkout failed, falling back to registry: {e}")
            connectors = get_manifest_only_connectors()
            return [connector_name for connector_name, _ in connectors]
    else:
        connectors = get_manifest_only_connectors()
        return [connector_name for connector_name, _ in connectors]


def load_declarative_component_schema() -> Dict[str, Any]:
    """Load the declarative component schema from the CDK."""
    schema_path = (
        Path(__file__).resolve().parent.parent.parent.parent
        / "airbyte_cdk/sources/declarative/declarative_component_schema.yaml"
    )
    with open(schema_path, "r") as file:
        schema = yaml.safe_load(file)
        if not isinstance(schema, dict):
            raise ValueError("Schema must be a dictionary")
        return schema


def get_manifest_only_connectors() -> List[Tuple[str, str]]:
    """
    Fetch manifest-only connectors from the registry.

    Returns:
        List of tuples (connector_name, cdk_version) where cdk_version will be
        determined from the manifest.yaml file itself.
    """
    try:
        response = requests.get(CONNECTOR_REGISTRY_URL, timeout=30)
        response.raise_for_status()
        registry = response.json()

        manifest_connectors: List[Tuple[str, str]] = []
        for source in registry.get("sources", []):
            if source.get("language") == "manifest-only":
                connector_name = source.get("dockerRepository", "").replace("airbyte/", "")
                if connector_name:
                    manifest_connectors.append((connector_name, "unknown"))

        return manifest_connectors
    except Exception as e:
        pytest.fail(f"Failed to fetch connector registry: {e}")


# Global cache for git-downloaded manifests
_git_manifest_cache: Dict[str, Tuple[str, str]] = {}


def download_manifest(
    connector_name: str, download_failures: List[Tuple[str, str]]
) -> Tuple[str, str]:
    """
    Download manifest.yaml for a connector.

    Returns:
        Tuple of (manifest_content, cdk_version) where cdk_version is extracted
        from the manifest's version field.
    """
    global _git_manifest_cache

    if USE_GIT_SPARSE_CHECKOUT and not _git_manifest_cache:
        try:
            logger.info("Initializing git sparse-checkout cache...")
            _git_manifest_cache = download_manifests_via_git()
            logger.info(f"Cached {len(_git_manifest_cache)} manifests from git")
        except Exception as e:
            logger.warning(f"Git sparse-checkout failed, using HTTP fallback: {e}")

    if connector_name in _git_manifest_cache:
        return _git_manifest_cache[connector_name]

    url = MANIFEST_URL_TEMPLATE.format(connector_name=connector_name)
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        manifest_content = response.text

        manifest_dict = yaml.safe_load(manifest_content)
        cdk_version = manifest_dict.get("version", "unknown")

        return manifest_content, cdk_version
    except Exception as e:
        download_failures.append((connector_name, str(e)))
        raise


def download_manifests_via_git() -> Dict[str, Tuple[str, str]]:
    """
    Download all manifest files using git sparse-checkout for better performance.

    Returns:
        Dict mapping connector_name to (manifest_content, cdk_version)
    """
    manifests: Dict[str, Tuple[str, str]] = {}

    with tempfile.TemporaryDirectory() as temp_dir:
        repo_path = Path(temp_dir) / "airbyte"

        try:
            logger.info("Cloning airbyte repo with sparse-checkout...")
            subprocess.run(
                [
                    "git",
                    "clone",
                    "--filter=blob:none",
                    "--sparse",
                    "--depth=1",
                    "https://github.com/airbytehq/airbyte.git",
                    str(repo_path),
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=120,
            )

            logger.info("Setting sparse-checkout pattern...")
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(repo_path),
                    "sparse-checkout",
                    "set",
                    "airbyte-integrations/connectors/*/manifest.yaml",
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=30,
            )

            logger.info("Processing manifest files...")
            manifest_files = list(repo_path.glob("airbyte-integrations/connectors/*/manifest.yaml"))
            logger.info(f"Found {len(manifest_files)} manifest files")

            for i, manifest_path in enumerate(manifest_files):
                connector_name = manifest_path.parent.name
                if i % 50 == 0:
                    logger.info(
                        f"Processing manifest {i + 1}/{len(manifest_files)}: {connector_name}"
                    )
                try:
                    with open(manifest_path, "r") as f:
                        manifest_content = f.read()

                    manifest_dict = yaml.safe_load(manifest_content)
                    cdk_version = manifest_dict.get("version", "unknown")
                    manifests[connector_name] = (manifest_content, cdk_version)
                except Exception as e:
                    logger.warning(f"Failed to process manifest for {connector_name}: {e}")

        except subprocess.TimeoutExpired:
            logger.error("Git sparse-checkout timed out. Falling back to HTTP downloads.")
            return {}
        except subprocess.CalledProcessError as e:
            logger.warning(f"Git sparse-checkout failed: {e}. Falling back to HTTP downloads.")
            return {}
        except Exception as e:
            logger.error(
                f"Unexpected error in git sparse-checkout: {e}. Falling back to HTTP downloads."
            )
            return {}

    logger.info(f"Successfully cached {len(manifests)} manifests from git")
    return manifests


def get_manifest_only_connector_names() -> List[str]:
    """
    Get all manifest-only connector names from the registry.

    Returns:
        List of connector names (e.g., "source-hubspot")
    """
    connectors = get_manifest_only_connectors()
    return [connector_name for connector_name, _ in connectors]


@pytest.mark.parametrize("connector_name", get_manifest_only_connector_names())
def test_manifest_validates_against_schema(
    connector_name: str,
    schema_validator: ValidateAdheresToSchema,
    validation_successes: List[Tuple[str, str]],
    validation_failures: List[Tuple[str, str, str]],
    download_failures: List[Tuple[str, str]],
) -> None:
    """
    Test that manifest.yaml files from the registry validate against the CDK schema.

    Args:
        connector_name: Name of the connector (e.g., "source-hubspot")
    """
    # Download manifest first to get CDK version
    try:
        manifest_content, cdk_version = download_manifest(connector_name, download_failures)
    except Exception as e:
        pytest.fail(f"Failed to download manifest for {connector_name}: {e}")

    is_excluded = (connector_name, cdk_version) in EXCLUDED_CONNECTORS

    if RECHECK_EXCLUSION_LIST:
        expected_to_fail = is_excluded
    else:
        # Normal mode: skip excluded connectors
        if is_excluded:
            pytest.skip(
                f"Skipping {connector_name} - connector declares it is compatible with "
                f"CDK version {cdk_version} but is known to fail validation"
            )

    try:
        manifest_dict = yaml.safe_load(manifest_content)
    except yaml.YAMLError as e:
        error_msg = f"Invalid YAML in manifest for {connector_name}: {e}"
        validation_failures.append((connector_name, cdk_version, error_msg))
        pytest.fail(error_msg)

    try:
        if "type" not in manifest_dict:
            manifest_dict["type"] = "DeclarativeSource"

        # Resolve references in the manifest
        resolved_manifest = ManifestReferenceResolver().preprocess_manifest(manifest_dict)

        # Propagate types and parameters throughout the manifest
        preprocessed_manifest = ManifestComponentTransformer().propagate_types_and_parameters(
            "", resolved_manifest, {}
        )

        schema_validator.validate(preprocessed_manifest)
        validation_successes.append((connector_name, cdk_version))
        logger.info(f"✓ {connector_name} (CDK {cdk_version}) - validation passed")

        if RECHECK_EXCLUSION_LIST and expected_to_fail:
            pytest.fail(
                f"EXCLUSION LIST ERROR: {connector_name} (CDK {cdk_version}) was expected to fail "
                f"but passed validation. Remove from EXCLUDED_CONNECTORS."
            )

    except ValueError as e:
        error_msg = (
            f"Manifest validation failed for {connector_name} "
            f"(connector declares it is compatible with CDK version {cdk_version}): {e}"
        )
        validation_failures.append((connector_name, cdk_version, str(e)))
        logger.error(f"✗ {connector_name} (CDK {cdk_version}) - validation failed: {e}")

        if RECHECK_EXCLUSION_LIST and not expected_to_fail:
            pytest.fail(
                f"EXCLUSION LIST ERROR: {connector_name} (CDK {cdk_version}) was expected to pass "
                f"but failed validation. Add to EXCLUDED_CONNECTORS: {error_msg}"
            )
        elif not RECHECK_EXCLUSION_LIST:
            pytest.fail(error_msg)


def test_schema_loads_successfully() -> None:
    """Test that the declarative component schema loads without errors."""
    schema = load_declarative_component_schema()
    assert isinstance(schema, dict)
    assert "type" in schema
    assert schema["type"] == "object"


def test_connector_registry_accessible() -> None:
    """Test that the connector registry is accessible."""
    response = requests.get(CONNECTOR_REGISTRY_URL, timeout=30)
    assert response.status_code == 200
    registry = response.json()
    assert "sources" in registry
    assert isinstance(registry["sources"], list)


def test_manifest_only_connectors_found() -> None:
    """Test that we can find manifest-only connectors in the registry."""
    connectors = get_manifest_only_connectors()
    assert len(connectors) > 0, "No manifest-only connectors found in registry"

    for connector_name, _ in connectors:
        assert isinstance(connector_name, str)
        assert len(connector_name) > 0
        assert connector_name.startswith("source-") or connector_name.startswith("destination-")


def test_sample_manifest_download(download_failures: List[Tuple[str, str]]) -> None:
    """Test that we can download a sample manifest file."""
    connectors = get_manifest_only_connectors()
    if not connectors:
        pytest.skip("No manifest-only connectors available for testing")

    connector_name, _ = connectors[0]
    try:
        manifest_content, cdk_version = download_manifest(connector_name, download_failures)
    except Exception as e:
        pytest.skip(f"Could not download sample manifest from {connector_name}: {e}")

    assert isinstance(manifest_content, str)
    assert len(manifest_content) > 0
    assert isinstance(cdk_version, str)
    assert len(cdk_version) > 0

    manifest_dict = yaml.safe_load(manifest_content)
    assert isinstance(manifest_dict, dict)
    assert "version" in manifest_dict
    assert manifest_dict["version"] == cdk_version


def log_test_results(
    validation_successes: List[Tuple[str, str]],
    validation_failures: List[Tuple[str, str, str]],
    download_failures: List[Tuple[str, str]],
) -> None:
    """Log comprehensive test results for analysis."""
    print("\n" + "=" * 80)
    print("MANIFEST VALIDATION TEST RESULTS SUMMARY")
    print("=" * 80)

    print(f"\n✓ SUCCESSFUL VALIDATIONS ({len(validation_successes)}):")
    for connector_name, cdk_version in validation_successes:
        print(f"  - {connector_name} (CDK {cdk_version})")

    print(f"\n✗ VALIDATION FAILURES ({len(validation_failures)}):")
    for connector_name, cdk_version, error in validation_failures:
        print(f"  - {connector_name} (CDK {cdk_version}): {error}")

    print(f"\n⚠ DOWNLOAD FAILURES ({len(download_failures)}):")
    for connector_name, error in download_failures:
        print(f"  - {connector_name}: {error}")

    print("\n" + "=" * 80)
    print(
        f"TOTAL: {len(validation_successes)} passed, {len(validation_failures)} failed, {len(download_failures)} download errors"
    )
    print("=" * 80)


def pytest_sessionfinish(session: Any, exitstatus: Any) -> None:
    """Called after whole test run finished, right before returning the exit status to the system."""
    validation_successes = getattr(session, "_validation_successes", [])
    validation_failures = getattr(session, "_validation_failures", [])
    download_failures = getattr(session, "_download_failures", [])
    log_test_results(validation_successes, validation_failures, download_failures)
