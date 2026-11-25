from airbyte_cdk.manifest_server import app as manifest_server_app
from airbyte_cdk.manifest_server.helpers.security_events import log_egress_denied
from airbyte_cdk.utils.outbound_request_filter import OutboundRequestPolicy


def test_cloud_manifest_server_eagerly_installs_airbyte_service_policy(mocker):
    policy = OutboundRequestPolicy(blocked_hosts=frozenset({"mcp.airbyte.com"}))
    mocker.patch.object(manifest_server_app, "is_cloud_environment", return_value=True)
    policy_builder = mocker.patch.object(
        manifest_server_app,
        "cloud_outbound_request_policy",
        return_value=policy,
    )
    installer = mocker.patch.object(manifest_server_app, "init_internal_request_filter")

    manifest_server_app._configure_outbound_request_filter()

    policy_builder.assert_called_once_with(include_airbyte_service_names=True)
    installer.assert_called_once_with(policy, deny_observer=log_egress_denied)


def test_oss_manifest_server_does_not_install_cloud_request_filter(mocker):
    mocker.patch.object(manifest_server_app, "is_cloud_environment", return_value=False)
    installer = mocker.patch.object(manifest_server_app, "init_internal_request_filter")

    manifest_server_app._configure_outbound_request_filter()

    installer.assert_not_called()
