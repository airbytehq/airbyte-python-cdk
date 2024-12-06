#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from airbyte_cdk.sources.declarative.resolvers.components_resolver import ComponentsResolver, ComponentMappingDefinition, ResolvedComponentMappingDefinition
from airbyte_cdk.sources.declarative.resolvers.http_components_resolver import HttpComponentsResolver
from airbyte_cdk.sources.declarative.resolvers.config_components_resolver import ConfigComponentsResolver, StreamConfig
from airbyte_cdk.sources.declarative.models import HttpComponentsResolver as HttpComponentsResolverModel
from airbyte_cdk.sources.declarative.models import ConfigComponentsResolver as ConfigComponentsResolverModel

COMPONENTS_RESOLVER_TYPE_MAPPING = {
    "HttpComponentsResolver": HttpComponentsResolverModel,
    "ConfigComponentsResolver": ConfigComponentsResolverModel
}

__all__ = ["ComponentsResolver", "HttpComponentsResolver", "ComponentMappingDefinition", "ResolvedComponentMappingDefinition", "StreamConfig", "ConfigComponentsResolver", "COMPONENTS_RESOLVER_TYPE_MAPPING"]
