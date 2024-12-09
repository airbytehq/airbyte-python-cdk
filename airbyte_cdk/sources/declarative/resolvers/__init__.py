#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from airbyte_cdk.sources.declarative.resolvers.components_resolver import ComponentsResolver, ComponentMappingDefinition, ResolvedComponentMappingDefinition
from airbyte_cdk.sources.declarative.resolvers.http_components_resolver import HttpComponentsResolver
from airbyte_cdk.sources.declarative.models import HttpComponentsResolver as HttpComponentsResolverModel

COMPONENTS_RESOLVER_TYPE_MAPPING = {
    "HttpComponentsResolver": HttpComponentsResolverModel
}

__all__ = ["ComponentsResolver", "HttpComponentsResolver", "ComponentMappingDefinition", "ResolvedComponentMappingDefinition", "COMPONENTS_RESOLVER_TYPE_MAPPING"]
