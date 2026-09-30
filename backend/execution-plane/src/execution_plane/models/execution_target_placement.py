"""Typed platform-specific metadata for execution targets."""

from __future__ import annotations

import re
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator

_LABEL_NAME = re.compile(r"^[A-Za-z0-9]([A-Za-z0-9_.-]*[A-Za-z0-9])?$")
_DNS_SUBDOMAIN = re.compile(r"^(?:[a-z0-9]([-a-z0-9]*[a-z0-9])?)(?:\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$")
_TOLERATION_EFFECTS = {"NoSchedule", "PreferNoSchedule", "NoExecute"}
_MAX_LABEL_NAME_LENGTH = 63
_MAX_DNS_SUBDOMAIN_LENGTH = 253


class KubernetesPlacement(BaseModel):
    """Metadata required to place workers on Kubernetes."""

    type: Literal["kubernetes"] = "kubernetes"
    namespace: str = Field(
        min_length=1,
        max_length=63,
        pattern=r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$",
        description="Kubernetes namespace reserved for execution workloads",
    )
    node_selectors: list[str] = Field(default_factory=list)
    tolerations: list[str] = Field(default_factory=list)

    @field_validator("node_selectors")
    @classmethod
    def validate_node_selectors(cls, values: list[str]) -> list[str]:
        """Validate compact Kubernetes node-selector expressions."""
        for value in values:
            key, separator, selector_value = value.partition("=")
            prefix, prefix_separator, name = key.partition("/")
            if not prefix_separator:
                name = prefix
            valid_key = (
                (
                    not prefix_separator
                    or (len(prefix) <= _MAX_DNS_SUBDOMAIN_LENGTH and _DNS_SUBDOMAIN.fullmatch(prefix))
                )
                and len(name) <= _MAX_LABEL_NAME_LENGTH
                and bool(_LABEL_NAME.fullmatch(name))
            )
            if (
                not separator
                or not valid_key
                or len(selector_value) > _MAX_LABEL_NAME_LENGTH
                or not _LABEL_NAME.fullmatch(selector_value)
            ):
                msg = "must use a valid Kubernetes label selector in key=value form"
                raise ValueError(msg)
        return values

    @field_validator("tolerations")
    @classmethod
    def validate_tolerations(cls, values: list[str]) -> list[str]:
        """Validate compact Kubernetes toleration expressions."""
        for value in values:
            key_value, separator, effect = value.rpartition(":")
            key, equals, toleration_value = key_value.partition("=")
            prefix, prefix_separator, name = key.partition("/")
            if not prefix_separator:
                name = prefix
            valid_key = (
                (
                    not prefix_separator
                    or (len(prefix) <= _MAX_DNS_SUBDOMAIN_LENGTH and _DNS_SUBDOMAIN.fullmatch(prefix))
                )
                and len(name) <= _MAX_LABEL_NAME_LENGTH
                and bool(_LABEL_NAME.fullmatch(name))
            )
            valid_value = len(toleration_value) <= _MAX_LABEL_NAME_LENGTH and bool(
                _LABEL_NAME.fullmatch(toleration_value)
            )
            if not separator or not equals or not valid_key or not valid_value or effect not in _TOLERATION_EFFECTS:
                msg = "must use key=value:Effect form with a valid Kubernetes toleration effect"
                raise ValueError(msg)
        return values


class RHELPlacement(BaseModel):
    """Placeholder for RHEL-specific execution-target metadata."""

    type: Literal["rhel"] = "rhel"


ExecutionTargetPlacementTypes = KubernetesPlacement | RHELPlacement
ExecutionTargetPlacement = Annotated[
    ExecutionTargetPlacementTypes,
    Field(discriminator="type"),
]
