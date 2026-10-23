"""插件注册表：登记、路由与缺能力时的错误码。"""

from __future__ import annotations

import pytest

from core.plugins.errors import PluginCapabilityError, PluginNotFoundError
from core.plugins.registry import PluginCapabilities, PluginRegistry
from plugins.fake.plugin import (
    FakeEnvironmentProvider,
    FakeScriptAdapter,
    FakeVerifier,
)


def make_registry() -> PluginRegistry:
    registry = PluginRegistry()
    registry.register(
        PluginCapabilities(
            plugin_id="fake",
            environment=FakeEnvironmentProvider(),
            script=FakeScriptAdapter(),
            verifier=FakeVerifier(),
        )
    )
    return registry


def test_register_and_get() -> None:
    registry = make_registry()
    assert registry.get("fake").declared() == ("environment", "script", "verifier")
    assert registry.plugin_ids() == ("fake",)
    assert isinstance(registry.environment("fake"), FakeEnvironmentProvider)
    assert isinstance(registry.script("fake"), FakeScriptAdapter)
    assert isinstance(registry.verifier("fake"), FakeVerifier)


def test_duplicate_registration_is_rejected() -> None:
    registry = make_registry()
    with pytest.raises(PluginCapabilityError):
        registry.register(PluginCapabilities(plugin_id="fake"))


def test_missing_plugin_is_reported() -> None:
    registry = PluginRegistry()
    with pytest.raises(PluginNotFoundError):
        registry.environment("missing")


def test_missing_capability_is_reported() -> None:
    registry = PluginRegistry()
    registry.register(
        PluginCapabilities(plugin_id="env-only", environment=FakeEnvironmentProvider())
    )
    with pytest.raises(PluginCapabilityError):
        registry.script("env-only")
    with pytest.raises(PluginCapabilityError):
        registry.verifier("env-only")


def test_declared_lists_only_present_capabilities() -> None:
    capabilities = PluginCapabilities(plugin_id="x", verifier=FakeVerifier())
    assert capabilities.declared() == ("verifier",)
