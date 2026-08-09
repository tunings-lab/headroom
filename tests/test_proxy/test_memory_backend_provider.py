"""Tests for the pluggable proxy-mode memory backend provider seam.

The ``headroom.memory_backend`` entry-point group lets external packages
register memory backends for proxy mode. ``MemoryHandler._init_backend_locked``
delegates to a discovered provider when ``MemoryConfig.backend`` is
``"external"`` (with ``external_backend_name``) or any value other than
the built-in names (``"local"``, ``"qdrant-neo4j"``).

These tests patch ``importlib.metadata.entry_points`` so no real
installed package is required.
"""

from __future__ import annotations

import asyncio
import inspect

import pytest

from headroom.proxy.memory_handler import (
    MEMORY_BACKEND_ENTRY_POINT_GROUP,
    MemoryConfig,
    MemoryHandler,
)


class _FakeEntryPoint:
    """Minimal stand-in for ``importlib.metadata.EntryPoint``."""

    def __init__(self, name: str, target):
        self.name = name
        self._target = target

    def load(self):
        return self._target

    def __repr__(self):
        return f"<FakeEntryPoint {self.name}>"


class _FakeBackend:
    """A fake ``MemoryBackend`` implementation for tests."""

    def __init__(self, config: MemoryConfig):
        self.config = config
        self.initialized = False

    async def ensure_initialized(self) -> None:
        self.initialized = True

    async def save_memory(self, content, user_id, importance=0.5, **kwargs):
        return None

    async def search_memories(self, query, user_id, **kwargs):
        return []

    async def close(self) -> None:
        pass


def _patch_entry_points(monkeypatch, entries: list[_FakeEntryPoint]):
    """Patch ``entry_points`` in the memory_handler module."""
    from headroom.proxy import memory_handler as mod

    def fake_entry_points(*, group: str):
        if group == MEMORY_BACKEND_ENTRY_POINT_GROUP:
            return list(entries)
        return []

    monkeypatch.setattr(mod, "entry_points", fake_entry_points)


# ---------------------------------------------------------------------------
# Delegation to external providers
# ---------------------------------------------------------------------------


class TestExternalBackendProvider:
    def test_provider_name_as_backend(self, monkeypatch):
        """A bare provider name in ``backend`` is resolved via entry points."""
        created: list[MemoryConfig] = []

        def factory(config: MemoryConfig):
            created.append(config)
            return _FakeBackend(config)

        _patch_entry_points(
            monkeypatch,
            [_FakeEntryPoint("surrealdb", factory)],
        )

        config = MemoryConfig(enabled=True, backend="surrealdb")
        handler = MemoryHandler(config)

        asyncio.run(handler._init_backend_locked())

        assert handler._initialized is True
        assert isinstance(handler._backend, _FakeBackend)
        assert handler._backend.initialized is True
        assert created and created[0] is config

    def test_external_sentinel_with_name(self, monkeypatch):
        """``backend="external"`` + ``external_backend_name`` resolves the provider."""
        created: list[MemoryConfig] = []

        def factory(config: MemoryConfig):
            created.append(config)
            return _FakeBackend(config)

        _patch_entry_points(
            monkeypatch,
            [_FakeEntryPoint("mybackend", factory)],
        )

        config = MemoryConfig(
            enabled=True, backend="external", external_backend_name="mybackend"
        )
        handler = MemoryHandler(config)

        asyncio.run(handler._init_backend_locked())

        assert handler._initialized is True
        assert isinstance(handler._backend, _FakeBackend)
        assert handler._backend.initialized is True
        assert created and created[0] is config

    def test_external_without_name_raises(self, monkeypatch):
        """``backend="external"`` with no ``external_backend_name`` raises."""
        _patch_entry_points(monkeypatch, [])

        config = MemoryConfig(enabled=True, backend="external")
        handler = MemoryHandler(config)

        with pytest.raises(ValueError, match="external_backend_name"):
            asyncio.run(handler._init_backend_locked())

    def test_unknown_provider_name_raises(self, monkeypatch):
        """An unknown provider name raises a helpful ValueError."""
        _patch_entry_points(monkeypatch, [])

        config = MemoryConfig(enabled=True, backend="nonexistent")
        handler = MemoryHandler(config)

        with pytest.raises(ValueError, match="No entry point registered"):
            asyncio.run(handler._init_backend_locked())

    def test_provider_without_async_init(self, monkeypatch):
        """A provider with no ``ensure_initialized`` is still accepted."""

        class _NoInitBackend:
            def __init__(self, config):
                self.config = config

        def factory(config):
            return _NoInitBackend(config)

        _patch_entry_points(
            monkeypatch,
            [_FakeEntryPoint("plain", factory)],
        )

        config = MemoryConfig(enabled=True, backend="plain")
        handler = MemoryHandler(config)

        asyncio.run(handler._init_backend_locked())

        assert handler._initialized is True
        assert isinstance(handler._backend, _NoInitBackend)


# ---------------------------------------------------------------------------
# Built-in backends are unaffected
# ---------------------------------------------------------------------------


class TestBuiltinsUnaffected:
    """The ``local`` and ``qdrant-neo4j`` branches must not change behavior."""

    def test_local_backend_unchanged(self, monkeypatch):
        """``backend="local"`` still routes to the local branch, not external."""
        # If external discovery were accidentally triggered, this would
        # raise ValueError because no entry point is registered.
        _patch_entry_points(monkeypatch, [])

        config = MemoryConfig(enabled=True, backend="local")
        handler = MemoryHandler(config)

        # We can't fully initialize LocalBackend in a unit test (it needs
        # ONNX / sentence-transformers), so we just verify the branch was
        # taken: the external-discovery error must NOT be raised. Instead
        # we expect either success or an import/embedder error — never a
        # "No entry point registered" ValueError.
        with pytest.raises(Exception) as exc_info:
            asyncio.run(handler._init_backend_locked())

        assert "No entry point registered" not in str(exc_info.value)
        assert "external_backend_name" not in str(exc_info.value)

    def test_qdrant_neo4j_branch_unchanged(self, monkeypatch):
        """``backend="qdrant-neo4j"`` still routes to its own branch."""
        _patch_entry_points(monkeypatch, [])

        config = MemoryConfig(enabled=True, backend="qdrant-neo4j")
        handler = MemoryHandler(config)

        # The qdrant-neo4j branch will fail on missing deps (ImportError)
        # or Mem0 init — but it must NOT produce the external-discovery
        # ValueError. This proves the branch routing is intact.
        with pytest.raises(Exception) as exc_info:
            asyncio.run(handler._init_backend_locked())

        assert "No entry point registered" not in str(exc_info.value)
        assert "external_backend_name" not in str(exc_info.value)