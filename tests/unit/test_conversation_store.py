import json

import pytest

from self_healthy_kafka.storage.conversation import (
    ConversationStoreError,
    RedisConversationStore,
)


class FakeRedis:
    def __init__(self):
        self.now = 0
        self.values = {}
        self.closed = False
        self.fail = False

    def get(self, key):
        if self.fail:
            raise OSError("unavailable")
        value = self.values.get(key)
        if value and value[0] <= self.now:
            self.values.pop(key)
            return None
        return value[1] if value else None

    def setex(self, key, ttl, value):
        if self.fail:
            raise OSError("unavailable")
        self.values[key] = (self.now + ttl, value)

    def delete(self, key):
        if self.fail:
            raise OSError("unavailable")
        self.values.pop(key, None)

    def close(self):
        self.closed = True


def test_redis_conversation_store_round_trips_with_namespace_ttl_and_delete():
    client = FakeRedis()
    store = RedisConversationStore(
        "redis://unused", ttl_seconds=30, client=client, key_prefix="chat:test"
    )

    store.set("conversation-1", {"version": 1, "connector": "orders"})

    assert set(client.values) == {"chat:test:conversation-1"}
    assert store.get("conversation-1") == {"version": 1, "connector": "orders"}
    client.now = 31
    assert store.get("conversation-1") is None
    store.set("conversation-1", {"version": 1})
    store.delete("conversation-1")
    assert store.get("conversation-1") is None
    store.close()
    assert client.closed is True


def test_redis_conversation_store_rejects_malformed_or_oversized_state():
    client = FakeRedis()
    store = RedisConversationStore("redis://unused", ttl_seconds=30, client=client)
    client.values["self-healthy-kafka:conversation:bad"] = (30, b"not-json")
    with pytest.raises(ConversationStoreError, match="invalid"):
        store.get("bad")
    with pytest.raises(ConversationStoreError, match="too large"):
        store.set("large", {"value": "x" * (64 * 1024)})
    client.values["self-healthy-kafka:conversation:list"] = (30, json.dumps([]).encode())
    with pytest.raises(ConversationStoreError, match="invalid"):
        store.get("list")


def test_redis_conversation_store_classifies_dependency_failure():
    client = FakeRedis()
    client.fail = True
    store = RedisConversationStore("redis://unused", ttl_seconds=30, client=client)
    with pytest.raises(ConversationStoreError, match="unavailable"):
        store.get("conversation-1")
    with pytest.raises(ConversationStoreError, match="unavailable"):
        store.set("conversation-1", {"version": 1})
    with pytest.raises(ConversationStoreError, match="unavailable"):
        store.delete("conversation-1")
