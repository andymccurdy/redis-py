from binascii import crc_hqx

import pytest
from redis.crc import REDIS_CLUSTER_HASH_SLOTS, key_slot


@pytest.mark.fixed_client
class TestKeySlot:
    """Unit tests for the client-side cluster key-slot calculation.

    ``key_slot`` is CRC16 (XMODEM) over the key, modulo the number of hash
    slots, with Redis Cluster hash-tag handling: if the key contains a
    ``{...}`` with at least one character inside, only that substring is
    hashed. These behaviors are exercised indirectly by the cluster tests
    but were not covered on their own, and need no server to verify.
    """

    def test_known_crc16_value(self):
        # 0x31C3 is the standard CRC16/XMODEM check value for "123456789".
        assert crc_hqx(b"123456789", 0) == 0x31C3
        assert key_slot(b"123456789") == 0x31C3 % REDIS_CLUSTER_HASH_SLOTS

    def test_plain_key_hashes_whole_key(self):
        assert key_slot(b"foo") == crc_hqx(b"foo", 0) % REDIS_CLUSTER_HASH_SLOTS

    def test_slot_within_range(self):
        for key in (b"", b"foo", b"123456789", b"{user1000}.following"):
            assert 0 <= key_slot(key) < REDIS_CLUSTER_HASH_SLOTS

    def test_hash_tag_maps_to_same_slot(self):
        # Keys sharing a hash tag must land in the same slot so they can be
        # used together in multi-key commands.
        base = key_slot(b"{user1000}")
        assert key_slot(b"{user1000}.following") == base
        assert key_slot(b"{user1000}.followers") == base
        assert key_slot(b"foo{user1000}bar") == base

    def test_only_first_hash_tag_is_used(self):
        # Only the substring between the first "{" and the first "}" after
        # it is hashed; later braces are ignored.
        assert key_slot(b"{a}{b}") == key_slot(b"a")
        assert key_slot(b"{a}{b}") != key_slot(b"b")

    def test_nested_opening_brace_is_part_of_tag(self):
        # "{{bar}}zap": first "{" at 0, first "}" at 5, so the tag is "{bar".
        assert key_slot(b"{{bar}}zap") == key_slot(b"{bar")

    def test_empty_hash_tag_falls_back_to_whole_key(self):
        # "{}" is not a valid tag (nothing between the braces), so the entire
        # key is hashed instead.
        for key in (b"{}foo", b"foo{}", b"foo{}{bar}"):
            assert key_slot(key) == crc_hqx(key, 0) % REDIS_CLUSTER_HASH_SLOTS

    def test_unclosed_brace_falls_back_to_whole_key(self):
        assert key_slot(b"{foo") == crc_hqx(b"{foo", 0) % REDIS_CLUSTER_HASH_SLOTS

    def test_custom_bucket_is_honored(self):
        assert key_slot(b"foo", 100) == crc_hqx(b"foo", 0) % 100
        assert 0 <= key_slot(b"123456789", 100) < 100

    def test_memoryview_key_matches_bytes(self):
        assert key_slot(memoryview(b"foo")) == key_slot(b"foo")
        assert key_slot(memoryview(b"{user1000}.x")) == key_slot(b"{user1000}")
