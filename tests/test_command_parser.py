import pytest
from redis._parsers import CommandsParser
from redis.commands.metadata import RequestPolicy, ResponsePolicy
from tests.helpers import get_expected_command_policies

from .conftest import (
    assert_resp_response,
    skip_if_redis_enterprise,
    skip_if_server_version_gte,
    skip_if_server_version_lt,
)


class TestCommandsParser:
    def test_init_commands(self, r):
        commands_parser = CommandsParser(r)
        assert commands_parser.commands is not None
        assert "get" in commands_parser.commands

    def test_get_keys_predetermined_key_location(self, r):
        commands_parser = CommandsParser(r)
        args1 = ["GET", "foo"]
        args2 = ["OBJECT", "encoding", "foo"]
        args3 = ["MGET", "foo", "bar", "foobar"]
        assert commands_parser.get_keys(r, *args1) == ["foo"]
        assert commands_parser.get_keys(r, *args2) == ["foo"]
        assert commands_parser.get_keys(r, *args3) == ["foo", "bar", "foobar"]

    @pytest.mark.filterwarnings("ignore:ResponseError")
    @skip_if_redis_enterprise()
    def test_get_moveable_keys(self, r):
        commands_parser = CommandsParser(r)
        args1 = [
            "EVAL",
            "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
            2,
            "key1",
            "key2",
            "first",
            "second",
        ]
        args2 = ["XREAD", "COUNT", 2, b"STREAMS", "mystream", "writers", 0, 0]
        args3 = ["ZUNIONSTORE", "out", 2, "zset1", "zset2", "WEIGHTS", 2, 3]
        args4 = ["GEORADIUS", "Sicily", 15, 37, 200, "km", "WITHCOORD", b"STORE", "out"]
        args5 = ["MEMORY USAGE", "foo"]
        args6 = [
            "MIGRATE",
            "192.168.1.34",
            6379,
            "",
            0,
            5000,
            b"KEYS",
            "key1",
            "key2",
            "key3",
        ]
        args7 = ["MIGRATE", "192.168.1.34", 6379, "key1", 0, 5000]

        assert_resp_response(
            r,
            sorted(commands_parser.get_keys(r, *args1)),
            ["key1", "key2"],
            [b"key1", b"key2"],
        )
        assert_resp_response(
            r,
            sorted(commands_parser.get_keys(r, *args2)),
            ["mystream", "writers"],
            [b"mystream", b"writers"],
        )
        assert_resp_response(
            r,
            sorted(commands_parser.get_keys(r, *args3)),
            ["out", "zset1", "zset2"],
            [b"out", b"zset1", b"zset2"],
        )
        assert_resp_response(
            r,
            sorted(commands_parser.get_keys(r, *args4)),
            ["Sicily", "out"],
            [b"Sicily", b"out"],
        )
        assert sorted(commands_parser.get_keys(r, *args5)) in [["foo"], [b"foo"]]
        assert_resp_response(
            r,
            sorted(commands_parser.get_keys(r, *args6)),
            ["key1", "key2", "key3"],
            [b"key1", b"key2", b"key3"],
        )
        assert_resp_response(
            r, sorted(commands_parser.get_keys(r, *args7)), ["key1"], [b"key1"]
        )

    # A bug in redis<7.0 causes this to fail: https://github.com/redis/redis/issues/9493
    @skip_if_server_version_lt("7.0.0")
    def test_get_eval_keys_with_0_keys(self, r):
        commands_parser = CommandsParser(r)
        args = ["EVAL", "return {ARGV[1],ARGV[2]}", 0, "key1", "key2"]
        assert commands_parser.get_keys(r, *args) == []

    def test_get_pubsub_keys(self, r):
        commands_parser = CommandsParser(r)
        args1 = ["PUBLISH", "foo", "bar"]
        args2 = ["PUBSUB NUMSUB", "foo1", "foo2", "foo3"]
        args3 = ["PUBSUB channels", "*"]
        args4 = ["SUBSCRIBE", "foo1", "foo2", "foo3"]
        assert commands_parser.get_keys(r, *args1) == ["foo"]
        assert commands_parser.get_keys(r, *args2) == ["foo1", "foo2", "foo3"]
        assert commands_parser.get_keys(r, *args3) == ["*"]
        assert commands_parser.get_keys(r, *args4) == ["foo1", "foo2", "foo3"]

    # Key specs, and every command below, first ship in 7.0.
    @skip_if_server_version_lt("7.0.0")
    def test_movablekeys_commands_derive_the_keyed_defaults(self, r):
        """
        A ``movablekeys`` command reports ``first_key_pos`` 0, so the keyed default is
        derived from its key specs. A command whose only key spec is ``not_key`` keeps
        routing by its first argument's slot, as SPUBLISH must.
        """
        metadata = CommandsParser(r).get_commands_metadata_cache()["core"]
        keyed = (RequestPolicy.DEFAULT_KEYED, ResponsePolicy.DEFAULT_KEYED)

        for name in ("zinter", "sintercard", "xread", "eval", "eval_ro", "fcall_ro"):
            policies = (metadata[name].request_policy, metadata[name].response_policy)
            assert policies == keyed, name
        assert metadata["spublish"].request_policy is RequestPolicy.DEFAULT_KEYED
        assert metadata["lastsave"].request_policy is RequestPolicy.DEFAULT_KEYLESS

    @skip_if_server_version_lt("8.0.0")
    @skip_if_server_version_gte("8.5.240")
    @pytest.mark.onlycluster
    def test_get_command_policies(self, r):
        commands_parser = CommandsParser(r)

        expected_command_policies = get_expected_command_policies()

        actual_policies = commands_parser.get_command_policies()
        assert len(actual_policies) > 0

        for module_name, commands in expected_command_policies.items():
            for command, command_policies in commands.items():
                assert command in actual_policies[module_name]
                assert command_policies == [
                    command,
                    actual_policies[module_name][command].request_policy,
                    actual_policies[module_name][command].response_policy,
                ]

    @skip_if_server_version_lt("8.5.240")
    @skip_if_server_version_gte("8.10.0")
    @pytest.mark.onlycluster
    def test_get_command_policies_json_debug_without_its_key_stays_keyless(self, r):
        """
        From 8.5.240, JSON.DEBUG reports ``first_key_pos`` 0. Before 8.10 the server does
        not report its key in the key specs either, so the keyless default is derived.
        """
        self._assert_json_debug_policies(
            r, RequestPolicy.DEFAULT_KEYLESS, ResponsePolicy.DEFAULT_KEYLESS
        )

    @skip_if_server_version_lt("8.10.0")
    @pytest.mark.onlycluster
    def test_get_command_policies_json_debug_movablekeys_stays_keyed(self, r):
        """
        From 8.10, JSON.DEBUG is ``movablekeys`` and its key is only in the key specs, as
        ``first_key_pos`` is 0. The keyed default is derived from those specs instead.
        """
        self._assert_json_debug_policies(
            r, RequestPolicy.DEFAULT_KEYED, ResponsePolicy.DEFAULT_KEYED
        )

    @staticmethod
    def _assert_json_debug_policies(r, request_policy, response_policy):
        commands_parser = CommandsParser(r)

        changes_in_defaults = {
            "json": {
                "debug": ["debug", request_policy, response_policy],
            },
        }

        expected_command_policies = get_expected_command_policies(changes_in_defaults)

        actual_policies = commands_parser.get_command_policies()
        assert len(actual_policies) > 0

        for module_name, commands in expected_command_policies.items():
            for command, command_policies in commands.items():
                assert command in actual_policies[module_name]
                assert command_policies == [
                    command,
                    actual_policies[module_name][command].request_policy,
                    actual_policies[module_name][command].response_policy,
                ]
