import binascii
import datetime
import re
import threading
import time
import warnings
from asyncio import CancelledError
from collections import ChainMap, UserDict
from string import ascii_letters
from types import MappingProxyType
from unittest import mock
from unittest.mock import patch

import pytest
from redis import DataError, RedisClusterException, ResponseError
import redis
from redis import exceptions
from redis._parsers.helpers import (
    _RedisCallbacks,
    get_response_callbacks,
    parse_info,
    parse_zscan,
    zset_score_for_rank,
    zset_score_pairs,
)
from redis.client import EMPTY_RESPONSE, NEVER_DECODE
from redis.commands.core import (
    HAS_XXHASH,
    ArrayAggregateOperations,
    ArrayPredicateCombinator,
    ArrayPredicateType,
    DataPersistOptions,
    HotkeysMetricsTypes,
    SetDataPersistOptions,
)
from redis.commands.json.path import Path
from redis.commands.search.field import TextField
from redis.commands.search.query import Query
from redis.utils import safe_str
from tests.test_utils import redis_server_time

from .conftest import (
    _get_client,
    assert_resp_response,
    assert_resp_response_in,
    expects_resp2_shape,
    expects_resp3_shape,
    expects_unified_shape,
    expected_response_shape,
    skip_if_redis_enterprise,
    skip_if_server_version_gte,
    skip_if_server_version_lt,
    skip_unless_arch_bits,
)


@pytest.fixture()
def slowlog(request, r):
    current_config = r.config_get()
    old_slower_than_value = current_config["slowlog-log-slower-than"]
    old_max_legnth_value = current_config["slowlog-max-len"]

    def cleanup():
        r.config_set("slowlog-log-slower-than", old_slower_than_value)
        r.config_set("slowlog-max-len", old_max_legnth_value)

    request.addfinalizer(cleanup)

    r.config_set("slowlog-log-slower-than", 0)
    r.config_set("slowlog-max-len", 128)


def get_stream_message(client, stream, message_id):
    "Fetch a stream message and format it as a (message_id, fields) pair"
    response = client.xrange(stream, min=message_id, max=message_id)
    assert len(response) == 1
    return response[0]


@pytest.mark.fixed_client
def test_server_deprecated_commands_do_not_emit_python_warnings():
    client = redis.Redis()
    with patch.object(client, "execute_command", return_value=True) as execute_command:
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            assert client.setex("a", 60, "1")
            assert client.hmset("a", {"field": "value"})

    execute_command.assert_any_call("SETEX", "a", 60, "1")
    execute_command.assert_any_call("HMSET", "a", "field", "value")


@pytest.mark.fixed_client
@pytest.mark.parametrize(
    "call,expected",
    [
        (
            lambda r: r.lmpop("1", "a", direction="LEFT", count=None),
            ("LMPOP", "1", "a", "LEFT"),
        ),
        (
            lambda r: r.blmpop(0, "1", "a", direction="LEFT", count=None),
            ("BLMPOP", 0, "1", "a", "LEFT"),
        ),
        (
            lambda r: r.zmpop("1", ["a"], min=True, count=None),
            ("ZMPOP", "1", "a", "MIN"),
        ),
        (
            lambda r: r.bzmpop(0, "1", ["a"], min=True, count=None),
            ("BZMPOP", 0, "1", "a", "MIN"),
        ),
    ],
)
def test_mpop_commands_omit_count_when_none(call, expected):
    client = redis.Redis()
    with patch.object(client, "execute_command") as execute_command:
        call(client)

    execute_command.assert_called_once_with(*expected)


# RESPONSE CALLBACKS
@pytest.mark.onlynoncluster
class TestResponseCallbacks:
    "Tests for the response callback system"

    def test_response_callbacks(self, r):
        kwargs = r.connection_pool.connection_kwargs
        callbacks = get_response_callbacks(
            kwargs.get("protocol"), kwargs.get("legacy_responses", True)
        )
        assert r.response_callbacks == callbacks
        assert id(r.response_callbacks) != id(_RedisCallbacks)
        r.set_response_callback("GET", lambda x: "static")
        r["a"] = "foo"
        assert r["a"] == "static"

    def test_case_insensitive_command_names(self, r):
        assert r.response_callbacks["ping"] == r.response_callbacks["PING"]


class TestRedisCommands:
    def _wait_for_bgsave(self, r, timeout=10):
        deadline = time.monotonic() + timeout
        while True:
            info = r.info("persistence")
            if int(info.get("rdb_bgsave_in_progress", 0)) == 0:
                return
            if time.monotonic() > deadline:
                pytest.fail("Timed out waiting for BGSAVE to finish")
            time.sleep(0.05)

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_auth(self, r, request):
        # sending an AUTH command before setting a user/password on the
        # server should return an AuthenticationError
        with pytest.raises(exceptions.AuthenticationError):
            r.auth("some_password")

        with pytest.raises(exceptions.AuthenticationError):
            r.auth("some_password", "some_user")

        # first, test for default user (`username` is supposed to be optional)
        default_username = "default"
        temp_pass = "temp_pass"
        r.config_set("requirepass", temp_pass)

        assert r.auth(temp_pass, default_username) is True
        assert r.auth(temp_pass) is True

        # test for other users
        username = "redis-py-auth"

        def teardown():
            try:
                # this is needed because after an AuthenticationError the connection
                # is closed, and if we send an AUTH command a new connection is
                # created, but in this case we'd get an "Authentication required"
                # error when switching to the db 9 because we're not authenticated yet
                # setting the password on the connection itself triggers the
                # authentication in the connection's `on_connect` method
                r.connection.password = temp_pass
            except AttributeError:
                # connection field is not set in Redis Cluster, but that's ok
                # because the problem discussed above does not apply to Redis Cluster
                pass
            r.auth(temp_pass)
            r.config_set("requirepass", "")
            r.acl_deluser(username)

        request.addfinalizer(teardown)

        assert r.acl_setuser(
            username, enabled=True, passwords=["+strong_password"], commands=["+acl"]
        )

        assert r.auth(username=username, password="strong_password") is True

        with pytest.raises(exceptions.AuthenticationError):
            r.auth(username=username, password="wrong_password")

    def test_command_on_invalid_key_type(self, r):
        r.lpush("a", "1")
        with pytest.raises(redis.ResponseError):
            r["a"]

    # SERVER INFORMATION
    @skip_if_server_version_lt("6.0.0")
    def test_acl_cat_no_category(self, r):
        categories = r.acl_cat()
        assert isinstance(categories, list)
        assert "read" in categories or b"read" in categories

    @pytest.mark.redismod
    @skip_if_server_version_lt("7.9.0")
    def test_acl_cat_contain_modules_no_category(self, r):
        modules_list = [
            "search",
            "bloom",
            "json",
            "cuckoo",
            "timeseries",
            "cms",
            "topk",
            "tdigest",
        ]
        categories = r.acl_cat()
        assert isinstance(categories, list)
        for module_cat in modules_list:
            assert module_cat in categories or module_cat.encode() in categories

    @skip_if_server_version_lt("6.0.0")
    def test_acl_cat_with_category(self, r):
        commands = r.acl_cat("read")
        assert isinstance(commands, list)
        assert "get" in commands or b"get" in commands

    @pytest.mark.redismod
    @skip_if_server_version_lt("7.9.0")
    def test_acl_modules_cat_with_category(self, r):
        search_commands = r.acl_cat("search")
        assert isinstance(search_commands, list)
        assert "FT.SEARCH" in search_commands or b"FT.SEARCH" in search_commands

        bloom_commands = r.acl_cat("bloom")
        assert isinstance(bloom_commands, list)
        assert "bf.add" in bloom_commands or b"bf.add" in bloom_commands

        json_commands = r.acl_cat("json")
        assert isinstance(json_commands, list)
        assert "json.get" in json_commands or b"json.get" in json_commands

        cuckoo_commands = r.acl_cat("cuckoo")
        assert isinstance(cuckoo_commands, list)
        assert "cf.insert" in cuckoo_commands or b"cf.insert" in cuckoo_commands

        cms_commands = r.acl_cat("cms")
        assert isinstance(cms_commands, list)
        assert "cms.query" in cms_commands or b"cms.query" in cms_commands

        topk_commands = r.acl_cat("topk")
        assert isinstance(topk_commands, list)
        assert "topk.list" in topk_commands or b"topk.list" in topk_commands

        tdigest_commands = r.acl_cat("tdigest")
        assert isinstance(tdigest_commands, list)
        assert "tdigest.rank" in tdigest_commands or b"tdigest.rank" in tdigest_commands

        timeseries_commands = r.acl_cat("timeseries")
        assert isinstance(timeseries_commands, list)
        assert "ts.range" in timeseries_commands or b"ts.range" in timeseries_commands

    @skip_if_server_version_lt("7.0.0")
    @skip_if_redis_enterprise()
    def test_acl_dryrun(self, r, request):
        username = "redis-py-user"

        def teardown():
            r.acl_deluser(username)

        request.addfinalizer(teardown)

        r.acl_setuser(username, keys=["*"], commands=["+set"])
        assert r.acl_dryrun(username, "set", "key", "value") == b"OK"
        no_permissions_message = b"user has no permissions to run the"
        assert no_permissions_message in r.acl_dryrun(username, "get", "key")

    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    def test_acl_deluser(self, r, request):
        username = "redis-py-user"

        def teardown():
            r.acl_deluser(username)

        request.addfinalizer(teardown)

        assert r.acl_deluser(username) == 0
        assert r.acl_setuser(username, enabled=False, reset=True)
        assert r.acl_deluser(username) == 1

        # now, a group of users
        users = [f"bogususer_{r}" for r in range(0, 5)]
        for u in users:
            r.acl_setuser(u, enabled=False, reset=True)
        assert r.acl_deluser(*users) > 1
        assert r.acl_getuser(users[0]) is None
        assert r.acl_getuser(users[1]) is None
        assert r.acl_getuser(users[2]) is None
        assert r.acl_getuser(users[3]) is None
        assert r.acl_getuser(users[4]) is None

    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    def test_acl_genpass(self, r):
        password = r.acl_genpass()
        assert isinstance(password, (str, bytes))

        with pytest.raises(exceptions.DataError):
            r.acl_genpass("value")
            r.acl_genpass(-5)
            r.acl_genpass(5555)

        password = r.acl_genpass(555)
        assert isinstance(password, (str, bytes))
        assert len(password) == 139

    @skip_if_server_version_lt("7.0.0")
    @skip_if_redis_enterprise()
    def test_acl_getuser_setuser(self, r, request):
        r.flushall()
        username = "redis-py-user"

        def teardown():
            r.acl_deluser(username)

        request.addfinalizer(teardown)

        # test enabled=False
        assert r.acl_setuser(username, enabled=False, reset=True)
        acl = r.acl_getuser(username)
        assert acl["categories"] == ["-@all"]
        assert acl["commands"] == []
        assert acl["keys"] == []
        assert acl["passwords"] == []
        assert "off" in acl["flags"]
        assert acl["enabled"] is False

        # test nopass=True
        assert r.acl_setuser(username, enabled=True, reset=True, nopass=True)
        acl = r.acl_getuser(username)
        assert acl["categories"] == ["-@all"]
        assert acl["commands"] == []
        assert acl["keys"] == []
        assert acl["passwords"] == []
        assert "on" in acl["flags"]
        assert "nopass" in acl["flags"]
        assert acl["enabled"] is True

        # test all args
        assert r.acl_setuser(
            username,
            enabled=True,
            reset=True,
            passwords=["+pass1", "+pass2"],
            categories=["+set", "+@hash", "-@geo"],
            commands=["+get", "+mget", "-hset"],
            keys=["cache:*", "objects:*"],
        )
        acl = r.acl_getuser(username)
        assert set(acl["categories"]) == {"+@hash", "+@set", "-@all", "-@geo"}
        assert set(acl["commands"]) == {"+get", "+mget", "-hset"}
        assert acl["enabled"] is True
        assert "on" in acl["flags"]
        assert set(acl["keys"]) == {"~cache:*", "~objects:*"}
        assert len(acl["passwords"]) == 2

        # # test reset=False keeps existing ACL and applies new ACL on top
        assert r.acl_setuser(
            username,
            enabled=True,
            reset=True,
            passwords=["+pass1"],
            categories=["+@set"],
            commands=["+get"],
            keys=["cache:*"],
        )
        assert r.acl_setuser(
            username,
            enabled=True,
            passwords=["+pass2"],
            categories=["+@hash"],
            commands=["+mget"],
            keys=["objects:*"],
        )
        acl = r.acl_getuser(username)
        assert set(acl["commands"]) == {"+get", "+mget"}
        assert acl["enabled"] is True
        assert "on" in acl["flags"]
        assert set(acl["keys"]) == {"~cache:*", "~objects:*"}
        assert len(acl["passwords"]) == 2

        # # test removal of passwords
        assert r.acl_setuser(
            username, enabled=True, reset=True, passwords=["+pass1", "+pass2"]
        )
        assert len(r.acl_getuser(username)["passwords"]) == 2
        assert r.acl_setuser(username, enabled=True, passwords=["-pass2"])
        assert len(r.acl_getuser(username)["passwords"]) == 1

        # # Resets and tests that hashed passwords are set properly.
        hashed_password = (
            "5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8"
        )
        assert r.acl_setuser(
            username, enabled=True, reset=True, hashed_passwords=["+" + hashed_password]
        )
        acl = r.acl_getuser(username)
        assert acl["passwords"] == [hashed_password]

        # test removal of hashed passwords
        assert r.acl_setuser(
            username,
            enabled=True,
            reset=True,
            hashed_passwords=["+" + hashed_password],
            passwords=["+pass1"],
        )
        assert len(r.acl_getuser(username)["passwords"]) == 2
        assert r.acl_setuser(
            username, enabled=True, hashed_passwords=["-" + hashed_password]
        )
        assert len(r.acl_getuser(username)["passwords"]) == 1

        # # test selectors
        assert r.acl_setuser(
            username,
            enabled=True,
            reset=True,
            passwords=["+pass1", "+pass2"],
            categories=["+set", "+@hash", "-geo"],
            commands=["+get", "+mget", "-hset"],
            keys=["cache:*", "objects:*"],
            channels=["message:*"],
            selectors=[("+set", "%W~app*")],
        )
        acl = r.acl_getuser(username)
        assert set(acl["categories"]) == {"+@hash", "+@set", "-@all", "-@geo"}
        assert set(acl["commands"]) == {"+get", "+mget", "-hset"}
        assert acl["enabled"] is True
        assert "on" in acl["flags"]
        assert set(acl["keys"]) == {"~cache:*", "~objects:*"}
        assert len(acl["passwords"]) == 2
        assert set(acl["channels"]) == {"&message:*"}
        assert_resp_response(
            r,
            acl["selectors"],
            [["commands", "-@all +set", "keys", "%W~app*", "channels", ""]],
            [{"commands": "-@all +set", "keys": "%W~app*", "channels": ""}],
        )

    @skip_if_server_version_lt("6.0.0")
    def test_acl_help(self, r):
        res = r.acl_help()
        assert isinstance(res, list)
        assert len(res) != 0

    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    def test_acl_list(self, r, request):
        username = "redis-py-user"
        start = r.acl_list()

        def teardown():
            r.acl_deluser(username)

        request.addfinalizer(teardown)

        assert r.acl_setuser(username, enabled=False, reset=True)
        users = r.acl_list()
        assert len(users) == len(start) + 1

    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    @pytest.mark.onlynoncluster
    def test_acl_log(self, r, request):
        username = "redis-py-user"

        def teardown():
            r.acl_deluser(username)

        request.addfinalizer(teardown)
        r.acl_setuser(
            username,
            enabled=True,
            reset=True,
            commands=["+get", "+set", "+select"],
            keys=["cache:*"],
            nopass=True,
        )
        r.acl_log_reset()

        user_client = _get_client(
            redis.Redis, request, flushdb=False, username=username
        )

        # Valid operation and key
        assert user_client.set("cache:0", 1)
        assert user_client.get("cache:0") == b"1"

        # Invalid key
        with pytest.raises(exceptions.NoPermissionError):
            user_client.get("violated_cache:0")

        # Invalid operation
        with pytest.raises(exceptions.NoPermissionError):
            user_client.hset("cache:0", "hkey", "hval")

        assert isinstance(r.acl_log(), list)
        assert len(r.acl_log()) == 3
        assert len(r.acl_log(count=1)) == 1
        assert isinstance(r.acl_log()[0], dict)
        expected = r.acl_log(count=1)[0]
        assert_resp_response_in(
            r,
            "client-info",
            expected,
            expected.keys(),
        )

    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    def test_acl_setuser_categories_without_prefix_fails(self, r, request):
        username = "redis-py-user"

        def teardown():
            r.acl_deluser(username)

        request.addfinalizer(teardown)

        with pytest.raises(exceptions.DataError):
            r.acl_setuser(username, categories=["list"])

    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    def test_acl_setuser_commands_without_prefix_fails(self, r, request):
        username = "redis-py-user"

        def teardown():
            r.acl_deluser(username)

        request.addfinalizer(teardown)

        with pytest.raises(exceptions.DataError):
            r.acl_setuser(username, commands=["get"])

    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    def test_acl_setuser_add_passwords_and_nopass_fails(self, r, request):
        username = "redis-py-user"

        def teardown():
            r.acl_deluser(username)

        request.addfinalizer(teardown)

        with pytest.raises(exceptions.DataError):
            r.acl_setuser(username, passwords="+mypass", nopass=True)

    @skip_if_server_version_lt("6.0.0")
    def test_acl_users(self, r):
        users = r.acl_users()
        assert isinstance(users, list)
        assert len(users) > 0

    @skip_if_server_version_lt("6.0.0")
    def test_acl_whoami(self, r):
        username = r.acl_whoami()
        assert isinstance(username, (str, bytes))

    @pytest.mark.redismod
    @skip_if_server_version_lt("7.9.0")
    # Redis Enterprise manages ACLs itself and disallows ACL SETUSER here.
    @skip_if_redis_enterprise()
    def test_acl_modules_commands(self, r, request):
        default_username = "default"
        username = "redis-py-user"
        password = "pass-for-test-user"

        def teardown():
            r.auth("", default_username)
            r.acl_deluser(username)

        request.addfinalizer(teardown)

        r.ft().create_index((TextField("txt"),))
        r.hset("doc1", mapping={"txt": "foo baz"})
        r.hset("doc2", mapping={"txt": "foo bar"})

        r.acl_setuser(
            username,
            enabled=True,
            reset=True,
            passwords=[f"+{password}"],
            categories=["-all"],
            commands=[
                "+FT.SEARCH",
                "-FT.DROPINDEX",
                "+json.set",
                "+json.get",
                "-json.clear",
                "+bf.reserve",
                "-bf.info",
                "+cf.reserve",
                "+cms.initbydim",
                "+topk.reserve",
                "+tdigest.create",
                "+ts.create",
                "-ts.info",
            ],
            keys=["*"],
        )
        r.auth(password, username)

        assert r.ft().search(Query("foo ~bar"))
        with pytest.raises(exceptions.NoPermissionError):
            r.ft().dropindex()

        r.json().set("foo", Path.root_path(), "bar")
        assert r.json().get("foo") == "bar"
        with pytest.raises(exceptions.NoPermissionError):
            r.json().clear("foo")

        assert r.bf().create("bloom", 0.01, 1000)
        assert r.cf().create("cuckoo", 1000)
        assert r.cms().initbydim("cmsDim", 100, 5)
        assert r.topk().reserve("topk", 5, 100, 5, 0.9)
        assert r.tdigest().create("to-tDigest", 10)
        with pytest.raises(exceptions.NoPermissionError):
            r.bf().info("bloom")

        assert r.ts().create(1, labels={"Redis": "Labs"})
        with pytest.raises(exceptions.NoPermissionError):
            r.ts().info(1)

    @pytest.mark.redismod
    @skip_if_server_version_lt("7.9.0")
    # Redis Enterprise manages ACLs itself and disallows ACL SETUSER here.
    @skip_if_redis_enterprise()
    def test_acl_modules_category_commands(self, r, request):
        default_username = "default"
        username = "redis-py-user"
        password = "pass-for-test-user"

        def teardown():
            r.auth("", default_username)
            r.acl_deluser(username)

        request.addfinalizer(teardown)

        # validate modules categories acl config
        r.acl_setuser(
            username,
            enabled=True,
            reset=True,
            passwords=[f"+{password}"],
            categories=[
                "-all",
                "+@search",
                "+@json",
                "+@bloom",
                "+@cuckoo",
                "+@topk",
                "+@cms",
                "+@timeseries",
                "+@tdigest",
            ],
            keys=["*"],
        )
        r.ft().create_index((TextField("txt"),))
        r.hset("doc1", mapping={"txt": "foo baz"})
        r.hset("doc2", mapping={"txt": "foo bar"})

        r.auth(password, username)

        assert r.ft().search(Query("foo ~bar"))
        assert r.ft().dropindex()

        assert r.json().set("foo", Path.root_path(), "bar")
        assert r.json().get("foo") == "bar"

        assert r.bf().create("bloom", 0.01, 1000)
        assert r.bf().info("bloom")
        assert r.cf().create("cuckoo", 1000)
        assert r.cms().initbydim("cmsDim", 100, 5)
        assert r.topk().reserve("topk", 5, 100, 5, 0.9)
        assert r.tdigest().create("to-tDigest", 10)

        assert r.ts().create(1, labels={"Redis": "Labs"})
        assert r.ts().info(1)

    @pytest.mark.onlynoncluster
    def test_client_list(self, r):
        clients = r.client_list()
        assert isinstance(clients[0], dict)
        assert "addr" in clients[0]

    @pytest.mark.onlynoncluster
    def test_client_list_iter(self, r):
        clients = list(r.client_list_iter())
        assert isinstance(clients[0], dict)
        assert "addr" in clients[0]

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    def test_client_info(self, r):
        info = r.client_info()
        assert isinstance(info, dict)
        assert "addr" in info

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    def test_client_info_name_with_equals(self, r):
        # A client name may contain "=", which the "key=value" CLIENT INFO
        # format made easy to mis-split. Check the name survives the round trip
        # through the real server rather than only the unit-tested parser.
        r.client_setname("test=name")
        info = r.client_info()
        assert info["name"] == "test=name"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("5.0.0")
    def test_client_list_types_not_replica(self, r):
        with pytest.raises(exceptions.RedisError):
            r.client_list(_type="not a client type")
        for client_type in ["normal", "master", "pubsub"]:
            clients = r.client_list(_type=client_type)
            assert isinstance(clients, list)

    @skip_if_redis_enterprise()
    def test_client_list_replica(self, r):
        clients = r.client_list(_type="replica")
        assert isinstance(clients, list)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    # Redis Enterprise fronts the database with a proxy, so CLIENT LIST does not
    # report the same per-connection client set.
    @skip_if_redis_enterprise()
    def test_client_list_client_id(self, r, request):
        clients = r.client_list()
        clients = r.client_list(client_id=[clients[0]["id"]])
        assert len(clients) == 1
        assert "addr" in clients[0]

        # testing multiple client ids
        client_list = list()
        client_count = 3
        for i in range(client_count):
            client = _get_client(redis.Redis, request, flushdb=False)
            client_list.append(client)

        multiple_client_ids = [str(client.client_id()) for client in client_list]
        clients_listed = r.client_list(client_id=multiple_client_ids)
        assert len(clients_listed) == len(multiple_client_ids)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("5.0.0")
    def test_client_id(self, r):
        assert r.client_id() > 0

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    @skip_if_redis_enterprise()
    def test_client_trackinginfo(self, r):
        res = r.client_trackinginfo()
        assert len(res) > 2
        assert "prefixes" in res or b"prefixes" in res

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    def test_client_tracking(self, r, r2):
        # simple case
        assert r.client_tracking_on()
        assert r.client_tracking_off()

        # id based
        client_id = r.client_id()
        assert r.client_tracking_on(client_id)
        assert r.client_tracking_off(client_id)

        # id exists
        client_id = r2.client_id()
        assert r.client_tracking_on(client_id)
        assert r2.client_tracking_off(client_id)

        # now with some prefixes
        with pytest.raises(exceptions.DataError):
            assert r.client_tracking_on(prefix=["foo", "bar", "blee"])

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("5.0.0")
    def test_client_unblock(self, r):
        myid = r.client_id()
        assert not r.client_unblock(myid)
        assert not r.client_unblock(myid, error=True)
        assert not r.client_unblock(myid, error=False)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.6.9")
    def test_client_getname(self, r):
        assert r.client_getname() is None

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.6.9")
    def test_client_setname(self, r):
        assert r.client_setname("redis_py_test")
        assert_resp_response(
            r, r.client_getname(), "redis_py_test", b"redis_py_test", "redis_py_test"
        )

    @skip_if_server_version_lt("7.2.0")
    # Exercises a default localhost client for the deprecated lib_name/lib_version
    # params, which cannot target a remote managed Redis Enterprise endpoint.
    @skip_if_redis_enterprise()
    def test_client_setinfo(self, r: redis.Redis):
        from redis.utils import get_lib_version

        r.ping()
        info = r.client_info()
        assert info["lib-name"] == "redis-py"
        assert info["lib-ver"] == get_lib_version()
        assert r.client_setinfo("lib-name", "test")
        assert r.client_setinfo("lib-ver", "123")
        info = r.client_info()
        assert info["lib-name"] == "test"
        assert info["lib-ver"] == "123"

        # Test deprecated lib_name/lib_version parameters
        with pytest.warns(DeprecationWarning):
            r2 = redis.Redis(lib_name="test2", lib_version="1234")
        info = r2.client_info()
        assert info["lib-name"] == "test2"
        assert info["lib-ver"] == "1234"

    @pytest.mark.fixed_client
    @skip_if_server_version_lt("7.2.0")
    # Exercises a default localhost client, which cannot target a remote managed
    # Redis Enterprise endpoint.
    @skip_if_redis_enterprise()
    def test_client_setinfo_with_driver_info(self):
        from redis import DriverInfo
        from redis.utils import get_lib_version

        info = DriverInfo().add_upstream_driver("django-redis", "5.4.0")
        r2 = redis.Redis(driver_info=info)
        r2.ping()
        client_info = r2.client_info()
        assert client_info["lib-name"] == "redis-py(django-redis_v5.4.0)"
        assert client_info["lib-ver"] == get_lib_version()

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.6.9")
    def test_client_kill(self, r, r2):
        r.client_setname("redis-py-c1")
        r2.client_setname("redis-py-c2")
        clients = [
            client
            for client in r.client_list()
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 2

        clients_by_name = {client.get("name"): client for client in clients}

        client_addr = clients_by_name["redis-py-c2"].get("addr")
        assert r.client_kill(client_addr) is True

        clients = [
            client
            for client in r.client_list()
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 1
        assert clients[0].get("name") == "redis-py-c1"

    @skip_if_server_version_lt("2.8.12")
    def test_client_kill_filter_invalid_params(self, r):
        # empty
        with pytest.raises(exceptions.DataError):
            r.client_kill_filter()

        # invalid skipme
        with pytest.raises(exceptions.DataError):
            r.client_kill_filter(skipme="yeah")

        # invalid type
        with pytest.raises(exceptions.DataError):
            r.client_kill_filter(_type="caster")

    def test_client_kill_filter_accepts_replica_type(self, r):
        with mock.patch.object(r, "execute_command", return_value=1) as execute_command:
            assert r.client_kill_filter(_type="REPLICA") == 1

        execute_command.assert_called_once_with("CLIENT KILL", b"TYPE", "REPLICA")

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.12")
    def test_client_kill_filter_by_id(self, r, r2):
        r.client_setname("redis-py-c1")
        r2.client_setname("redis-py-c2")
        clients = [
            client
            for client in r.client_list()
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 2

        clients_by_name = {client.get("name"): client for client in clients}

        client_2_id = clients_by_name["redis-py-c2"].get("id")
        resp = r.client_kill_filter(_id=client_2_id)
        assert resp == 1

        clients = [
            client
            for client in r.client_list()
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 1
        assert clients[0].get("name") == "redis-py-c1"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.12")
    def test_client_kill_filter_by_addr(self, r, r2):
        r.client_setname("redis-py-c1")
        r2.client_setname("redis-py-c2")
        clients = [
            client
            for client in r.client_list()
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 2

        clients_by_name = {client.get("name"): client for client in clients}

        client_2_addr = clients_by_name["redis-py-c2"].get("addr")
        resp = r.client_kill_filter(addr=client_2_addr)
        assert resp == 1

        clients = [
            client
            for client in r.client_list()
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 1
        assert clients[0].get("name") == "redis-py-c1"

    @skip_if_server_version_lt("2.6.9")
    def test_client_list_after_client_setname(self, r):
        r.client_setname("redis_py_test")
        clients = r.client_list()
        # we don't know which client ours will be
        assert "redis_py_test" in [c["name"] for c in clients]

    @skip_if_server_version_lt("6.2.0")
    def test_client_kill_filter_by_laddr(self, r, r2):
        r.client_setname("redis-py-c1")
        r2.client_setname("redis-py-c2")
        clients = [
            client
            for client in r.client_list()
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 2

        clients_by_name = {client.get("name"): client for client in clients}

        client_2_addr = clients_by_name["redis-py-c2"].get("laddr")
        assert r.client_kill_filter(laddr=client_2_addr)

    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    def test_client_kill_filter_by_user(self, r, request):
        killuser = "user_to_kill"
        r.acl_setuser(
            killuser,
            enabled=True,
            reset=True,
            commands=["+get", "+set", "+select", "+cluster", "+command", "+info"],
            keys=["cache:*"],
            nopass=True,
        )
        _get_client(redis.Redis, request, flushdb=False, username=killuser)
        r.client_kill_filter(user=killuser)
        clients = r.client_list()
        for c in clients:
            assert c["user"] != killuser
        r.acl_deluser(killuser)

    @skip_if_server_version_lt("7.3.240")
    @skip_if_redis_enterprise()
    @pytest.mark.onlynoncluster
    def test_client_kill_filter_by_maxage(self, r, request):
        r2 = _get_client(redis.Redis, request, flushdb=False)
        name = "target-foobar"
        r2.client_setname(name)
        time.sleep(1.5)
        initial_clients = [c["name"] for c in r.client_list()]
        assert name in initial_clients
        r.client_kill_filter(maxage=1)
        final_clients = [c["name"] for c in r.client_list()]
        assert name not in final_clients

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.9.50")
    @skip_if_redis_enterprise()
    def test_client_pause(self, r):
        assert r.client_pause(1)
        assert r.client_pause(timeout=1)
        with pytest.raises(exceptions.RedisError):
            r.client_pause(timeout="not an integer")

    @skip_if_server_version_lt("6.2.0")
    @skip_if_redis_enterprise()
    def test_client_pause_all(self, r, r2):
        assert r.client_pause(1, all=False)
        assert r2.set("foo", "bar")
        assert r2.get("foo") == b"bar"
        assert r.get("foo") == b"bar"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    @skip_if_redis_enterprise()
    def test_client_unpause(self, r):
        assert r.client_unpause() == b"OK"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    # Redis Enterprise does not allow the CLIENT NO-EVICT admin command.
    @skip_if_redis_enterprise()
    def test_client_no_evict(self, r):
        assert r.client_no_evict("ON")
        with pytest.raises(TypeError):
            r.client_no_evict()

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.2.0")
    def test_client_no_touch(self, r):
        assert r.client_no_touch("ON") == b"OK"
        assert r.client_no_touch("OFF") == b"OK"
        with pytest.raises(TypeError):
            r.client_no_touch()

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("3.2.0")
    def test_client_reply(self, r, r_timeout):
        assert r_timeout.client_reply("ON") == b"OK"
        with pytest.raises(exceptions.RedisError):
            r_timeout.client_reply("OFF")

            r_timeout.client_reply("SKIP")

        assert r_timeout.set("foo", "bar")

        # validate it was set
        assert r.get("foo") == b"bar"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    def test_client_getredir(self, r):
        assert isinstance(r.client_getredir(), int)
        assert r.client_getredir() == -1

    @skip_if_server_version_lt("6.0.0")
    def test_hello_notI_implemented(self, r):
        with pytest.raises(NotImplementedError):
            r.hello()

    def test_config_get(self, r):
        data = r.config_get()
        assert len(data.keys()) > 10
        # # assert 'maxmemory' in data
        # assert data['maxmemory'].isdigit()

    @skip_if_server_version_lt("7.0.0")
    # Redis Enterprise does not expose the same CONFIG GET parameter set (e.g. maxmemory).
    @skip_if_redis_enterprise()
    def test_config_get_multi_params(self, r: redis.Redis):
        res = r.config_get("*max-*-entries*", "maxmemory")
        assert "maxmemory" in res
        assert "hash-max-listpack-entries" in res

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_config_resetstat(self, r):
        r.ping()
        prior_commands_processed = int(r.info()["total_commands_processed"])
        assert prior_commands_processed >= 1
        r.config_resetstat()
        reset_commands_processed = int(r.info()["total_commands_processed"])
        assert reset_commands_processed < prior_commands_processed

    @skip_if_redis_enterprise()
    def test_config_set(self, r):
        r.config_set("timeout", 70)
        assert r.config_get()["timeout"] == "70"
        assert r.config_set("timeout", 0)
        assert r.config_get()["timeout"] == "0"

    @skip_if_server_version_lt("7.0.0")
    @skip_if_redis_enterprise()
    def test_config_set_multi_params(self, r: redis.Redis):
        r.config_set("timeout", 70, "maxmemory", 100)
        assert r.config_get()["timeout"] == "70"
        assert r.config_get()["maxmemory"] == "100"
        assert r.config_set("timeout", 0, "maxmemory", 0)
        assert r.config_get()["timeout"] == "0"
        assert r.config_get()["maxmemory"] == "0"

    @pytest.mark.redismod
    @skip_if_server_version_lt("7.9.0")
    # Redis Enterprise exposes a different module CONFIG surface (e.g. search-timeout).
    @skip_if_redis_enterprise()
    def test_config_get_for_modules(self, r: redis.Redis):
        search_module_configs = r.config_get("search-*")
        assert "search-timeout" in search_module_configs

        ts_module_configs = r.config_get("ts-*")
        assert "ts-retention-policy" in ts_module_configs

        bf_module_configs = r.config_get("bf-*")
        assert "bf-error-rate" in bf_module_configs

        cf_module_configs = r.config_get("cf-*")
        assert "cf-initial-size" in cf_module_configs

    @pytest.mark.redismod
    @skip_if_server_version_lt("7.9.0")
    # FT.CONFIG is not available on Redis Enterprise (managed search config).
    @skip_if_redis_enterprise()
    def test_config_set_for_search_module(self, r: redis.Redis):
        initial_default_search_dialect = r.config_get("*")["search-default-dialect"]
        try:
            default_dialect_new = "3"
            assert r.config_set("search-default-dialect", default_dialect_new)
            assert r.config_get("*")["search-default-dialect"] == default_dialect_new
            search_config = r.ft().config_get("*")
            if expects_resp2_shape(r) or expects_resp3_shape(r):
                dialect = search_config[b"DEFAULT_DIALECT"].decode()
            elif expects_unified_shape(r):
                dialect = search_config["DEFAULT_DIALECT"]
            assert dialect == default_dialect_new
        except AssertionError as ex:
            raise ex
        finally:
            assert r.config_set(
                "search-default-dialect", initial_default_search_dialect
            )

        with pytest.raises(exceptions.ResponseError):
            r.config_set("search-max-doctablesize", 2000000)

    @skip_if_server_version_lt("6.0.0")
    @skip_if_redis_enterprise()
    def test_failover(self, r):
        with pytest.raises(NotImplementedError):
            r.failover()

    @pytest.mark.onlynoncluster
    def test_dbsize(self, r):
        r["a"] = "foo"
        r["b"] = "bar"
        assert r.dbsize() == 2

    @pytest.mark.onlynoncluster
    def test_echo(self, r):
        assert r.echo("foo bar") == b"foo bar"

    @pytest.mark.onlynoncluster
    def test_info(self, r):
        r["a"] = "foo"
        r["b"] = "bar"
        info = r.info()
        assert isinstance(info, dict)
        assert "arch_bits" in info.keys()
        assert "redis_version" in info.keys()

    @pytest.mark.onlynoncluster
    def test_info_with_custom_callback(self, decoded_r):
        decoded_r.set_response_callback("INFO", lambda response: response)
        assert "redis_version:" in decoded_r.info("server")

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    @skip_if_redis_enterprise()
    def test_info_with_non_utf8_errorstat(self, request, r, decoded_r):
        r.config_resetstat()
        with pytest.raises(redis.ResponseError):
            r.eval(b"return redis.error_reply('\xff\xfeBAD oops')", 0)
        try:
            info = decoded_r.info("errorstats")
            assert info["errorstat_\ufffd\ufffdBAD"] == {"count": 1}
            assert "redis_version" in decoded_r.info()

            with _get_client(
                redis.Redis,
                request,
                flushdb=False,
                decode_responses=True,
                encoding_errors="backslashreplace",
            ) as client:
                info = client.info("errorstats")
                assert info["errorstat_\\xff\\xfeBAD"] == {"count": 1}
        finally:
            r.config_resetstat()

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_info_multi_sections(self, r):
        res = r.info("clients", "server")
        assert isinstance(res, dict)
        assert "redis_version" in res
        assert "connected_clients" in res

    @pytest.mark.redismod
    @skip_if_server_version_lt("7.9.0")
    # Redis Enterprise reports a different INFO modules section shape.
    @skip_if_redis_enterprise()
    def test_info_with_modules(self, r: redis.Redis):
        res = r.info(section="everything")
        assert "modules" in res
        assert "search_number_of_indexes" in res

        res = r.info(section="modules")
        assert "modules" in res
        assert "search_number_of_indexes" in res

        res = r.info(section="search")
        assert "modules" not in res
        assert "search_number_of_indexes" in res

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_lastsave(self, r):
        assert isinstance(r.lastsave(), datetime.datetime)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("5.0.0")
    @skip_if_server_version_gte("8.0.0")
    def test_lolwut(self, r):
        lolwut = r.lolwut().decode("utf-8")
        assert "Redis ver." in lolwut

        lolwut = r.lolwut(5, 6, 7, 8).decode("utf-8")
        assert "Redis ver." in lolwut

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.0.0")
    def test_lolwut_v8_and_higher(self, r):
        lolwut = r.lolwut().decode("utf-8")
        assert lolwut

        lolwut = r.lolwut(5, 6, 7, 8).decode("utf-8")
        assert lolwut

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    @skip_if_redis_enterprise()
    def test_reset(self, r):
        assert_resp_response(r, r.reset(), "RESET", b"RESET", "RESET")

    def test_object(self, r):
        r["a"] = "foo"
        assert isinstance(r.object("refcount", "a"), int)
        assert isinstance(r.object("idletime", "a"), int)
        assert r.object("encoding", "a") in (b"raw", b"embstr")
        assert r.object("idletime", "invalid-key") is None

    def test_ping(self, r):
        assert r.ping()

    @pytest.mark.onlynoncluster
    def test_quit(self, r):
        assert r.quit()

    @skip_if_server_version_lt("2.8.12")
    @skip_if_redis_enterprise()
    @pytest.mark.onlynoncluster
    def test_role(self, r):
        assert r.role()[0] == b"master"
        assert isinstance(r.role()[1], int)
        assert isinstance(r.role()[2], list)

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_select(self, r):
        assert r.select(5)
        assert r.select(2)
        assert r.select(9)

    @pytest.mark.onlynoncluster
    # Redis Enterprise's SLOWLOG GET entries omit the client_address field.
    @skip_if_redis_enterprise()
    def test_slowlog_get(self, r, slowlog):
        assert r.slowlog_reset()
        unicode_string = chr(3456) + "abcd" + chr(3421)
        r.get(unicode_string)
        slowlog = r.slowlog_get()
        assert isinstance(slowlog, list)
        commands = [log["command"] for log in slowlog]

        get_command = b" ".join((b"GET", unicode_string.encode("utf-8")))
        assert get_command in commands
        assert b"SLOWLOG RESET" in commands
        # the order should be ['GET <uni string>', 'SLOWLOG RESET'],
        # but if other clients are executing commands at the same time, there
        # could be commands, before, between, or after, so just check that
        # the two we care about are in the appropriate order.
        assert commands.index(get_command) < commands.index(b"SLOWLOG RESET")

        # make sure other attributes are typed correctly
        assert isinstance(slowlog[0]["start_time"], int)
        assert isinstance(slowlog[0]["duration"], int)
        assert isinstance(slowlog[0]["client_address"], bytes)
        assert isinstance(slowlog[0]["client_name"], bytes)

        # Mock result if we didn't get slowlog complexity info.
        if "complexity" not in slowlog[0]:
            # monkey patch parse_response()
            COMPLEXITY_STATEMENT = "Complexity info: N:4712,M:3788"
            old_parse_response = r.parse_response

            def parse_response(connection, command_name, **options):
                if command_name != "SLOWLOG GET":
                    return old_parse_response(connection, command_name, **options)
                responses = connection.read_response()
                for response in responses:
                    # Complexity info stored as fourth item in list
                    response.insert(3, COMPLEXITY_STATEMENT)
                return r.response_callbacks[command_name](responses, **options)

            r.parse_response = parse_response

            # test
            slowlog = r.slowlog_get()
            assert isinstance(slowlog, list)
            commands = [log["command"] for log in slowlog]
            assert get_command in commands
            idx = commands.index(get_command)
            assert slowlog[idx]["complexity"] == COMPLEXITY_STATEMENT

            # tear down monkeypatch
            r.parse_response = old_parse_response

    @pytest.mark.onlynoncluster
    def test_slowlog_get_limit(self, r, slowlog):
        assert r.slowlog_reset()
        r.get("foo")
        slowlog = r.slowlog_get(1)
        assert isinstance(slowlog, list)
        # only one command, based on the number we passed to slowlog_get()
        assert len(slowlog) == 1

    @pytest.mark.onlynoncluster
    def test_slowlog_length(self, r, slowlog):
        r.get("foo")
        assert isinstance(r.slowlog_len(), int)

    @skip_if_server_version_lt("2.6.0")
    def test_time(self, r):
        t = r.time()
        assert len(t) == 2
        assert isinstance(t[0], int)
        assert isinstance(t[1], int)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    def test_hotkeys_start_basic(self, r):
        """Test basic HOTKEYS START command with CPU metric"""
        # Reset any previous session
        try:
            r.hotkeys_stop()
        except Exception:
            pass

        # Start collection with CPU metric
        result = r.hotkeys_start(count=10, metrics=[HotkeysMetricsTypes.CPU])
        assert result == b"OK"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    def test_hotkeys_start_with_all_metrics(self, r):
        """Test HOTKEYS START with both CPU and NET metrics"""
        try:
            r.hotkeys_stop()
        except Exception:
            pass

        result = r.hotkeys_start(
            count=5, metrics=[HotkeysMetricsTypes.CPU, HotkeysMetricsTypes.NET]
        )
        assert result == b"OK"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    def test_hotkeys_start_with_duration(self, r):
        """Test HOTKEYS START with duration parameter"""
        try:
            r.hotkeys_stop()
        except Exception:
            pass

        result = r.hotkeys_start(
            count=10, metrics=[HotkeysMetricsTypes.CPU], duration=60
        )
        assert result == b"OK"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    def test_hotkeys_start_with_sample_ratio(self, r):
        """Test HOTKEYS START with sample ratio"""
        try:
            r.hotkeys_stop()
        except Exception:
            pass

        result = r.hotkeys_start(
            count=10, metrics=[HotkeysMetricsTypes.CPU], sample_ratio=10
        )
        assert result == b"OK"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    def test_hotkeys_start_fail__on_noncluster_setup_with_slots(self, r):
        """Test HOTKEYS START with specific hash slots"""
        try:
            r.hotkeys_stop()
        except Exception:
            pass

        # slots is not supported argument for non-cluster setups
        with pytest.raises(Exception):
            r.hotkeys_start(
                count=10, metrics=[HotkeysMetricsTypes.CPU], slots=[0, 100, 200]
            )

    @pytest.mark.onlynoncluster
    def test_hotkeys_start_empty_slots_omits_slots(self, r):
        """Test HOTKEYS START does not send SLOTS for an empty slots list"""
        with mock.patch.object(r, "execute_command", return_value=b"OK") as cmd:
            r.hotkeys_start(metrics=[HotkeysMetricsTypes.CPU], slots=[])
        cmd.assert_called_once_with("HOTKEYS", "START", "METRICS", 1, "CPU")

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    def test_hotkeys_start_with_all_parameters(self, r):
        """Test HOTKEYS START with all optional parameters"""
        try:
            r.hotkeys_stop()
        except Exception:
            pass

        result = r.hotkeys_start(
            count=5,
            metrics=[HotkeysMetricsTypes.CPU, HotkeysMetricsTypes.NET],
            duration=30,
            sample_ratio=5,
        )
        assert result == b"OK"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    def test_hotkeys_stop(self, r):
        """Test HOTKEYS STOP command"""
        try:
            r.hotkeys_stop()
        except Exception:
            pass

        # Start a collection session
        r.hotkeys_start(count=10, metrics=[HotkeysMetricsTypes.CPU])

        # Stop the session
        result = r.hotkeys_stop()
        assert result == b"OK"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    def test_hotkeys_reset(self, r):
        """Test HOTKEYS RESET command"""
        try:
            r.hotkeys_stop()
        except Exception:
            pass

        # Start a session and generate some data
        r.hotkeys_start(count=10, metrics=[HotkeysMetricsTypes.CPU])

        # Perform some operations to generate hotkeys data
        for i in range(5):
            r.set(f"resetkey{i}", f"value{i}")
            r.get(f"resetkey{i}")

        # Stop the session
        r.hotkeys_stop()

        # Get results before reset - should have data
        result_before = r.hotkeys_get()
        assert isinstance(result_before, list)
        for res_elem in result_before:
            assert isinstance(res_elem, dict)
            assert b"tracking-active" in res_elem

        # Reset the results
        result = r.hotkeys_reset()
        assert result == b"OK"

        # No session exists after reset
        assert r.hotkeys_get() is None

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    def test_hotkeys_get_no_session(self, r):
        """Test HOTKEYS GET when there is no collection session"""
        try:
            r.hotkeys_stop()
        except Exception:
            pass
        r.hotkeys_reset()

        assert r.hotkeys_get() is None

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    def test_hotkeys_get_ongoing_session(self, r):
        """Test HOTKEYS GET during an ongoing collection session"""
        try:
            r.hotkeys_stop()
        except Exception:
            pass

        # Start a collection session
        r.hotkeys_start(
            count=10, metrics=[HotkeysMetricsTypes.CPU, HotkeysMetricsTypes.NET]
        )

        # Perform some operations to generate hotkeys data
        for i in range(10):
            r.set(f"key{i}", f"value{i}")
            r.get(f"key{i}")

        # Get the results
        result = r.hotkeys_get()

        # Verify the response structure
        assert isinstance(result, list)
        for res_elem in result:
            assert isinstance(res_elem, dict)
            # Check tracking-active is 1 (ongoing session)
            assert res_elem[b"tracking-active"] == 1

        # Stop the session
        r.hotkeys_stop()

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    def test_hotkeys_get_terminated_session(self, r):
        """Test HOTKEYS GET after stopping a collection session"""
        try:
            r.hotkeys_stop()
        except Exception:
            pass

        # Start a collection session
        r.hotkeys_start(count=10, metrics=[HotkeysMetricsTypes.CPU])

        # Perform some operations
        for i in range(5):
            r.set(f"testkey{i}", f"testvalue{i}")

        # Stop the session
        r.hotkeys_stop()

        # Get the results
        result = r.hotkeys_get()

        # Verify the response structure
        assert isinstance(result, list)
        for res_elem in result:
            assert isinstance(res_elem, dict)

            # Check tracking-active is 0 (terminated session)
            assert res_elem[b"tracking-active"] == 0

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    def test_hotkeys_get_all_fields(self, r):
        """Test HOTKEYS GET returns all documented fields"""
        try:
            r.hotkeys_stop()
        except Exception:
            pass

        # Start a collection session with all parameters
        r.hotkeys_start(
            count=5,
            metrics=[HotkeysMetricsTypes.CPU, HotkeysMetricsTypes.NET],
            sample_ratio=1,
        )

        # Perform operations to generate data
        # one round trip; plain strings, so "{3}" stays a hashtag in the GETs
        pipe = r.pipeline(transaction=False)
        for i in range(20):
            pipe.set("anyprefix:{3}:key", f"value{i}")
            pipe.get("anyprefix:{3}:key")
            pipe.set("anyprefix:{1}:key", f"value{i}")
            pipe.get("anyprefix:{1}:key")
        pipe.execute()

        # Stop the session
        r.hotkeys_stop()

        # Get the results
        result = r.hotkeys_get()
        assert isinstance(result, list)

        # Verify all documented fields are present
        expected_fields = [
            b"tracking-active",
            b"sample-ratio",
            b"selected-slots",
            b"net-bytes-all-commands-all-slots",
            b"collection-start-time-unix-ms",
            b"collection-duration-ms",
            b"total-cpu-time-user-ms",
            b"total-cpu-time-sys-ms",
            b"total-net-bytes",
            b"by-cpu-time-us",
            b"by-net-bytes",
        ]

        for res_elem in result:
            for field in expected_fields:
                assert field in res_elem, (
                    f"Field '{field}' is missing from HOTKEYS GET response"
                )

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    def test_hotkeys_get_all_fields_decoded(self, decoded_r: redis.Redis):
        """Test HOTKEYS GET returns all documented fields"""
        try:
            decoded_r.hotkeys_stop()
        except Exception:
            pass

        # Start a collection session with all parameters
        decoded_r.hotkeys_start(
            count=5,
            metrics=[HotkeysMetricsTypes.CPU, HotkeysMetricsTypes.NET],
            sample_ratio=1,
        )

        # Perform operations to generate data
        # one round trip; plain strings, so "{3}" stays a hashtag in the GETs
        pipe = decoded_r.pipeline(transaction=False)
        for i in range(20):
            pipe.set("anyprefix:{3}:key", f"value{i}")
            pipe.get("anyprefix:{3}:key")
            pipe.set("anyprefix:{1}:key", f"value{i}")
            pipe.get("anyprefix:{1}:key")
        pipe.execute()

        # Stop the session
        decoded_r.hotkeys_stop()

        # Get the results
        result = decoded_r.hotkeys_get()

        # Verify all documented fields are present
        expected_fields = [
            "tracking-active",
            "sample-ratio",
            "selected-slots",
            "net-bytes-all-commands-all-slots",
            "collection-start-time-unix-ms",
            "collection-duration-ms",
            "total-cpu-time-user-ms",
            "total-cpu-time-sys-ms",
            "total-net-bytes",
            "by-cpu-time-us",
            "by-net-bytes",
        ]

        for elem in result:
            for field in expected_fields:
                assert field in elem, (
                    f"Field '{field}' is missing from HOTKEYS GET response"
                )

    @skip_if_redis_enterprise()
    def test_bgsave(self, r):
        self._wait_for_bgsave(r)
        assert r.bgsave()
        self._wait_for_bgsave(r)
        assert r.bgsave(True)
        self._wait_for_bgsave(r)

    def test_never_decode_option(self, r: redis.Redis):
        opts = {NEVER_DECODE: []}
        r.delete("a")
        assert r.execute_command("EXISTS", "a", **opts) == 0

    def test_empty_response_option(self, r: redis.Redis):
        opts = {EMPTY_RESPONSE: []}
        r.delete("a")
        assert r.execute_command("EXISTS", "a", **opts) == 0

    # BASIC KEY COMMANDS
    def test_append(self, r):
        assert r.append("a", "a1") == 2
        assert r["a"] == b"a1"
        assert r.append("a", "a2") == 4
        assert r["a"] == b"a1a2"

    @skip_if_server_version_lt("2.6.0")
    def test_bitcount(self, r):
        r.setbit("a", 5, True)
        assert r.bitcount("a") == 1
        r.setbit("a", 6, True)
        assert r.bitcount("a") == 2
        r.setbit("a", 5, False)
        assert r.bitcount("a") == 1
        r.setbit("a", 9, True)
        r.setbit("a", 17, True)
        r.setbit("a", 25, True)
        r.setbit("a", 33, True)
        assert r.bitcount("a") == 5
        assert r.bitcount("a", 0, -1) == 5
        assert r.bitcount("a", 2, 3) == 2
        assert r.bitcount("a", 2, -1) == 3
        assert r.bitcount("a", -2, -1) == 2
        assert r.bitcount("a", 1, 1) == 1

    @skip_if_server_version_lt("7.0.0")
    def test_bitcount_mode(self, r):
        r.set("mykey", "foobar")
        assert r.bitcount("mykey") == 26
        assert r.bitcount("mykey", 1, 1, "byte") == 6
        assert r.bitcount("mykey", 5, 30, "bit") == 17
        with pytest.raises(redis.ResponseError):
            assert r.bitcount("mykey", 5, 30, "but")

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.6.0")
    def test_bitop_not_empty_string(self, r):
        r["a"] = ""
        r.bitop("not", "r", "a")
        assert r.get("r") is None

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.6.0")
    def test_bitop_not(self, r):
        test_str = b"\xaa\x00\xff\x55"
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        r["a"] = test_str
        r.bitop("not", "r", "a")
        assert int(binascii.hexlify(r["r"]), 16) == correct

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.6.0")
    def test_bitop_not_in_place(self, r):
        test_str = b"\xaa\x00\xff\x55"
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        r["a"] = test_str
        r.bitop("not", "a", "a")
        assert int(binascii.hexlify(r["a"]), 16) == correct

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.6.0")
    def test_bitop_single_string(self, r):
        test_str = b"\x01\x02\xff"
        r["a"] = test_str
        r.bitop("and", "res1", "a")
        r.bitop("or", "res2", "a")
        r.bitop("xor", "res3", "a")
        assert r["res1"] == test_str
        assert r["res2"] == test_str
        assert r["res3"] == test_str

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.6.0")
    def test_bitop_string_operands(self, r):
        r["a"] = b"\x01\x02\xff\xff"
        r["b"] = b"\x01\x02\xff"
        r.bitop("and", "res1", "a", "b")
        r.bitop("or", "res2", "a", "b")
        r.bitop("xor", "res3", "a", "b")
        assert int(binascii.hexlify(r["res1"]), 16) == 0x0102FF00
        assert int(binascii.hexlify(r["res2"]), 16) == 0x0102FFFF
        assert int(binascii.hexlify(r["res3"]), 16) == 0x000000FF

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.1.224")
    def test_bitop_diff(self, r):
        r["a"] = b"\xf0"
        r["b"] = b"\xc0"
        r["c"] = b"\x80"

        result = r.bitop("DIFF", "result", "a", "b", "c")
        assert result == 1
        assert r["result"] == b"\x30"

        r.bitop("DIFF", "result2", "a", "nonexistent")
        assert r["result2"] == b"\xf0"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.1.224")
    def test_bitop_diff1(self, r):
        r["a"] = b"\xf0"
        r["b"] = b"\xc0"
        r["c"] = b"\x80"

        result = r.bitop("DIFF1", "result", "a", "b", "c")
        assert result == 1
        assert r["result"] == b"\x00"

        r["d"] = b"\x0f"
        r["e"] = b"\x03"
        r.bitop("DIFF1", "result2", "d", "e")
        assert r["result2"] == b"\x00"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.1.224")
    def test_bitop_andor(self, r):
        r["a"] = b"\xf0"
        r["b"] = b"\xc0"
        r["c"] = b"\x80"

        result = r.bitop("ANDOR", "result", "a", "b", "c")
        assert result == 1
        assert r["result"] == b"\xc0"

        r["x"] = b"\xf0"
        r["y"] = b"\x0f"
        r.bitop("ANDOR", "result2", "x", "y")
        assert r["result2"] == b"\x00"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.1.224")
    def test_bitop_one(self, r):
        r["a"] = b"\xf0"
        r["b"] = b"\xc0"
        r["c"] = b"\x80"

        result = r.bitop("ONE", "result", "a", "b", "c")
        assert result == 1
        assert r["result"] == b"\x30"

        r["x"] = b"\xf0"
        r["y"] = b"\x0f"
        r.bitop("ONE", "result2", "x", "y")
        assert r["result2"] == b"\xff"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.1.224")
    def test_bitop_new_operations_with_empty_keys(self, r):
        r["a"] = b"\xff"

        r.bitop("DIFF", "empty_result", "nonexistent", "a")
        assert r.get("empty_result") == b"\x00"

        r.bitop("DIFF1", "empty_result2", "a", "nonexistent")
        assert r.get("empty_result2") == b"\x00"

        r.bitop("ANDOR", "empty_result3", "a", "nonexistent")
        assert r.get("empty_result3") == b"\x00"

        r.bitop("ONE", "empty_result4", "nonexistent")
        assert r.get("empty_result4") is None

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.1.224")
    def test_bitop_new_operations_return_values(self, r):
        r["a"] = b"\xff\x00\xff"
        r["b"] = b"\x00\xff"

        result1 = r.bitop("DIFF", "result1", "a", "b")
        assert result1 == 3

        result2 = r.bitop("DIFF1", "result2", "a", "b")
        assert result2 == 3

        result3 = r.bitop("ANDOR", "result3", "a", "b")
        assert result3 == 3

        result4 = r.bitop("ONE", "result4", "a", "b")
        assert result4 == 3

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.7")
    def test_bitpos(self, r):
        key = "key:bitpos"
        r.set(key, b"\xff\xf0\x00")
        assert r.bitpos(key, 0) == 12
        assert r.bitpos(key, 0, 2, -1) == 16
        assert r.bitpos(key, 0, -2, -1) == 12
        r.set(key, b"\x00\xff\xf0")
        assert r.bitpos(key, 1, 0) == 8
        assert r.bitpos(key, 1, 1) == 8
        r.set(key, b"\x00\x00\x00")
        assert r.bitpos(key, 1) == -1

    @skip_if_server_version_lt("2.8.7")
    def test_bitpos_wrong_arguments(self, r):
        key = "key:bitpos:wrong:args"
        r.set(key, b"\xff\xf0\x00")
        with pytest.raises(exceptions.RedisError):
            r.bitpos(key, 0, end=1) == 12
        with pytest.raises(exceptions.RedisError):
            r.bitpos(key, 7) == 12

    @skip_if_server_version_lt("7.0.0")
    def test_bitpos_mode(self, r):
        r.set("mykey", b"\x00\xff\xf0")
        assert r.bitpos("mykey", 1, 0) == 8
        assert r.bitpos("mykey", 1, 2, -1, "byte") == 16
        assert r.bitpos("mykey", 0, 7, 15, "bit") == 7
        with pytest.raises(redis.ResponseError):
            r.bitpos("mykey", 1, 7, 15, "bite")

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    def test_copy(self, r):
        assert r.copy("a", "b") == 0
        r.set("a", "foo")
        assert r.copy("a", "b") == 1
        assert r.get("a") == b"foo"
        assert r.get("b") == b"foo"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    def test_copy_and_replace(self, r):
        r.set("a", "foo1")
        r.set("b", "foo2")
        assert r.copy("a", "b") == 0
        assert r.copy("a", "b", replace=True) == 1

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    # Redis Enterprise exposes a single logical database, so copying across DB
    # indexes is not supported.
    @skip_if_redis_enterprise()
    def test_copy_to_another_database(self, request):
        r0 = _get_client(redis.Redis, request, db=0)
        r1 = _get_client(redis.Redis, request, db=1)
        r0.set("a", "foo")
        assert r0.copy("a", "b", destination_db=1) == 1
        assert r1.get("b") == b"foo"

    def test_decr(self, r):
        assert r.decr("a") == -1
        assert r["a"] == b"-1"
        assert r.decr("a") == -2
        assert r["a"] == b"-2"
        assert r.decr("a", amount=5) == -7
        assert r["a"] == b"-7"

    def test_decrby(self, r):
        assert r.decrby("a", amount=2) == -2
        assert r.decrby("a", amount=3) == -5
        assert r["a"] == b"-5"

    def test_delete(self, r):
        assert r.delete("a") == 0
        r["a"] = "foo"
        assert r.delete("a") == 1

    def test_delete_with_multiple_keys(self, r):
        r["a"] = "foo"
        r["b"] = "bar"
        assert r.delete("a", "b") == 2
        assert r.get("a") is None
        assert r.get("b") is None

    def test_delitem(self, r):
        r["a"] = "foo"
        del r["a"]
        assert r.get("a") is None

    def _ensure_str(self, x):
        return x.decode("ascii") if isinstance(x, (bytes, bytearray)) else x

    def _server_xxh3_digest(self, r, key):
        """
        Get the server-computed XXH3 hex digest for the key's value.
        Requires the DIGEST command implemented on the server.
        """
        d = r.execute_command("DIGEST", key)
        return None if d is None else self._ensure_str(d).lower()

    @skip_if_server_version_lt("8.3.224")
    def test_delex_nonexistent(self, r):
        r.delete("nope")
        assert r.delex("nope") == 0

    @skip_if_server_version_lt("8.3.224")
    def test_delex_unconditional_delete_string(self, r):
        r.set("k", b"v")
        assert r.exists("k") == 1
        assert r.delex("k") == 1
        assert r.exists("k") == 0

    @skip_if_server_version_lt("8.3.224")
    def test_delex_unconditional_delete_nonstring_allowed(self, r):
        # Spec: error happens only when a condition is specified on a non-string key.
        r.lpush("lst", "a")
        assert r.delex("lst") == 1
        assert r.exists("lst") == 0

        r.lpush("lst", "a")

        with pytest.raises(redis.ResponseError):
            r.delex("lst", ifeq=b"a")
        assert r.exists("lst") == 1

    @skip_if_server_version_lt("8.3.224")
    def test_delex_ifeq(self, r):
        r.set("k", b"abc")
        assert r.delex("k", ifeq=b"abc") == 1  # matches → deleted
        assert r.exists("k") == 0

        r.set("k", b"abc")
        assert r.delex("k", ifeq=b"zzz") == 0  # not match → not deleted
        assert r.get("k") == b"abc"  # still there

    @skip_if_server_version_lt("8.3.224")
    def test_delex_ifne(self, r):
        r.set("k2", b"abc")
        assert r.delex("k2", ifne=b"zzz") == 1  # different → deleted
        assert r.exists("k2") == 0

        r.set("k2", b"abc")
        assert r.delex("k2", ifne=b"abc") == 0  # equal → not deleted
        assert r.get("k2") == b"abc"

    @skip_if_server_version_lt("8.3.224")
    def test_delex_with_conditionon_nonstring_values(self, r):
        r.lpush("nk", "x")
        with pytest.raises(redis.ResponseError):
            r.delex("nk", ifeq=b"x")
        with pytest.raises(redis.ResponseError):
            r.delex("nk", ifne=b"x")
        with pytest.raises(redis.ResponseError):
            r.delex("nk", ifdeq="deadbeef")

    @skip_if_server_version_lt("8.3.224")
    @pytest.mark.parametrize("val", [b"", b"abc", b"The quick brown fox"])
    def test_delex_ifdeq_and_ifdne(self, r, val):
        r.set("h", val)
        d = self._server_xxh3_digest(r, "h")
        assert d is not None

        # IFDEQ should delete with exact digest
        r.set("h", val)
        assert r.delex("h", ifdeq=d) == 1
        assert r.exists("h") == 0

        # IFDNE should NOT delete when digest matches
        r.set("h", val)
        assert r.delex("h", ifdne=d) == 0
        assert r.get("h") == val

        # IFDNE should delete when digest doesn't match
        r.set("h", val)
        wrong = "0" * len(d)
        if wrong == d:
            wrong = "f" * len(d)
        assert r.delex("h", ifdne=wrong) == 1
        assert r.exists("h") == 0

    @skip_if_server_version_lt("8.3.224")
    def test_delex_pipeline(self, r):
        r.mset({"p1{45}": b"A", "p2{45}": b"B"})
        p = r.pipeline()
        p.delex("p1{45}", ifeq=b"A")
        p.delex("p2{45}", ifne=b"B")  # false → 0
        p.delex("nope")  # nonexistent → 0
        out = p.execute()
        assert out == [1, 0, 0]

    @skip_if_server_version_lt("8.3.224")
    def test_delex_mutual_exclusion_client_side(self, r):
        with pytest.raises(ValueError):
            r.delex("k", ifeq=b"A", ifne=b"B")
        with pytest.raises(ValueError):
            r.delex("k", ifdeq="aa", ifdne="bb")

    @skip_if_server_version_lt("4.0.0")
    def test_unlink(self, r):
        assert r.unlink("a") == 0
        r["a"] = "foo"
        assert r.unlink("a") == 1
        assert r.get("a") is None

    @skip_if_server_version_lt("4.0.0")
    def test_unlink_with_multiple_keys(self, r):
        r["a"] = "foo"
        r["b"] = "bar"
        assert r.unlink("a", "b") == 2
        assert r.get("a") is None
        assert r.get("b") is None

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_lcs(self, r):
        r.mset({"foo": "ohmytext", "bar": "mynewtext"})
        assert r.lcs("foo", "bar") == b"mytext"
        assert r.lcs("foo", "bar", len=True) == 6
        assert_resp_response(
            r,
            r.lcs("foo", "bar", idx=True, minmatchlen=3),
            [b"matches", [[[4, 7], [5, 8]]], b"len", 6],
            {b"matches": [[[4, 7], [5, 8]]], b"len": 6},
            {"matches": [[[4, 7], [5, 8]]], "len": 6},
        )
        with pytest.raises(redis.ResponseError):
            assert r.lcs("foo", "bar", len=True, idx=True)

    @skip_if_server_version_lt("2.6.0")
    def test_dump_and_restore(self, r):
        r["a"] = "foo"
        dumped = r.dump("a")
        del r["a"]
        r.restore("a", 0, dumped)
        assert r["a"] == b"foo"

    @skip_if_server_version_lt("3.0.0")
    def test_dump_and_restore_and_replace(self, r):
        r["a"] = "bar"
        dumped = r.dump("a")
        with pytest.raises(redis.ResponseError):
            r.restore("a", 0, dumped)

        r.restore("a", 0, dumped, replace=True)
        assert r["a"] == b"bar"

    @skip_if_server_version_lt("5.0.0")
    def test_dump_and_restore_absttl(self, r):
        r["a"] = "foo"
        dumped = r.dump("a")
        del r["a"]
        ttl = int(
            (redis_server_time(r) + datetime.timedelta(minutes=1)).timestamp() * 1000
        )
        r.restore("a", ttl, dumped, absttl=True)
        assert r["a"] == b"foo"
        assert 0 < r.ttl("a") <= 61

    def test_exists(self, r):
        assert r.exists("a") == 0
        r["a"] = "foo"
        r["b"] = "bar"
        assert r.exists("a") == 1
        assert r.exists("a", "b") == 2

    def test_exists_contains(self, r):
        assert "a" not in r
        r["a"] = "foo"
        assert "a" in r

    def test_expire(self, r):
        assert r.expire("a", 10) is False
        r["a"] = "foo"
        assert r.expire("a", 10) is True
        assert 0 < r.ttl("a") <= 10
        assert r.persist("a")
        assert r.ttl("a") == -1

    @skip_if_server_version_lt("7.0.0")
    def test_expire_option_nx(self, r):
        r.set("key", "val")
        assert r.expire("key", 100, nx=True) == 1
        assert r.expire("key", 500, nx=True) == 0

    @skip_if_server_version_lt("7.0.0")
    def test_expire_option_xx(self, r):
        r.set("key", "val")
        assert r.expire("key", 100, xx=True) == 0
        assert r.expire("key", 100)
        assert r.expire("key", 500, xx=True) == 1

    @skip_if_server_version_lt("7.0.0")
    def test_expire_option_gt(self, r):
        r.set("key", "val", 100)
        assert r.expire("key", 50, gt=True) == 0
        assert r.expire("key", 500, gt=True) == 1

    @skip_if_server_version_lt("7.0.0")
    def test_expire_option_lt(self, r):
        r.set("key", "val", 100)
        assert r.expire("key", 50, lt=True) == 1
        assert r.expire("key", 150, lt=True) == 0

    def test_expireat_datetime(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        r["a"] = "foo"
        assert r.expireat("a", expire_at) is True
        assert 0 < r.ttl("a") <= 61

    def test_expireat_no_key(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        assert r.expireat("a", expire_at) is False

    def test_expireat_unixtime(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        r["a"] = "foo"
        expire_at_seconds = int(expire_at.timestamp())
        assert r.expireat("a", expire_at_seconds) is True
        assert 0 < r.ttl("a") <= 61

    @skip_if_server_version_lt("7.0.0")
    def test_expiretime(self, r):
        r.set("a", "foo")
        r.expireat("a", 33177117420)
        assert r.expiretime("a") == 33177117420

    @skip_if_server_version_lt("7.0.0")
    def test_expireat_option_nx(self, r):
        assert r.set("key", "val") is True
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        assert r.expireat("key", expire_at, nx=True) is True
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=2)
        assert r.expireat("key", expire_at, nx=True) is False

    @skip_if_server_version_lt("7.0.0")
    def test_expireat_option_xx(self, r):
        assert r.set("key", "val") is True
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        assert r.expireat("key", expire_at, xx=True) is False
        assert r.expireat("key", expire_at) is True
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=2)
        assert r.expireat("key", expire_at, xx=True) is True

    @skip_if_server_version_lt("7.0.0")
    def test_expireat_option_gt(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=2)
        assert r.set("key", "val") is True
        assert r.expireat("key", expire_at) is True
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        assert r.expireat("key", expire_at, gt=True) is False
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=3)
        assert r.expireat("key", expire_at, gt=True) is True

    @skip_if_server_version_lt("7.0.0")
    def test_expireat_option_lt(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=2)
        assert r.set("key", "val") is True
        assert r.expireat("key", expire_at) is True
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=3)
        assert r.expireat("key", expire_at, lt=True) is False
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        assert r.expireat("key", expire_at, lt=True) is True

    @skip_if_server_version_lt("8.3.224")
    def test_digest_nonexistent_returns_none(self, r):
        assert r.digest("no:such:key") is None

    @skip_if_server_version_lt("8.3.224")
    def test_digest_wrong_type_raises(self, r):
        r.lpush("alist", "x")
        with pytest.raises(Exception):  # or redis.exceptions.ResponseError
            r.digest("alist")

    @skip_if_server_version_lt("8.3.224")
    @pytest.mark.parametrize(
        "value", [b"", b"abc", b"The quick brown fox jumps over the lazy dog"]
    )
    def test_digest_response_when_available(self, r, value):
        key = "k:digest"
        r.delete(key)
        r.set(key, value)

        res = r.digest(key)

        # got is str if decode_responses=True; ensure bytes->str for comparison
        if isinstance(res, bytes):
            res = res.decode()
        assert res is not None
        assert all(c in "0123456789abcdefABCDEF" for c in res)

        assert len(res) == 16

    @skip_if_server_version_lt("8.3.224")
    @pytest.mark.parametrize(
        "value",
        [
            b"",
            b"abc",
            b"The quick brown fox jumps over the lazy dog",
            "",
            "abc",
            "The quick brown fox jumps over the lazy dog",
        ],
    )
    def test_local_digest_matches_server(self, r, value):
        key = "k:digest"
        r.delete(key)
        r.set(key, value)

        res_server = r.digest(key)
        res_local = r.digest_local(value)

        # Verify type consistency between server and local digest
        if isinstance(res_server, bytes):
            assert isinstance(res_local, bytes)
        else:
            assert isinstance(res_local, str)

        assert res_server is not None
        assert len(res_server) == 16
        assert res_local is not None
        assert len(res_local) == 16
        assert res_server == res_local

    @pytest.mark.fixed_client
    @pytest.mark.skipif(not HAS_XXHASH, reason="xxhash is not installed")
    def test_local_digest_encodes_with_the_client_encoding(self):
        """
        The client's ``encoding`` decides the bytes that are hashed, which is what makes a
        local digest comparable with the server's: the server hashes what it stored, and
        what it stored was encoded by this same encoder.
        """
        utf16_client = redis.Redis(encoding="utf-16", decode_responses=False)
        utf8_client = redis.Redis(encoding="utf-8", decode_responses=False)
        value = "hello"

        utf16_digest = utf16_client.digest_local(value)
        utf8_digest = utf8_client.digest_local(value)

        # The digest is a hex string, so the bytes carry no BOM and no multi-byte width
        # however the client encodes.
        assert isinstance(utf16_digest, bytes)
        assert len(utf16_digest) == 16
        assert utf16_digest.decode("ascii").isalnum()

        # Pre-encoded input passes through the encoder untouched, so hashing the utf-16
        # bytes on a utf-8 client must land on the same digest as hashing the string on a
        # utf-16 one. Both of these were the utf-8 digest before digest_local consulted
        # the encoder, which is what makes them the assertions that pin the behavior.
        assert utf16_digest != utf8_digest
        assert utf16_digest == utf8_client.digest_local(value.encode("utf-16"))
        assert utf8_digest == utf8_client.digest_local(value.encode("utf-8"))

    @skip_if_server_version_lt("8.3.224")
    def test_pipeline_digest(self, r):
        k1, k2 = "k:d1{42}", "k:d2{42}"
        r.mset({k1: b"A", k2: b"B"})
        p = r.pipeline()
        p.digest(k1)
        p.digest(k2)
        out = p.execute()
        assert len(out) == 2
        for d in out:
            if isinstance(d, bytes):
                d = d.decode()
            assert d is None or len(d) == 16

    def test_get_and_set(self, r):
        # get and set can't be tested independently of each other
        assert r.get("a") is None
        byte_string = b"value"
        integer = 5
        unicode_string = chr(3456) + "abcd" + chr(3421)
        assert r.set("byte_string", byte_string)
        assert r.set("integer", 5)
        assert r.set("unicode_string", unicode_string)
        assert r.get("byte_string") == byte_string
        assert r.get("integer") == str(integer).encode()
        assert r.get("unicode_string").decode("utf-8") == unicode_string

    @skip_if_server_version_lt("6.2.0")
    def test_getdel(self, r):
        assert r.getdel("a") is None
        r.set("a", 1)
        assert r.getdel("a") == b"1"
        assert r.getdel("a") is None

    @skip_if_server_version_lt("6.2.0")
    def test_getex(self, r):
        r.set("a", 1)
        assert r.getex("a") == b"1"
        assert r.ttl("a") == -1
        assert r.getex("a", ex=60) == b"1"
        assert r.ttl("a") == 60
        assert r.getex("a", px=6000) == b"1"
        assert r.ttl("a") == 6
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        assert r.getex("a", pxat=expire_at) == b"1"
        assert r.ttl("a") <= 61
        assert r.getex("a", persist=True) == b"1"
        assert r.ttl("a") == -1

    def test_getex_zero_expiry_options_are_mutually_exclusive(self, r):
        with pytest.raises(DataError):
            r.getex("a", ex=0, px=1)

    def test_getitem_and_setitem(self, r):
        r["a"] = "bar"
        assert r["a"] == b"bar"

    def test_getitem_raises_keyerror_for_missing_key(self, r):
        with pytest.raises(KeyError):
            r["a"]

    def test_getitem_does_not_raise_keyerror_for_empty_string(self, r):
        r["a"] = b""
        assert r["a"] == b""

    def test_get_set_bit(self, r):
        # no value
        assert not r.getbit("a", 5)
        # set bit 5
        assert not r.setbit("a", 5, True)
        assert r.getbit("a", 5)
        # unset bit 4
        assert not r.setbit("a", 4, False)
        assert not r.getbit("a", 4)
        # set bit 4
        assert not r.setbit("a", 4, True)
        assert r.getbit("a", 4)
        # set bit 5 again
        assert r.setbit("a", 5, True)
        assert r.getbit("a", 5)

    def test_getrange(self, r):
        r["a"] = "foo"
        assert r.getrange("a", 0, 0) == b"f"
        assert r.getrange("a", 0, 2) == b"foo"
        assert r.getrange("a", 3, 4) == b""

    def test_getset(self, r):
        assert r.getset("a", "foo") is None
        assert r.getset("a", "bar") == b"foo"
        assert r.get("a") == b"bar"

    def test_incr(self, r):
        assert r.incr("a") == 1
        assert r["a"] == b"1"
        assert r.incr("a") == 2
        assert r["a"] == b"2"
        assert r.incr("a", amount=5) == 7
        assert r["a"] == b"7"

    def test_incrby(self, r):
        assert r.incrby("a") == 1
        assert r.incrby("a", 4) == 5
        assert r["a"] == b"5"

    @skip_if_server_version_lt("2.6.0")
    def test_incrbyfloat(self, r):
        assert r.incrbyfloat("a") == 1.0
        assert r["a"] == b"1"
        assert r.incrbyfloat("a", 1.1) == 2.1
        assert float(r["a"]) == float(2.1)

    @skip_if_server_version_lt("8.7.0")
    def test_increx_default(self, r):
        key = "increx:default"
        assert r.set(key, 10)
        assert r.increx(key) == [11, 1]
        assert r[key] == b"11"

    @skip_if_server_version_lt("8.7.0")
    def test_increx_byint(self, r):
        key = "increx:byint"
        assert r.set(key, 20)
        assert r.increx(key, byint=4) == [24, 4]
        assert r[key] == b"24"

    @skip_if_server_version_lt("8.7.0")
    def test_increx_byfloat(self, r):
        key = "increx:float"
        assert r.set(key, "1.5")
        result = r.increx(key, byfloat=0.25, lbound=0, ubound=2)
        assert [float(value) for value in result] == pytest.approx([1.75, 0.25])
        assert float(r[key]) == pytest.approx(1.75)

    @pytest.mark.fixed_client
    @skip_if_server_version_lt("8.7.0")
    @pytest.mark.parametrize("protocol", [2, 3])
    def test_increx_unified_responses(self, request, protocol):
        r = _get_client(redis.Redis, request, protocol=protocol, legacy_responses=False)
        key = "increx:unified"
        assert r.set(key, "1.5")
        assert r.increx(key, byfloat=0.25) == [1.75, 0.25]
        # A rejected increment returns the current value and a zero increment.
        result = r.increx(key, byfloat=0.75, ubound=2)
        assert result == [1.75, 0.0]
        assert all(type(value) is float for value in result)
        assert r.set(key, 20)
        result = r.increx(key, byint=4)
        assert result == [24, 4]
        assert all(type(value) is int for value in result)

    @skip_if_server_version_lt("8.7.0")
    def test_increx_saturating_bounds(self, r):
        key = "increx:sat"
        assert r.set(key, "1.8")
        result = r.increx(key, byfloat=0.7, ubound=2, saturate=True)
        assert [float(value) for value in result] == pytest.approx([2.0, 0.2])
        assert float(r[key]) == pytest.approx(2.0)

    @skip_if_server_version_lt("8.7.0")
    def test_increx_overflow_rejected_keeps_value_and_ttl(self, r):
        key = "increx:overflow"
        assert r.set(key, 10)
        assert r.expire(key, 60)
        ttl = r.pttl(key)
        assert r.increx(key, byint=5, ubound=12) == [10, 0]
        assert r[key] == b"10"
        assert 0 < r.pttl(key) <= ttl

    @skip_if_server_version_lt("8.7.0")
    def test_increx_integer_overflow_uses_type_limits(self, r):
        key = "increx:type-overflow"
        max_int = 9223372036854775807
        almost_max = max_int - 1

        assert r.set(key, almost_max)
        assert r.expire(key, 60)
        ttl = r.pttl(key)
        assert r.increx(key, byint=2) == [almost_max, 0]
        assert r[key] == str(almost_max).encode()
        assert 0 < r.pttl(key) <= ttl

        assert r.set(key, almost_max)
        assert r.increx(key, byint=2, saturate=True) == [max_int, 1]
        assert r[key] == str(max_int).encode()

    @skip_if_server_version_lt("8.7.0")
    def test_increx_expiration_enx(self, r):
        key = "increx:expiration"
        assert r.set(key, 40)
        assert r.ttl(key) == -1

        assert r.increx(key, byint=2, ex=60, enx=True) == [42, 2]
        ttl = r.ttl(key)
        assert 0 < ttl <= 60

        assert r.increx(key, byint=3, ex=600, enx=True) == [45, 3]
        assert 0 < r.ttl(key) <= ttl

    def test_increx_invalid_options(self, r):
        key = "increx:invalid"
        assert r.set(key, 1)
        with pytest.raises(DataError):
            r.increx(key, byfloat=1.0, byint=1)
        with pytest.raises(DataError):
            r.increx(key, ex=10, px=10)
        with pytest.raises(DataError):
            r.increx(key, enx=True)

    def test_increx_zero_expiry_options_are_mutually_exclusive(self, r):
        with pytest.raises(DataError):
            r.increx("a", ex=0, px=1)

    @pytest.mark.onlynoncluster
    def test_keys(self, r):
        assert r.keys() == []
        keys_with_underscores = {b"test_a", b"test_b"}
        keys = keys_with_underscores.union({b"testc"})
        for key in keys:
            r[key] = 1
        assert set(r.keys(pattern="test_*")) == keys_with_underscores
        assert set(r.keys(pattern="test*")) == keys

    @pytest.mark.onlynoncluster
    def test_mget(self, r):
        assert r.mget([]) == []
        assert r.mget(["a", "b"]) == [None, None]
        r["a"] = "1"
        r["b"] = "2"
        r["c"] = "3"
        assert r.mget("a", "other", "b", "c") == [b"1", None, b"2", b"3"]

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    def test_lmove(self, r):
        r.rpush("a", "one", "two", "three", "four")
        assert r.lmove("a", "b")
        assert r.lmove("a", "b", "right", "left")

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    def test_blmove(self, r):
        r.rpush("a", "one", "two", "three", "four")
        assert r.blmove("a", "b", 5)
        assert r.blmove("a", "b", 1, "RIGHT", "LEFT")

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.9.0")
    def test_lmovem(self, r):
        r.rpush("a", "1", "2", "3", "4", "5")
        # single element (no count block), still an array reply
        assert r.lmovem("a", "b") == [b"1"]
        # COUNT with OBO ordering: pushed one-by-one -> reversed block order
        assert r.lmovem("a", "b", "LEFT", "LEFT", count=3, ordering="OBO") == [
            b"4",
            b"3",
            b"2",
        ]
        # up to count, fewer available; BULK preserves relative order
        assert r.lmovem("a", "b", "LEFT", "LEFT", count=5, ordering="BULK") == [b"5"]
        # empty source moves nothing
        assert r.lmovem("a", "b", count=2, ordering="BULK") is None
        # EXACTLY with too few elements moves nothing (nil reply, source untouched)
        r.rpush("names", "john")
        assert (
            r.lmovem(
                "names",
                "processed",
                "LEFT",
                "RIGHT",
                count=2,
                mode="EXACTLY",
                ordering="BULK",
            )
            is None
        )
        assert r.lrange("names", 0, -1) == [b"john"]
        # EXACTLY with enough elements, BULK preserves order
        r.rpush("names", "doe")
        assert r.lmovem(
            "names",
            "processed",
            "LEFT",
            "RIGHT",
            count=2,
            mode="EXACTLY",
            ordering="BULK",
        ) == [b"john", b"doe"]
        # ordering is mandatory whenever count is given (and vice versa)
        with pytest.raises(redis.DataError):
            r.lmovem("a", "b", count=2)
        with pytest.raises(redis.DataError):
            r.lmovem("a", "b", ordering="BULK")

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.9.0")
    def test_blmovem(self, r):
        r.rpush("a", "1", "2", "3", "4", "5")
        assert r.blmovem("a", "b", 1) == [b"1"]
        assert r.blmovem("a", "b", 1, "LEFT", "LEFT", count=3, ordering="BULK") == [
            b"2",
            b"3",
            b"4",
        ]
        # up to count, fewer available
        assert r.blmovem("a", "b", 1, count=5, ordering="BULK") == [b"5"]
        # timeout with empty source returns None
        assert r.blmovem("foo", "bar", 1, count=2, ordering="BULK") is None
        # ordering is mandatory whenever count is given
        with pytest.raises(redis.DataError):
            r.blmovem("a", "b", 1, count=2)

    @pytest.mark.onlynoncluster
    def test_mset(self, r):
        d = {"a": b"1", "b": b"2", "c": b"3"}
        assert r.mset(d)
        for k, v in d.items():
            assert r[k] == v

    @pytest.mark.onlycluster
    def test_mset_on_cluster(self, r):
        # validate that mset command works in cluster client
        # when the keys are in the same slot
        d = {"a:{test:1}": b"1", "b:{test:1}": b"2", "c:{test:1}": b"3"}
        assert r.mset(d)
        for k, v in d.items():
            assert r[k] == v

    @pytest.mark.onlycluster
    def test_mset_on_cluster_multiple_slots(self, r):
        d = {"a": b"1", "b": b"2", "c": b"3"}
        with pytest.raises(RedisClusterException):
            assert r.mset(d)

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("8.3.224")
    def test_msetex_no_expiration_with_cluster_client(self, r):
        all_test_keys = ["1:{test:1}", "2:{test:1}"]
        for key in all_test_keys:
            r.delete(key)

        # set items from mapping without expiration
        assert r.msetex(mapping={"1:{test:1}": 1, "2:{test:1}": b"four"}) == 1
        assert r.mget("1:{test:1}", "2:{test:1}") == [b"1", b"four"]
        assert r.ttl("1:{test:1}") == -1
        assert r.ttl("2:{test:1}") == -1

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("8.3.224")
    def test_msetex_expiration_ex_and_keepttl_with_cluster_client(self, r):
        all_test_keys = ["1:{test:1}", "2:{test:1}"]
        for key in all_test_keys:
            r.delete(key)

        # set items from mapping with expiration - testing ex field
        assert (
            r.msetex(
                mapping={"1:{test:1}": 1, "2:{test:1}": "2"},
                ex=10,
            )
            == 1
        )
        ttls = [r.ttl(key) for key in all_test_keys]
        for ttl in ttls:
            assert pytest.approx(ttl) == 10

        assert r.mget(*all_test_keys) == [b"1", b"2"]
        time.sleep(0.01)
        # validate keepttl: the TTL keeps counting down instead of being cleared
        assert r.msetex(mapping={"1:{test:1}": 11}, keepttl=True) == 1
        assert 0 < r.pttl("1:{test:1}") < 10000

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("8.3.224")
    def test_msetex_expiration_px_with_cluster_client(self, r):
        all_test_keys = ["1:{test:1}", "2:{test:1}"]
        for key in all_test_keys:
            r.delete(key)

        mapping = {"1:{test:1}": 1, "2:{test:1}": "2"}
        # set key/value pairs provided in mapping
        # with expiration - testing px field
        assert r.msetex(mapping=mapping, px=60000) == 1

        ttls = [r.ttl(key) for key in mapping.keys()]
        for ttl in ttls:
            assert pytest.approx(ttl) == 60
        assert r.mget(*mapping.keys()) == [b"1", b"2"]

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("8.3.224")
    def test_msetex_expiration_pxat_and_nx_with_cluster_client(self, r):
        all_test_keys = [
            "1:{test:1}",
            "2:{test:1}",
            "3:{test:1}",
            "new:{test:1}",
            "new_2:{test:1}",
        ]
        for key in all_test_keys:
            r.delete(key)

        mapping = {"1:{test:1}": 1, "2:{test:1}": "2", "3:{test:1}": "three"}
        assert r.msetex(mapping=mapping, ex=30) == 1

        # NX is set with existing keys - nothing should be saved or updated
        expire_at = redis_server_time(r) + datetime.timedelta(seconds=10)
        assert (
            r.msetex(
                mapping={"1:{test:1}": "new_value", "new:{test:1}": "ok"},
                pxat=expire_at,
                data_persist_option=DataPersistOptions.NX,
            )
            == 0
        )

        ttls = [r.ttl(key) for key in mapping.keys()]
        for ttl in ttls:
            assert 10 < ttl <= 30
        assert r.mget(*mapping.keys(), "new:{test:1}") == [b"1", b"2", b"three", None]

        # NX is set with non existing keys - values should be set
        assert (
            r.msetex(
                mapping={"new:{test:1}": "ok", "new_2:{test:1}": "ok_2"},
                pxat=expire_at,
                data_persist_option=DataPersistOptions.NX,
            )
            == 1
        )
        old_ttls = [r.ttl(key) for key in mapping.keys()]
        new_ttls = [r.ttl(key) for key in ["new:{test:1}", "new_2:{test:1}"]]
        for ttl in old_ttls:
            assert 10 < ttl <= 30
        for ttl in new_ttls:
            assert ttl <= 11
        assert r.mget(*mapping.keys(), "new:{test:1}", "new_2:{test:1}") == [
            b"1",
            b"2",
            b"three",
            b"ok",
            b"ok_2",
        ]

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("8.3.224")
    def test_msetex_expiration_exat_and_xx_with_cluster_client(self, r):
        all_test_keys = ["1:{test:1}", "2:{test:1}", "3:{test:1}", "new:{test:1}"]
        for key in all_test_keys:
            r.delete(key)

        mapping = {"1:{test:1}": 1, "2:{test:1}": "2", "3:{test:1}": "three"}
        assert r.msetex(mapping, ex=30) == 1

        expire_at = redis_server_time(r) + datetime.timedelta(seconds=10)
        ## XX is set with unexisting key - nothing should be saved or updated
        assert (
            r.msetex(
                mapping={"1:{test:1}": "new_value", "new:{test:1}": "ok"},
                exat=expire_at,
                data_persist_option=DataPersistOptions.XX,
            )
            == 0
        )
        ttls = [r.ttl(key) for key in mapping.keys()]
        for ttl in ttls:
            assert 10 < ttl <= 30
        assert r.mget(*mapping.keys(), "new:{test:1}") == [b"1", b"2", b"three", None]

        # XX is set with existing keys - values should be updated
        assert (
            r.msetex(
                mapping={"1:{test:1}": "new_value", "2:{test:1}": "new_value_2"},
                exat=expire_at,
                data_persist_option=DataPersistOptions.XX,
            )
            == 1
        )
        ttls = [r.ttl(key) for key in mapping.keys()]
        assert ttls[0] <= 11
        assert ttls[1] <= 11
        assert 10 < ttls[2] <= 30
        assert r.mget("1:{test:1}", "2:{test:1}", "3:{test:1}", "new:{test:1}") == [
            b"new_value",
            b"new_value_2",
            b"three",
            None,
        ]

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("8.3.224")
    def test_msetex_invalid_inputs_with_cluster_client(self, r):
        mapping = {"1": 1, "2": "2", "3": "three"}
        with pytest.raises(exceptions.RedisClusterException):
            r.msetex(mapping)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.3.224")
    def test_msetex_no_expiration(self, r):
        all_test_keys = ["1", "2"]
        for key in all_test_keys:
            r.delete(key)

        # # set items from mapping without expiration
        assert r.msetex(mapping={"1": 1, "2": b"four"}) == 1
        assert r.mget("1", "2") == [b"1", b"four"]
        assert r.ttl("1") == -1
        assert r.ttl("2") == -1

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.3.224")
    def test_msetex_expiration_ex_and_keepttl(self, r):
        all_test_keys = ["1", "2"]
        for key in all_test_keys:
            r.delete(key)

        # set items from mapping with expiration - testing ex field
        assert (
            r.msetex(
                mapping={"1": 1, "2": "2"},
                ex=10,
            )
            == 1
        )
        ttls = [r.ttl(key) for key in all_test_keys]
        for ttl in ttls:
            assert pytest.approx(ttl) == 10

        assert r.mget(*all_test_keys) == [b"1", b"2"]
        time.sleep(0.01)
        # validate keepttl: the TTL keeps counting down instead of being cleared
        assert r.msetex(mapping={"1": 11}, keepttl=True) == 1
        assert 0 < r.pttl("1") < 10000

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.3.224")
    def test_msetex_expiration_px(self, r):
        all_test_keys = ["1", "2"]
        for key in all_test_keys:
            r.delete(key)

        mapping = {"1": 1, "2": "2"}
        # set key/value pairs provided in mapping
        # with expiration - testing px field
        assert r.msetex(mapping=mapping, px=60000) == 1

        ttls = [r.ttl(key) for key in mapping.keys()]
        for ttl in ttls:
            assert pytest.approx(ttl) == 60
        assert r.mget(*mapping.keys()) == [b"1", b"2"]

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.3.224")
    def test_msetex_expiration_pxat_and_nx(self, r):
        all_test_keys = ["1", "2", "3", "new", "new_2"]
        for key in all_test_keys:
            r.delete(key)

        mapping = {"1": 1, "2": "2", "3": "three"}
        assert r.msetex(mapping=mapping, ex=30) == 1

        # NX is set with existing keys - nothing should be saved or updated
        expire_at = redis_server_time(r) + datetime.timedelta(seconds=10)
        assert (
            r.msetex(
                mapping={"1": "new_value", "new": "ok"},
                pxat=expire_at,
                data_persist_option=DataPersistOptions.NX,
            )
            == 0
        )
        ttls = [r.ttl(key) for key in mapping.keys()]
        for ttl in ttls:
            assert 10 < ttl <= 30
        assert r.mget(*mapping.keys(), "new") == [b"1", b"2", b"three", None]

        # NX is set with non existing keys - values should be set
        assert (
            r.msetex(
                mapping={"new": "ok", "new_2": "ok_2"},
                pxat=expire_at,
                data_persist_option=DataPersistOptions.NX,
            )
            == 1
        )
        old_ttls = [r.ttl(key) for key in mapping.keys()]
        new_ttls = [r.ttl(key) for key in ["new", "new_2"]]
        for ttl in old_ttls:
            assert 10 < ttl <= 30
        for ttl in new_ttls:
            assert ttl <= 11
        assert r.mget(*mapping.keys(), "new", "new_2") == [
            b"1",
            b"2",
            b"three",
            b"ok",
            b"ok_2",
        ]

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.3.224")
    def test_msetex_expiration_exat_and_xx(self, r):
        all_test_keys = ["1", "2", "3", "new"]
        for key in all_test_keys:
            r.delete(key)

        mapping = {"1": 1, "2": "2", "3": "three"}
        assert r.msetex(mapping, ex=30) == 1

        expire_at = redis_server_time(r) + datetime.timedelta(seconds=10)
        ## XX is set with unexisting key - nothing should be saved or updated
        assert (
            r.msetex(
                mapping={"1": "new_value", "new": "ok"},
                exat=expire_at,
                data_persist_option=DataPersistOptions.XX,
            )
            == 0
        )
        ttls = [r.ttl(key) for key in mapping.keys()]
        for ttl in ttls:
            assert 10 < ttl <= 30
        assert r.mget(*mapping.keys(), "new") == [b"1", b"2", b"three", None]

        # XX is set with existing keys - values should be updated
        assert (
            r.msetex(
                mapping={"1": "new_value", "2": "new_value_2"},
                exat=expire_at,
                data_persist_option=DataPersistOptions.XX,
            )
            == 1
        )
        ttls = [r.ttl(key) for key in mapping.keys()]
        assert ttls[0] <= 11
        assert ttls[1] <= 11
        assert 10 < ttls[2] <= 30
        assert r.mget("1", "2", "3", "new") == [
            b"new_value",
            b"new_value_2",
            b"three",
            None,
        ]

    @skip_if_server_version_lt("8.3.224")
    def test_msetex_invalid_inputs(self, r):
        mapping = {"1": 1, "2": "2"}
        with pytest.raises(exceptions.DataError):
            r.msetex(mapping, ex=10, keepttl=True)

    def test_msetex_zero_expiry_options_are_mutually_exclusive(self, r):
        with pytest.raises(DataError):
            r.msetex({"a": 1}, ex=0, px=1)

    @pytest.mark.onlynoncluster
    def test_msetnx(self, r):
        d = {"a": b"1", "b": b"2", "c": b"3"}
        assert r.msetnx(d)
        d2 = {"a": b"x", "d": b"4"}
        assert not r.msetnx(d2)
        for k, v in d.items():
            assert r[k] == v
        assert r.get("d") is None

    @skip_if_server_version_lt("2.6.0")
    def test_pexpire(self, r):
        assert r.pexpire("a", 60000) is False
        r["a"] = "foo"
        assert r.pexpire("a", 60000) is True
        assert 0 < r.pttl("a") <= 60000
        assert r.persist("a")
        assert r.pttl("a") == -1

    @skip_if_server_version_lt("7.0.0")
    def test_pexpire_option_nx(self, r):
        assert r.set("key", "val") is True
        assert r.pexpire("key", 60000, nx=True) is True
        assert r.pexpire("key", 60000, nx=True) is False

    @skip_if_server_version_lt("7.0.0")
    def test_pexpire_option_xx(self, r):
        assert r.set("key", "val") is True
        assert r.pexpire("key", 60000, xx=True) is False
        assert r.pexpire("key", 60000) is True
        assert r.pexpire("key", 70000, xx=True) is True

    @skip_if_server_version_lt("7.0.0")
    def test_pexpire_option_gt(self, r):
        assert r.set("key", "val") is True
        assert r.pexpire("key", 60000) is True
        assert r.pexpire("key", 70000, gt=True) is True
        assert r.pexpire("key", 50000, gt=True) is False

    @skip_if_server_version_lt("7.0.0")
    def test_pexpire_option_lt(self, r):
        assert r.set("key", "val") is True
        assert r.pexpire("key", 60000) is True
        assert r.pexpire("key", 50000, lt=True) is True
        assert r.pexpire("key", 70000, lt=True) is False

    @skip_if_server_version_lt("2.6.0")
    def test_pexpireat_datetime(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        r["a"] = "foo"
        assert r.pexpireat("a", expire_at) is True
        assert 0 < r.pttl("a") <= 61000

    @skip_if_server_version_lt("2.6.0")
    def test_pexpireat_no_key(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        assert r.pexpireat("a", expire_at) is False

    @skip_if_server_version_lt("2.6.0")
    def test_pexpireat_unixtime(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        r["a"] = "foo"
        expire_at_milliseconds = int(expire_at.timestamp() * 1000)
        assert r.pexpireat("a", expire_at_milliseconds) is True
        assert 0 < r.pttl("a") <= 61000

    @skip_if_server_version_lt("7.0.0")
    def test_pexpireat_option_nx(self, r):
        assert r.set("key", "val") is True
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        assert r.pexpireat("key", expire_at, nx=True) is True
        assert r.pexpireat("key", expire_at, nx=True) is False

    @skip_if_server_version_lt("7.0.0")
    def test_pexpireat_option_xx(self, r):
        assert r.set("key", "val") is True
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        assert r.pexpireat("key", expire_at, xx=True) is False
        assert r.pexpireat("key", expire_at) is True
        assert r.pexpireat("key", expire_at, xx=True) is True

    @skip_if_server_version_lt("7.0.0")
    def test_pexpireat_option_gt(self, r):
        assert r.set("key", "val") is True
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=2)
        assert r.pexpireat("key", expire_at) is True
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        assert r.pexpireat("key", expire_at, gt=True) is False
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=3)
        assert r.pexpireat("key", expire_at, gt=True) is True

    @skip_if_server_version_lt("7.0.0")
    def test_pexpireat_option_lt(self, r):
        assert r.set("key", "val") is True
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=2)
        assert r.pexpireat("key", expire_at) is True
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=3)
        assert r.pexpireat("key", expire_at, lt=True) is False
        expire_at = redis_server_time(r) + datetime.timedelta(minutes=1)
        assert r.pexpireat("key", expire_at, lt=True) is True

    @skip_if_server_version_lt("7.0.0")
    def test_pexpiretime(self, r):
        r.set("a", "foo")
        r.pexpireat("a", 33177117420000)
        assert r.pexpiretime("a") == 33177117420000

    @skip_if_server_version_lt("2.6.0")
    def test_psetex(self, r):
        assert r.psetex("a", 1000, "value")
        assert r["a"] == b"value"
        assert 0 < r.pttl("a") <= 1000

    @skip_if_server_version_lt("2.6.0")
    def test_psetex_timedelta(self, r):
        expire_at = datetime.timedelta(milliseconds=1000)
        assert r.psetex("a", expire_at, "value")
        assert r["a"] == b"value"
        assert 0 < r.pttl("a") <= 1000

    @skip_if_server_version_lt("2.6.0")
    def test_pttl(self, r):
        assert r.pexpire("a", 10000) is False
        r["a"] = "1"
        assert r.pexpire("a", 10000) is True
        assert 0 < r.pttl("a") <= 10000
        assert r.persist("a")
        assert r.pttl("a") == -1

    @skip_if_server_version_lt("2.8.0")
    def test_pttl_no_key(self, r):
        "PTTL on servers 2.8 and after return -2 when the key doesn't exist"
        assert r.pttl("a") == -2

    @skip_if_server_version_lt("6.2.0")
    def test_hrandfield(self, r):
        assert r.hrandfield("key") is None
        r.hset("key", mapping={"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})
        assert r.hrandfield("key") is not None
        assert len(r.hrandfield("key", 2)) == 2
        # with values
        assert_resp_response(r, len(r.hrandfield("key", 2, withvalues=True)), 4, 2)
        # without duplications
        assert len(r.hrandfield("key", 10)) == 5
        # with duplications
        assert len(r.hrandfield("key", -10)) == 10

    @pytest.mark.onlynoncluster
    def test_randomkey(self, r):
        assert r.randomkey() is None
        for key in ("a", "b", "c"):
            r[key] = 1
        assert r.randomkey() in (b"a", b"b", b"c")

    @pytest.mark.onlynoncluster
    def test_rename(self, r):
        r["a"] = "1"
        assert r.rename("a", "b")
        assert r.get("a") is None
        assert r["b"] == b"1"

    @pytest.mark.onlynoncluster
    def test_renamenx(self, r):
        r["a"] = "1"
        r["b"] = "2"
        assert not r.renamenx("a", "b")
        assert r["a"] == b"1"
        assert r["b"] == b"2"

    @skip_if_server_version_lt("2.6.0")
    def test_set_nx(self, r):
        assert r.set("a", "1", nx=True)
        assert not r.set("a", "2", nx=True)
        assert r["a"] == b"1"

    @skip_if_server_version_lt("2.6.0")
    def test_set_xx(self, r):
        assert not r.set("a", "1", xx=True)
        assert r.get("a") is None
        r["a"] = "bar"
        assert r.set("a", "2", xx=True)
        assert r.get("a") == b"2"

    @skip_if_server_version_lt("2.6.0")
    def test_set_px(self, r):
        assert r.set("a", "1", px=10000)
        assert r["a"] == b"1"
        assert 0 < r.pttl("a") <= 10000
        assert 0 < r.ttl("a") <= 10
        with pytest.raises(exceptions.DataError):
            assert r.set("a", "1", px=10.0)

    @skip_if_server_version_lt("2.6.0")
    def test_set_px_timedelta(self, r):
        expire_at = datetime.timedelta(milliseconds=1000)
        assert r.set("a", "1", px=expire_at)
        assert 0 < r.pttl("a") <= 1000
        assert 0 < r.ttl("a") <= 1

    @skip_if_server_version_lt("2.6.0")
    def test_set_ex(self, r):
        assert r.set("a", "1", ex=10)
        assert 0 < r.ttl("a") <= 10
        with pytest.raises(exceptions.DataError):
            assert r.set("a", "1", ex=10.0)

    def test_set_zero_expiry_options_are_mutually_exclusive(self, r):
        with pytest.raises(DataError):
            r.set("a", "1", ex=0, px=1)

    @skip_if_server_version_lt("2.6.0")
    def test_set_ex_str(self, r):
        assert r.set("a", "1", ex="10")
        assert 0 < r.ttl("a") <= 10
        with pytest.raises(exceptions.DataError):
            assert r.set("a", "1", ex="10.5")

    @skip_if_server_version_lt("2.6.0")
    def test_set_ex_timedelta(self, r):
        expire_at = datetime.timedelta(seconds=60)
        assert r.set("a", "1", ex=expire_at)
        assert 0 < r.ttl("a") <= 60

    @skip_if_server_version_lt("6.2.0")
    def test_set_exat_timedelta(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(seconds=10)
        assert r.set("a", "1", exat=expire_at)
        assert 0 < r.ttl("a") <= 10

    @skip_if_server_version_lt("6.2.0")
    def test_set_pxat_timedelta(self, r):
        expire_at = redis_server_time(r) + datetime.timedelta(seconds=50)
        assert r.set("a", "1", pxat=expire_at)
        assert 0 < r.ttl("a") <= 100

    @skip_if_server_version_lt("2.6.0")
    def test_set_multipleoptions(self, r):
        r["a"] = "val"
        assert r.set("a", "1", xx=True, px=10000)
        assert 0 < r.ttl("a") <= 10

    @skip_if_server_version_lt("6.0.0")
    def test_set_keepttl(self, r):
        r["a"] = "val"
        assert r.set("a", "1", xx=True, px=10000)
        assert 0 < r.ttl("a") <= 10
        r.set("a", "2", keepttl=True)
        assert r.get("a") == b"2"
        assert 0 < r.ttl("a") <= 10

    def test_set_empty_condition_is_mutually_exclusive(self, r):
        with pytest.raises(DataError):
            r.set("a", "1", nx=True, ifeq=b"")

    @skip_if_server_version_lt("8.3.224")
    def test_set_ifeq_true_sets_and_returns_true(self, r):
        r.delete("k")
        r.set("k", b"foo")
        assert r.set("k", b"bar", ifeq=b"foo") is True
        assert r.get("k") == b"bar"

    @skip_if_server_version_lt("8.3.224")
    def test_set_ifeq_false_does_not_set_returns_none(self, r):
        r.delete("k")
        r.set("k", b"foo")
        assert r.set("k", b"bar", ifeq=b"nope") is None
        assert r.get("k") == b"foo"

    @skip_if_server_version_lt("8.3.224")
    def test_set_ifne_true_sets(self, r):
        r.delete("k")
        r.set("k", b"foo")
        assert r.set("k", b"bar", ifne=b"zzz") is True
        assert r.get("k") == b"bar"

    @skip_if_server_version_lt("8.3.224")
    def test_set_ifne_false_does_not_set(self, r):
        r.delete("k")
        r.set("k", b"foo")
        assert r.set("k", b"bar", ifne=b"foo") is None
        assert r.get("k") == b"foo"

    @skip_if_server_version_lt("8.3.224")
    def test_set_ifeq_when_key_missing_does_not_create(self, r):
        r.delete("k")
        assert r.set("k", b"bar", ifeq=b"foo") is None
        assert r.exists("k") == 0

    @skip_if_server_version_lt("8.3.224")
    def test_set_ifne_when_key_missing_creates(self, r):
        r.delete("k")
        assert r.set("k", b"bar", ifne=b"foo") is True
        assert r.get("k") == b"bar"

    @skip_if_server_version_lt("8.3.224")
    @pytest.mark.parametrize("val", [b"", b"abc", b"The quick brown fox"])
    def test_set_ifdeq_and_ifdne(self, r, val):
        r.delete("k")
        r.set("k", val)
        d = self._server_xxh3_digest(r, "k")
        assert d is not None

        # sanity check: local digest matches server's
        assert d == self._ensure_str(r.digest_local(val))

        # IFDEQ must match to set; if key missing => won't create
        assert r.set("k", b"X", ifdeq=d) is True
        assert r.get("k") == b"X"

        r.delete("k")
        # key missing + IFDEQ => not created
        assert r.set("k", b"Y", ifdeq=d) is None
        assert r.exists("k") == 0

        # IFDNE: create when missing, and set when digest differs
        assert r.set("k", b"bar", ifdne=d) is True
        prev_d = self._server_xxh3_digest(r, "k")
        assert prev_d is not None
        # If digest equal → do not set
        assert r.set("k", b"zzz", ifdne=prev_d) is None
        assert r.get("k") == b"bar"

    @skip_if_server_version_lt("8.3.224")
    def test_set_with_get_returns_previous_value(self, r):
        r.delete("k")
        # when key didn’t exist → returns None, and key is created if condition allows it
        prev = r.set("k", b"v1", get=True, ifne=b"any")  # IFNE on missing creates
        assert prev is None
        # subsequent GET returns previous value, regardless of whether set occurs
        prev2 = r.set("k", b"v2", get=True, ifeq=b"v1")  # matches → set; returns "v1"
        assert prev2 == b"v1"
        prev3 = r.set("k", b"v3", get=True, ifeq=b"no")  # no set; returns previous "v2"
        assert prev3 == b"v2"
        assert r.get("k") == b"v2"

    @skip_if_server_version_lt("8.3.224")
    def test_set_mutual_exclusion_client_side(self, r):
        r.delete("k")
        with pytest.raises(DataError):
            r.set("k", b"v", nx=True, ifeq=b"x")
        with pytest.raises(DataError):
            r.set("k", b"v", ifdeq="aa", ifdne="bb")
        with pytest.raises(DataError):
            r.set("k", b"v", ex=1, px=1)
        with pytest.raises(DataError):
            r.set("k", b"v", exat=1, pxat=1)
        with pytest.raises(DataError):
            r.set("k", b"v", ex=1, exat=1)

    @skip_if_server_version_lt("6.2.0")
    def test_set_get(self, r):
        assert r.set("a", "True", get=True) is None
        assert r.set("a", "True", get=True) == b"True"
        assert r.set("a", "foo") is True
        assert r.set("a", "bar", get=True) == b"foo"
        assert r.get("a") == b"bar"

    def test_setex(self, r):
        assert r.setex("a", 60, "1")
        assert r["a"] == b"1"
        assert 0 < r.ttl("a") <= 60

    def test_setnx(self, r):
        assert r.setnx("a", "1")
        assert r["a"] == b"1"
        assert not r.setnx("a", "2")
        assert r["a"] == b"1"

    def test_setrange(self, r):
        assert r.setrange("a", 5, "foo") == 8
        assert r["a"] == b"\0\0\0\0\0foo"
        r["a"] = "abcdefghijh"
        assert r.setrange("a", 6, "12345") == 11
        assert r["a"] == b"abcdef12345"

    @skip_if_server_version_lt("6.0.0")
    @skip_if_server_version_gte("7.0.0")
    @skip_if_redis_enterprise()
    def test_stralgo_lcs(self, r):
        key1 = "{foo}key1"
        key2 = "{foo}key2"
        value1 = "ohmytext"
        value2 = "mynewtext"
        res = "mytext"

        if skip_if_redis_enterprise().args[0] is True:
            with pytest.raises(redis.exceptions.ResponseError):
                assert r.stralgo("LCS", value1, value2) == res
            return

        # test LCS of strings
        assert r.stralgo("LCS", value1, value2) == res
        # test using keys
        r.mset({key1: value1, key2: value2})
        assert r.stralgo("LCS", key1, key2, specific_argument="keys") == res
        # test other labels
        assert r.stralgo("LCS", value1, value2, len=True) == len(res)
        assert_resp_response(
            r,
            r.stralgo("LCS", value1, value2, idx=True),
            {"len": len(res), "matches": [[(4, 7), (5, 8)], [(2, 3), (0, 1)]]},
            {"len": len(res), "matches": [[[4, 7], [5, 8]], [[2, 3], [0, 1]]]},
            {"len": len(res), "matches": [[[4, 7], [5, 8]], [[2, 3], [0, 1]]]},
        )
        assert_resp_response(
            r,
            r.stralgo("LCS", value1, value2, idx=True, withmatchlen=True),
            {"len": len(res), "matches": [[4, (4, 7), (5, 8)], [2, (2, 3), (0, 1)]]},
            {"len": len(res), "matches": [[[4, 7], [5, 8], 4], [[2, 3], [0, 1], 2]]},
            {"len": len(res), "matches": [[4, [4, 7], [5, 8]], [2, [2, 3], [0, 1]]]},
        )
        assert_resp_response(
            r,
            r.stralgo(
                "LCS", value1, value2, idx=True, withmatchlen=True, minmatchlen=4
            ),
            {"len": len(res), "matches": [[4, (4, 7), (5, 8)]]},
            {"len": len(res), "matches": [[[4, 7], [5, 8], 4]]},
            {"len": len(res), "matches": [[4, [4, 7], [5, 8]]]},
        )

    @skip_if_server_version_lt("6.0.0")
    @skip_if_server_version_gte("7.0.0")
    @skip_if_redis_enterprise()
    def test_stralgo_negative(self, r):
        with pytest.raises(exceptions.DataError):
            r.stralgo("ISSUB", "value1", "value2")
        with pytest.raises(exceptions.DataError):
            r.stralgo("LCS", "value1", "value2", len=True, idx=True)
        with pytest.raises(exceptions.DataError):
            r.stralgo("LCS", "value1", "value2", specific_argument="INT")
        with pytest.raises(ValueError):
            r.stralgo("LCS", "value1", "value2", idx=True, minmatchlen="one")

    def test_strlen(self, r):
        r["a"] = "foo"
        assert r.strlen("a") == 3

    def test_substr(self, r):
        r["a"] = "0123456789"

        if skip_if_redis_enterprise().args[0] is True:
            with pytest.raises(redis.exceptions.ResponseError):
                assert r.substr("a", 0) == b"0123456789"
            return

        assert r.substr("a", 0) == b"0123456789"
        assert r.substr("a", 2) == b"23456789"
        assert r.substr("a", 3, 5) == b"345"
        assert r.substr("a", 3, -2) == b"345678"

    def generate_lib_code(self, lib_name):
        return f"""#!js api_version=1.0 name={lib_name}\n redis.registerFunction('foo', ()=>{{return 'bar'}})"""  # noqa

    def try_delete_libs(self, r, *lib_names):
        for lib_name in lib_names:
            try:
                r.tfunction_delete(lib_name)
            except Exception:
                pass

    def test_ttl(self, r):
        r["a"] = "1"
        assert r.expire("a", 10)
        assert 0 < r.ttl("a") <= 10
        assert r.persist("a")
        assert r.ttl("a") == -1

    @skip_if_server_version_lt("2.8.0")
    def test_ttl_nokey(self, r):
        "TTL on servers 2.8 and after return -2 when the key doesn't exist"
        assert r.ttl("a") == -2

    def test_type(self, r):
        assert r.type("a") == b"none"
        r["a"] = "1"
        assert r.type("a") == b"string"
        del r["a"]
        r.lpush("a", "1")
        assert r.type("a") == b"list"
        del r["a"]
        r.sadd("a", "1")
        assert r.type("a") == b"set"
        del r["a"]
        r.zadd("a", {"1": 1})
        assert r.type("a") == b"zset"

    # LIST COMMANDS
    @pytest.mark.onlynoncluster
    def test_blpop(self, r):
        r.rpush("a", "1", "2")
        r.rpush("b", "3", "4")
        assert_resp_response(
            r, r.blpop(["b", "a"], timeout=1), (b"b", b"3"), [b"b", b"3"]
        )
        assert_resp_response(
            r, r.blpop(["b", "a"], timeout=1), (b"b", b"4"), [b"b", b"4"]
        )
        assert_resp_response(
            r, r.blpop(["b", "a"], timeout=1), (b"a", b"1"), [b"a", b"1"]
        )
        assert_resp_response(
            r, r.blpop(["b", "a"], timeout=1), (b"a", b"2"), [b"a", b"2"]
        )
        assert r.blpop(["b", "a"], timeout=0.1) is None
        r.rpush("c", "1")
        assert_resp_response(r, r.blpop("c", timeout=1), (b"c", b"1"), [b"c", b"1"])

    @pytest.mark.onlynoncluster
    def test_brpop(self, r):
        r.rpush("a", "1", "2")
        r.rpush("b", "3", "4")
        assert_resp_response(
            r, r.brpop(["b", "a"], timeout=1), (b"b", b"4"), [b"b", b"4"]
        )
        assert_resp_response(
            r, r.brpop(["b", "a"], timeout=1), (b"b", b"3"), [b"b", b"3"]
        )
        assert_resp_response(
            r, r.brpop(["b", "a"], timeout=1), (b"a", b"2"), [b"a", b"2"]
        )
        assert_resp_response(
            r, r.brpop(["b", "a"], timeout=1), (b"a", b"1"), [b"a", b"1"]
        )
        assert r.brpop(["b", "a"], timeout=0.1) is None
        r.rpush("c", "1")
        assert_resp_response(r, r.brpop("c", timeout=1), (b"c", b"1"), [b"c", b"1"])

    @pytest.mark.onlynoncluster
    def test_brpoplpush(self, r):
        r.rpush("a", "1", "2")
        r.rpush("b", "3", "4")
        assert r.brpoplpush("a", "b") == b"2"
        assert r.brpoplpush("a", "b") == b"1"
        assert r.brpoplpush("a", "b", timeout=0.1) is None
        assert r.lrange("a", 0, -1) == []
        assert r.lrange("b", 0, -1) == [b"1", b"2", b"3", b"4"]

    @pytest.mark.onlynoncluster
    def test_brpoplpush_empty_string(self, r):
        r.rpush("a", "")
        assert r.brpoplpush("a", "b") == b""

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_blmpop(self, r):
        r.rpush("a", "1", "2", "3", "4", "5")
        res = [b"a", [b"1", b"2"]]
        assert r.blmpop(1, "2", "b", "a", direction="LEFT", count=2) == res
        with pytest.raises(TypeError):
            r.blmpop(1, "2", "b", "a", count=2)
        r.rpush("b", "6", "7", "8", "9")
        assert r.blmpop(0, "2", "b", "a", direction="LEFT") == [b"b", [b"6"]]
        assert r.blmpop(0.1, "2", "foo", "bar", direction="RIGHT") is None

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_lmpop(self, r):
        r.rpush("foo", "1", "2", "3", "4", "5")
        result = [b"foo", [b"1", b"2"]]
        assert r.lmpop("2", "bar", "foo", direction="LEFT", count=2) == result
        with pytest.raises(redis.ResponseError):
            r.lmpop("2", "bar", "foo", direction="up", count=2)
        r.rpush("bar", "a", "b", "c", "d")
        assert r.lmpop("2", "bar", "foo", direction="LEFT") == [b"bar", [b"a"]]

    def test_lindex(self, r):
        r.rpush("a", "1", "2", "3")
        assert r.lindex("a", "0") == b"1"
        assert r.lindex("a", "1") == b"2"
        assert r.lindex("a", "2") == b"3"

    def test_linsert(self, r):
        r.rpush("a", "1", "2", "3")
        assert r.linsert("a", "after", "2", "2.5") == 4
        assert r.lrange("a", 0, -1) == [b"1", b"2", b"2.5", b"3"]
        assert r.linsert("a", "before", "2", "1.5") == 5
        assert r.lrange("a", 0, -1) == [b"1", b"1.5", b"2", b"2.5", b"3"]

    def test_llen(self, r):
        r.rpush("a", "1", "2", "3")
        assert r.llen("a") == 3

    def test_lpop(self, r):
        r.rpush("a", "1", "2", "3")
        assert r.lpop("a") == b"1"
        assert r.lpop("a") == b"2"
        assert r.lpop("a") == b"3"
        assert r.lpop("a") is None

    @skip_if_server_version_lt("6.2.0")
    def test_lpop_count(self, r):
        r.rpush("a", "1", "2", "3")
        assert r.lpop("a", 2) == [b"1", b"2"]
        assert r.lpop("a", 1) == [b"3"]
        assert r.lpop("a") is None
        assert r.lpop("a", 3) is None

    def test_lpush(self, r):
        assert r.lpush("a", "1") == 1
        assert r.lpush("a", "2") == 2
        assert r.lpush("a", "3", "4") == 4
        assert r.lrange("a", 0, -1) == [b"4", b"3", b"2", b"1"]

    def test_lpushx(self, r):
        assert r.lpushx("a", "1") == 0
        assert r.lrange("a", 0, -1) == []
        r.rpush("a", "1", "2", "3")
        assert r.lpushx("a", "4") == 4
        assert r.lrange("a", 0, -1) == [b"4", b"1", b"2", b"3"]

    @skip_if_server_version_lt("4.0.0")
    def test_lpushx_with_list(self, r):
        # now with a list
        r.lpush("somekey", "a")
        r.lpush("somekey", "b")
        assert r.lpushx("somekey", "foo", "asdasd", 55, "asdasdas") == 6
        res = r.lrange("somekey", 0, -1)
        assert res == [b"asdasdas", b"55", b"asdasd", b"foo", b"b", b"a"]

    def test_lrange(self, r):
        r.rpush("a", "1", "2", "3", "4", "5")
        assert r.lrange("a", 0, 2) == [b"1", b"2", b"3"]
        assert r.lrange("a", 2, 10) == [b"3", b"4", b"5"]
        assert r.lrange("a", 0, -1) == [b"1", b"2", b"3", b"4", b"5"]
        r.rpush(b"345", "12", "22", "32", "42", "52")
        assert r.lrange(b"345", 0, 0) == [b"12"]

    def test_lrem(self, r):
        r.rpush("a", "Z", "b", "Z", "Z", "c", "Z", "Z")
        # remove the first 'Z'  item
        assert r.lrem("a", 1, "Z") == 1
        assert r.lrange("a", 0, -1) == [b"b", b"Z", b"Z", b"c", b"Z", b"Z"]
        # remove the last 2 'Z' items
        assert r.lrem("a", -2, "Z") == 2
        assert r.lrange("a", 0, -1) == [b"b", b"Z", b"Z", b"c"]
        # remove all 'Z' items
        assert r.lrem("a", 0, "Z") == 2
        assert r.lrange("a", 0, -1) == [b"b", b"c"]

    def test_lset(self, r):
        r.rpush("a", "1", "2", "3")
        assert r.lrange("a", 0, -1) == [b"1", b"2", b"3"]
        assert r.lset("a", 1, "4")
        assert r.lrange("a", 0, 2) == [b"1", b"4", b"3"]

    @skip_if_server_version_lt("8.7.2")
    def test_arset_at_index_zero(self, r):
        assert r.arset("a", 0, "v0") == 1

    @skip_if_server_version_lt("8.7.2")
    def test_arset_at_positive_index(self, r):
        assert r.arset("a", 1, "v1") == 1

    @skip_if_server_version_lt("8.7.2")
    def test_arset_multiple_values(self, r):
        assert r.arset("a", 0, "v0", "v1", "v2") == 3

    @skip_if_server_version_lt("8.7.2")
    def test_arset_negative_index(self, r):
        r.arset("a", 0, "v0", "v1", "v2")
        with pytest.raises(redis.ResponseError):
            r.arset("a", -1, "vlast")

    @skip_if_server_version_lt("8.7.2")
    def test_armset_sets_values_at_correct_indices(self, r):
        assert r.armset("a", {0: "v0", 2: "v2", 4: "v4"}) == 3
        assert r.arget("a", 0) == b"v0"
        assert r.arget("a", 1) is None
        assert r.arget("a", 2) == b"v2"
        assert r.arget("a", 3) is None
        assert r.arget("a", 4) == b"v4"

    @skip_if_server_version_lt("8.7.2")
    def test_armset_single_pair(self, r):
        assert r.armset("a", {3: "v3"}) == 1
        assert r.arget("a", 3) == b"v3"

    @skip_if_server_version_lt("8.7.2")
    def test_armset_overwrites_existing_indices(self, r):
        r.arset("a", 0, "v0", "v1", "v2")
        assert r.armset("a", {0: "new0", 1: "new1", 2: "new2"}) == 0
        assert r.arget("a", 0) == b"new0"
        assert r.arget("a", 1) == b"new1"
        assert r.arget("a", 2) == b"new2"

    @skip_if_server_version_lt("8.7.2")
    def test_armset_mixed_new_and_existing_indices(self, r):
        r.arset("a", 0, "v0", "v1")
        assert r.armset("a", {0: "new0", 2: "v2", 3: "v3"}) == 2
        assert r.arget("a", 0) == b"new0"
        assert r.arget("a", 1) == b"v1"
        assert r.arget("a", 2) == b"v2"
        assert r.arget("a", 3) == b"v3"

    @skip_if_server_version_lt("8.7.2")
    def test_arget_returns_value_at_index(self, r):
        r.arset("a", 0, "v0", "v1", "v2")
        assert r.arget("a", 0) == b"v0"
        assert r.arget("a", 1) == b"v1"
        assert r.arget("a", 2) == b"v2"

    @skip_if_server_version_lt("8.7.2")
    def test_arget_missing_key_returns_none(self, r):
        assert r.arget("a", 0) is None

    @skip_if_server_version_lt("8.7.2")
    def test_arget_missing_index_returns_none(self, r):
        r.arset("a", 0, "v0")
        assert r.arget("a", 5) is None

    @skip_if_server_version_lt("8.7.2")
    def test_armget_returns_values_at_indices(self, r):
        r.arset("a", 0, "v0", "v1", "v2")
        assert r.armget("a", 0, 1, 2) == [b"v0", b"v1", b"v2"]

    @skip_if_server_version_lt("8.7.2")
    def test_armget_preserves_requested_order(self, r):
        r.arset("a", 0, "v0", "v1", "v2")
        assert r.armget("a", 2, 0, 1) == [b"v2", b"v0", b"v1"]

    @skip_if_server_version_lt("8.7.2")
    def test_armget_missing_key_returns_nones(self, r):
        assert r.armget("a", 0, 2, 5) == [None, None, None]

    @skip_if_server_version_lt("8.7.2")
    def test_armget_missing_indices_return_nones(self, r):
        r.arset("a", 0, "v0", "v1")
        assert r.armget("a", 0, 5, 1, 10) == [b"v0", None, b"v1", None]

    @skip_if_server_version_lt("8.7.2")
    def test_armget_single_index(self, r):
        r.arset("a", 0, "v0", "v1")
        assert r.armget("a", 1) == [b"v1"]

    @skip_if_server_version_lt("8.7.2")
    def test_argetrange_returns_array_slice(self, r):
        r.arset("a", 0, "v0", "v1", "v2", "v3", "v4")
        assert r.argetrange("a", 1, 3) == [b"v1", b"v2", b"v3"]

    @skip_if_server_version_lt("8.7.2")
    def test_argetrange_full_range(self, r):
        r.arset("a", 0, "v0", "v1", "v2")
        assert r.argetrange("a", 0, 2) == [b"v0", b"v1", b"v2"]

    @skip_if_server_version_lt("8.7.2")
    def test_argetrange_reversed_range(self, r):
        r.arset("a", 0, "v0", "v1", "v2", "v3", "v4")
        assert r.argetrange("a", 3, 1) == [b"v3", b"v2", b"v1"]

    @skip_if_server_version_lt("8.7.2")
    def test_argetrange_single_index(self, r):
        r.arset("a", 0, "v0", "v1", "v2")
        assert r.argetrange("a", 1, 1) == [b"v1"]

    @skip_if_server_version_lt("8.7.2")
    def test_argetrange_includes_empty_indices(self, r):
        r.arset("a", 0, "v0", "v1", "v2")
        r.ardel("a", 1)
        assert r.argetrange("a", 0, 2) == [b"v0", None, b"v2"]

    @skip_if_server_version_lt("8.7.2")
    def test_argetrange_missing_key(self, r):
        assert r.argetrange("a", 0, 4) == [None] * 5

    @skip_if_server_version_lt("8.7.2")
    def test_argetrange_out_of_bounds(self, r):
        r.arset("a", 0, "v0", "v1", "v2")
        assert r.argetrange("a", 10, 20) == [None] * 11

    @skip_if_server_version_lt("8.7.2")
    def test_arscan_returns_index_value_pairs(self, r):
        r.arset("a", 0, "v0", "v1", "v2")
        assert r.arscan("a", 0, 2) == [[0, b"v0"], [1, b"v1"], [2, b"v2"]]

    @skip_if_server_version_lt("8.7.2")
    def test_arscan_skips_empty_slots(self, r):
        r.armset("a", {0: "a", 5: "b", 9: "c"})
        assert r.arscan("a", 0, 10) == [[0, b"a"], [5, b"b"], [9, b"c"]]

    @skip_if_server_version_lt("8.7.2")
    def test_arscan_reverse(self, r):
        r.armset("a", {0: "a", 5: "b", 9: "c"})
        assert r.arscan("a", 10, 0) == [[9, b"c"], [5, b"b"], [0, b"a"]]

    @skip_if_server_version_lt("8.7.2")
    def test_arscan_with_limit(self, r):
        r.armset("a", {0: "a", 5: "b", 9: "c"})
        assert r.arscan("a", 0, 10, limit=2) == [[0, b"a"], [5, b"b"]]

    @skip_if_server_version_lt("8.7.2")
    def test_arscan_missing_key_returns_empty(self, r):
        assert r.arscan("a", 0, 10) == []

    @skip_if_server_version_lt("8.7.2")
    def test_argrep_exact_match(self, r):
        r.armset("a", {0: "boot", 1: "warn", 2: "error", 3: "boot"})
        assert r.argrep("a", 0, 3, [(ArrayPredicateType.EXACT, "boot")]) == [0, 3]

    @skip_if_server_version_lt("8.7.2")
    def test_argrep_substring_match(self, r):
        r.armset("a", {0: "boot: ok", 1: "warn: disk", 2: "ERROR: cpu"})
        assert r.argrep("a", 0, 2, [(ArrayPredicateType.MATCH, "warn")]) == [1]

    @skip_if_server_version_lt("8.7.2")
    def test_argrep_glob_match(self, r):
        r.armset("a", {0: "warn:disk", 1: "info:ok", 2: "warn:net"})
        result = r.argrep("a", 0, 2, [(ArrayPredicateType.GLOB, "warn:*")])
        assert result == [0, 2]

    @skip_if_server_version_lt("8.7.2")
    def test_argrep_regex_match(self, r):
        r.armset("a", {0: "boot: ok", 1: "ERROR: cpu", 2: "error: net"})
        result = r.argrep(
            "a",
            0,
            2,
            [(ArrayPredicateType.RE, r"^[A-Za-z]+: (cpu|net)$")],
            nocase=True,
        )
        assert result == [1, 2]

    @skip_if_server_version_lt("8.7.2")
    def test_argrep_nocase(self, r):
        r.armset("a", {0: "ERROR", 1: "info", 2: "Error"})
        assert r.argrep(
            "a", 0, 2, [(ArrayPredicateType.MATCH, "error")], nocase=True
        ) == [0, 2]

    @skip_if_server_version_lt("8.7.2")
    def test_argrep_withvalues(self, r):
        r.armset("a", {0: "ERROR: cpu", 1: "info", 2: "error: net"})
        result = r.argrep(
            "a",
            0,
            2,
            [(ArrayPredicateType.MATCH, "error")],
            withvalues=True,
            nocase=True,
        )
        assert result == [[0, b"ERROR: cpu"], [2, b"error: net"]]

    @skip_if_server_version_lt("8.7.2")
    def test_argrep_limit(self, r):
        r.armset("a", {0: "ERROR", 1: "info", 2: "Error"})
        result = r.argrep(
            "a",
            0,
            2,
            [(ArrayPredicateType.MATCH, "error")],
            limit=1,
            nocase=True,
        )
        assert result == [0]

    @skip_if_server_version_lt("8.7.2")
    def test_argrep_combinator_and(self, r):
        r.armset("a", {0: "warn: ok", 1: "error: ok", 2: "warn: bad"})
        result = r.argrep(
            "a",
            0,
            2,
            [
                (ArrayPredicateType.MATCH, "warn"),
                (ArrayPredicateType.MATCH, "ok"),
            ],
            combinator=ArrayPredicateCombinator.AND,
        )
        assert result == [0]

    @skip_if_server_version_lt("8.7.2")
    def test_argrep_combinator_or(self, r):
        r.armset("a", {0: "warn", 1: "info", 2: "error"})
        result = r.argrep(
            "a",
            0,
            2,
            [
                (ArrayPredicateType.EXACT, "warn"),
                (ArrayPredicateType.EXACT, "error"),
            ],
            combinator=ArrayPredicateCombinator.OR,
        )
        assert result == [0, 2]

    @skip_if_server_version_lt("8.7.2")
    def test_argrep_missing_key(self, r):
        assert r.argrep("a", 0, 10, [(ArrayPredicateType.MATCH, "x")]) == []

    @skip_if_server_version_lt("8.7.2")
    def test_argrep_reverse(self, r):
        r.armset("a", {0: "warn", 1: "info", 2: "warn"})
        assert r.argrep("a", 2, 0, [(ArrayPredicateType.EXACT, "warn")]) == [2, 0]

    @skip_if_server_version_lt("8.7.2")
    def test_ardel_single_index(self, r):
        r.arset("a", 0, "v0", "v1", "v2")
        assert r.ardel("a", 1) == 1
        assert r.arget("a", 1) is None
        assert r.arget("a", 0) == b"v0"
        assert r.arget("a", 2) == b"v2"

    @skip_if_server_version_lt("8.7.2")
    def test_ardel_multiple_indices(self, r):
        r.arset("a", 0, "v0", "v1", "v2", "v3")
        assert r.ardel("a", 0, 2, 3) == 3
        assert r.arget("a", 0) is None
        assert r.arget("a", 1) == b"v1"
        assert r.arget("a", 2) is None
        assert r.arget("a", 3) is None

    @skip_if_server_version_lt("8.7.2")
    def test_ardel_non_existing_index(self, r):
        r.arset("a", 0, "v0", "v1")
        assert r.ardel("a", 10) == 0
        assert r.arget("a", 0) == b"v0"
        assert r.arget("a", 1) == b"v1"

    @skip_if_server_version_lt("8.7.2")
    def test_ardel_mixed_existing_and_missing(self, r):
        r.arset("a", 0, "v0", "v1", "v2")
        assert r.ardel("a", 0, 10, 2) == 2
        assert r.arget("a", 0) is None
        assert r.arget("a", 1) == b"v1"
        assert r.arget("a", 2) is None

    @skip_if_server_version_lt("8.7.2")
    def test_ardelrange_single_range(self, r):
        r.arset("a", 0, "v0", "v1", "v2", "v3", "v4")
        assert r.ardelrange("a", (1, 3)) == 3
        assert r.arget("a", 0) == b"v0"
        assert r.arget("a", 1) is None
        assert r.arget("a", 2) is None
        assert r.arget("a", 3) is None
        assert r.arget("a", 4) == b"v4"

    @skip_if_server_version_lt("8.7.2")
    def test_ardelrange_multiple_ranges(self, r):
        r.arset("a", 0, "v0", "v1", "v2", "v3", "v4", "v5")
        assert r.ardelrange("a", (0, 1), (4, 5)) == 4
        assert r.arget("a", 0) is None
        assert r.arget("a", 1) is None
        assert r.arget("a", 2) == b"v2"
        assert r.arget("a", 3) == b"v3"
        assert r.arget("a", 4) is None
        assert r.arget("a", 5) is None

    @skip_if_server_version_lt("8.7.2")
    def test_ardelrange_reversed_range(self, r):
        r.arset("a", 0, "v0", "v1", "v2", "v3", "v4")
        assert r.ardelrange("a", (3, 1)) == 3
        assert r.arget("a", 0) == b"v0"
        assert r.arget("a", 1) is None
        assert r.arget("a", 2) is None
        assert r.arget("a", 3) is None
        assert r.arget("a", 4) == b"v4"

    @skip_if_server_version_lt("8.7.2")
    def test_ardelrange_overlapping_ranges(self, r):
        r.arset("a", 0, "v0", "v1", "v2", "v3", "v4")
        assert r.ardelrange("a", (0, 2), (1, 3)) == 4
        assert r.arget("a", 0) is None
        assert r.arget("a", 1) is None
        assert r.arget("a", 2) is None
        assert r.arget("a", 3) is None
        assert r.arget("a", 4) == b"v4"

    @skip_if_server_version_lt("8.7.2")
    def test_ardelrange_missing_key(self, r):
        assert r.ardelrange("a", (0, 4)) == 0

    @skip_if_server_version_lt("8.7.2")
    def test_ardelrange_empty_indices(self, r):
        r.arset("a", 0, "v0")
        assert r.ardelrange("a", (5, 10)) == 0
        assert r.arget("a", 0) == b"v0"

    @skip_if_server_version_lt("8.7.2")
    def test_arcount_missing_key(self, r):
        assert r.arcount("a") == 0

    @skip_if_server_version_lt("8.7.2")
    def test_arcount_returns_number_of_elements(self, r):
        r.arset("a", 0, "v0", "v1", "v2")
        assert r.arcount("a") == 3

    @skip_if_server_version_lt("8.7.2")
    def test_arcount_after_delete(self, r):
        r.arset("a", 0, "v0", "v1", "v2", "v3")
        r.ardel("a", 1, 3)
        assert r.arcount("a") == 2

    @skip_if_server_version_lt("8.7.2")
    def test_arlen_missing_key(self, r):
        assert r.arlen("a") == 0

    @skip_if_server_version_lt("8.7.2")
    def test_arlen_returns_length(self, r):
        r.arset("a", 0, "v0", "v1", "v2")
        assert r.arlen("a") == 3

    @skip_if_server_version_lt("8.7.2")
    def test_arlen_uses_max_index_plus_one(self, r):
        r.arset("a", 7, "v7")
        assert r.arlen("a") == 8

    @skip_if_server_version_lt("8.7.2")
    def test_arlen_unchanged_after_delete(self, r):
        r.arset("a", 0, "v0", "v1", "v2")
        r.ardel("a", 1)
        assert r.arlen("a") == 3

    @skip_if_server_version_lt("8.7.2")
    def test_arinfo_returns_basic_fields(self, r):
        r.armset("a", {0: "x", 1: "y", 100: "z"})
        info = r.arinfo("a")
        assert isinstance(info, dict)
        assert info["count"] == 3
        assert info["len"] == 101
        for field in ("next-insert-index", "slices", "slice-size"):
            assert field in info

    @skip_if_server_version_lt("8.7.2")
    def test_arinfo_full_includes_slice_stats(self, r):
        r.armset("a", {0: "x", 1: "y", 100: "z"})
        info = r.arinfo("a", full=True)
        for field in (
            "dense-slices",
            "sparse-slices",
            "avg-dense-size",
            "avg-dense-fill",
            "avg-sparse-size",
        ):
            assert field in info

    @skip_if_server_version_lt("8.7.2")
    def test_arinfo_missing_key_raises(self, r):
        with pytest.raises(redis.ResponseError):
            r.arinfo("a")

    @skip_if_server_version_lt("8.7.2")
    def test_arnext_missing_key(self, r):
        assert r.arnext("a") == 0

    @skip_if_server_version_lt("8.7.2")
    def test_arnext_returns_next_index(self, r):
        r.arinsert("a", "v0", "v1", "v2")
        assert r.arnext("a") == 3

    @skip_if_server_version_lt("8.7.2")
    def test_arnext_after_delete(self, r):
        r.arinsert("a", "v0", "v1", "v2")
        r.ardel("a", 1)
        assert r.arnext("a") == 3

    @skip_if_server_version_lt("8.7.2")
    def test_arseek_missing_key_returns_zero(self, r):
        assert r.arseek("a", 5) == 0

    @skip_if_server_version_lt("8.7.2")
    def test_arseek_repositions_insert_cursor(self, r):
        r.arinsert("a", "v0")
        assert r.arseek("a", 10) == 1
        assert r.arnext("a") == 10
        r.arinsert("a", "v10")
        assert r.arget("a", 10) == b"v10"

    @skip_if_server_version_lt("8.7.2")
    def test_arseek_followed_by_arnext(self, r):
        r.arinsert("a", "v0", "v1")
        assert r.arseek("a", 7) == 1
        assert r.arnext("a") == 7

    @skip_if_server_version_lt("8.7.2")
    def test_arinsert_single_value(self, r):
        assert r.arinsert("a", "v0") == 0
        assert r.arget("a", 0) == b"v0"

    @skip_if_server_version_lt("8.7.2")
    def test_arinsert_multiple_values(self, r):
        assert r.arinsert("a", "v0", "v1", "v2") == 2
        assert r.arget("a", 0) == b"v0"
        assert r.arget("a", 1) == b"v1"
        assert r.arget("a", 2) == b"v2"

    @skip_if_server_version_lt("8.7.2")
    def test_arinsert_advances_cursor(self, r):
        r.arinsert("a", "v0", "v1")
        assert r.arnext("a") == 2
        assert r.arinsert("a", "v2") == 2
        assert r.arnext("a") == 3

    @skip_if_server_version_lt("8.7.2")
    def test_arring_inserts_sequentially(self, r):
        assert r.arring("a", 3, "v0") == 0
        assert r.arring("a", 3, "v1") == 1
        assert r.arring("a", 3, "v2") == 2
        assert r.arget("a", 0) == b"v0"
        assert r.arget("a", 1) == b"v1"
        assert r.arget("a", 2) == b"v2"

    @skip_if_server_version_lt("8.7.2")
    def test_arring_wraps_when_full(self, r):
        r.arring("a", 3, "v0", "v1", "v2")
        assert r.arring("a", 3, "v3") == 0
        assert r.arget("a", 0) == b"v3"
        assert r.arget("a", 1) == b"v1"
        assert r.arget("a", 2) == b"v2"

    @skip_if_server_version_lt("8.7.2")
    def test_arring_multiple_values_in_one_call(self, r):
        assert r.arring("a", 4, "v0", "v1", "v2") == 2
        assert r.arget("a", 0) == b"v0"
        assert r.arget("a", 2) == b"v2"

    @skip_if_server_version_lt("8.7.2")
    def test_arring_returns_last_inserted_index(self, r):
        assert r.arring("a", 5, "v0", "v1", "v2", "v3", "v4", "v5") == 0

    @skip_if_server_version_lt("8.7.2")
    def test_arring_truncates_on_smaller_size(self, r):
        r.arring("a", 5, "v0", "v1", "v2", "v3", "v4")
        assert r.arring("a", 3, "v5") == 0
        assert r.arlen("a") == 3

    @skip_if_server_version_lt("8.7.2")
    def test_arlastitems_returns_last_elements(self, r):
        r.arinsert("a", "v0", "v1", "v2", "v3", "v4")
        assert r.arlastitems("a", 3) == [b"v2", b"v3", b"v4"]

    @skip_if_server_version_lt("8.7.2")
    def test_arlastitems_reverse_order(self, r):
        r.arinsert("a", "v0", "v1", "v2", "v3", "v4")
        assert r.arlastitems("a", 3, rev=True) == [b"v4", b"v3", b"v2"]

    @skip_if_server_version_lt("8.7.2")
    def test_arlastitems_count_greater_than_size(self, r):
        r.arinsert("a", "v0", "v1", "v2")
        assert r.arlastitems("a", 10) == [b"v0", b"v1", b"v2"]

    @skip_if_server_version_lt("8.7.2")
    def test_arlastitems_count_greater_than_size_reverse(self, r):
        r.arinsert("a", "v0", "v1", "v2")
        assert r.arlastitems("a", 10, rev=True) == [b"v2", b"v1", b"v0"]

    @skip_if_server_version_lt("8.7.2")
    def test_arlastitems_missing_key(self, r):
        assert r.arlastitems("a", 3) == []

    @skip_if_server_version_lt("8.7.2")
    def test_arlastitems_zero_count(self, r):
        r.arinsert("a", "v0", "v1", "v2")
        assert r.arlastitems("a", 0) == []

    @skip_if_server_version_lt("8.7.2")
    def test_arop_sum(self, r):
        r.arset("a", 0, 1, 2, 3, 4, 5)
        assert r.arop("a", 0, 4, ArrayAggregateOperations.SUM) == b"15"

    @skip_if_server_version_lt("8.7.2")
    def test_arop_min_max(self, r):
        r.arset("a", 0, 5, 1, 4, 2, 3)
        assert r.arop("a", 0, 4, ArrayAggregateOperations.MIN) == b"1"
        assert r.arop("a", 0, 4, ArrayAggregateOperations.MAX) == b"5"

    @skip_if_server_version_lt("8.7.2")
    def test_arop_bitwise(self, r):
        r.arset("a", 0, 1, 2, 3, 4, 5)
        assert r.arop("a", 0, 4, ArrayAggregateOperations.AND) == 0
        assert r.arop("a", 0, 4, ArrayAggregateOperations.OR) == 7
        assert r.arop("a", 0, 4, ArrayAggregateOperations.XOR) == 1

    @skip_if_server_version_lt("8.7.2")
    def test_arop_match(self, r):
        r.arset("a", 0, 1, 2, 3, 2, 1)
        assert r.arop("a", 0, 4, ArrayAggregateOperations.MATCH, 2) == 2
        assert r.arop("a", 0, 4, ArrayAggregateOperations.MATCH, 7) == 0

    @skip_if_server_version_lt("8.7.2")
    def test_arop_used(self, r):
        r.arset("a", 0, "v0", "v1", "v2")
        assert r.arop("a", 0, 2, ArrayAggregateOperations.USED) == 3
        r.ardel("a", 1)
        assert r.arop("a", 0, 2, ArrayAggregateOperations.USED) == 2

    @skip_if_server_version_lt("8.7.2")
    def test_arop_reversed_range(self, r):
        r.arset("a", 0, 1, 2, 3, 4, 5)
        assert r.arop("a", 4, 0, ArrayAggregateOperations.SUM) == b"15"

    @skip_if_server_version_lt("8.7.2")
    def test_arop_non_numeric_values(self, r):
        r.arset("a", 0, "x", "y", "z")
        assert r.arop("a", 0, 2, ArrayAggregateOperations.SUM) is None
        assert r.arop("a", 0, 2, ArrayAggregateOperations.MIN) is None
        assert r.arop("a", 0, 2, ArrayAggregateOperations.MAX) is None
        assert r.arop("a", 0, 2, ArrayAggregateOperations.USED) == 3
        assert r.arop("a", 0, 2, ArrayAggregateOperations.MATCH, "y") == 1

    @skip_if_server_version_lt("8.7.2")
    def test_arop_mixed_numeric_non_numeric(self, r):
        r.arset("a", 0, 1, "x", 3)
        assert r.arop("a", 0, 2, ArrayAggregateOperations.SUM) == b"4"
        assert r.arop("a", 0, 2, ArrayAggregateOperations.MIN) == b"1"
        assert r.arop("a", 0, 2, ArrayAggregateOperations.USED) == 3

    @skip_if_server_version_lt("8.7.2")
    def test_arop_empty_range(self, r):
        r.arset("a", 0, 1, 2, 3)
        assert r.arop("a", 100, 200, ArrayAggregateOperations.SUM) is None
        assert r.arop("a", 100, 200, ArrayAggregateOperations.AND) is None
        assert r.arop("a", 100, 200, ArrayAggregateOperations.OR) is None
        assert r.arop("a", 100, 200, ArrayAggregateOperations.XOR) is None
        assert r.arop("a", 100, 200, ArrayAggregateOperations.USED) == 0
        assert r.arop("a", 100, 200, ArrayAggregateOperations.MATCH, 1) == 0

    @skip_if_server_version_lt("8.7.2")
    def test_arop_missing_key(self, r):
        assert r.arop("a", 0, 4, ArrayAggregateOperations.SUM) is None
        assert r.arop("a", 0, 4, ArrayAggregateOperations.AND) is None
        assert r.arop("a", 0, 4, ArrayAggregateOperations.USED) == 0

    @skip_if_server_version_lt("8.7.2")
    def test_arop_match_requires_value(self, r):
        r.arset("a", 0, 1, 2, 3)
        with pytest.raises(redis.ResponseError):
            r.arop("a", 0, 2, ArrayAggregateOperations.MATCH)

    def test_ltrim(self, r):
        r.rpush("a", "1", "2", "3")
        assert r.ltrim("a", 0, 1)
        assert r.lrange("a", 0, -1) == [b"1", b"2"]

    def test_rpop(self, r):
        r.rpush("a", "1", "2", "3")
        assert r.rpop("a") == b"3"
        assert r.rpop("a") == b"2"
        assert r.rpop("a") == b"1"
        assert r.rpop("a") is None

    @skip_if_server_version_lt("6.2.0")
    def test_rpop_count(self, r):
        r.rpush("a", "1", "2", "3")
        assert r.rpop("a", 2) == [b"3", b"2"]
        assert r.rpop("a", 1) == [b"1"]
        assert r.rpop("a") is None
        assert r.rpop("a", 3) is None

    @pytest.mark.onlynoncluster
    def test_rpoplpush(self, r):
        r.rpush("a", "a1", "a2", "a3")
        r.rpush("b", "b1", "b2", "b3")
        assert r.rpoplpush("a", "b") == b"a3"
        assert r.lrange("a", 0, -1) == [b"a1", b"a2"]
        assert r.lrange("b", 0, -1) == [b"a3", b"b1", b"b2", b"b3"]

    def test_rpush(self, r):
        assert r.rpush("a", "1") == 1
        assert r.rpush("a", "2") == 2
        assert r.rpush("a", "3", "4") == 4
        assert r.lrange("a", 0, -1) == [b"1", b"2", b"3", b"4"]

    @skip_if_server_version_lt("6.0.6")
    def test_lpos(self, r):
        assert r.rpush("a", "a", "b", "c", "1", "2", "3", "c", "c") == 8
        assert r.lpos("a", "a") == 0
        assert r.lpos("a", "c") == 2

        assert r.lpos("a", "c", rank=1) == 2
        assert r.lpos("a", "c", rank=2) == 6
        assert r.lpos("a", "c", rank=4) is None
        assert r.lpos("a", "c", rank=-1) == 7
        assert r.lpos("a", "c", rank=-2) == 6

        assert r.lpos("a", "c", count=0) == [2, 6, 7]
        assert r.lpos("a", "c", count=1) == [2]
        assert r.lpos("a", "c", count=2) == [2, 6]
        assert r.lpos("a", "c", count=100) == [2, 6, 7]

        assert r.lpos("a", "c", count=0, rank=2) == [6, 7]
        assert r.lpos("a", "c", count=2, rank=-1) == [7, 6]

        assert r.lpos("axxx", "c", count=0, rank=2) == []
        assert r.lpos("axxx", "c") is None

        assert r.lpos("a", "x", count=2) == []
        assert r.lpos("a", "x") is None

        assert r.lpos("a", "a", count=0, maxlen=1) == [0]
        assert r.lpos("a", "c", count=0, maxlen=1) == []
        assert r.lpos("a", "c", count=0, maxlen=3) == [2]
        assert r.lpos("a", "c", count=0, maxlen=3, rank=-1) == [7, 6]
        assert r.lpos("a", "c", count=0, maxlen=7, rank=2) == [6]

    def test_rpushx(self, r):
        assert r.rpushx("a", "b") == 0
        assert r.lrange("a", 0, -1) == []
        r.rpush("a", "1", "2", "3")
        assert r.rpushx("a", "4") == 4
        assert r.lrange("a", 0, -1) == [b"1", b"2", b"3", b"4"]

    @pytest.mark.onlynoncluster
    def test_lists_with_byte_keys(self, r):
        r.rpush(b"b", b"1", b"2", b"3")
        assert r.lrange(b"b", 0, -1) == [b"1", b"2", b"3"]
        # LPOS command with byte keys
        assert r.lpos(b"b", b"2") == 1
        assert r.lpos(b"b", b"2", rank=1) == 1
        assert r.lpos(b"b", b"2", rank=2) is None
        # LCS command with byte keys
        r.set(b"key1", b"ohmytext")
        r.set(b"key2", b"mynewtext")
        assert r.lcs(b"key1", b"key2") == b"mytext"
        # TYPE command with byte keys
        assert r.type(b"b") == b"list"
        assert r.type(b"key1") == b"string"
        # SCAN command with byte keys
        r.set(b"scan_key1", b"value1")
        r.set(b"scan_key2", b"value2")
        cursor, keys = r.scan(match=b"scan_key*")
        assert cursor == 0
        assert set(keys) == {b"scan_key1", b"scan_key2"}
        # PEXPIRETIME command with byte keys
        r.set(b"expire_key", b"value")
        r.pexpire(b"expire_key", 10000)
        pexpiretime = r.pexpiretime(b"expire_key")
        assert pexpiretime > 0
        # LMOVE command with byte keys (src and dest)
        r.rpush(b"list_src", b"a", b"b", b"c")
        r.rpush(b"list_dest", b"x")
        moved = r.lmove(b"list_src", b"list_dest", src=b"LEFT", dest=b"RIGHT")
        assert moved == b"a"
        assert r.lrange(b"list_dest", 0, -1) == [b"x", b"a"]
        # SMOVE command with byte keys (src and dst)
        r.sadd(b"set_src", b"member1", b"member2")
        r.sadd(b"set_dest", b"member3")
        assert r.smove(b"set_src", b"set_dest", b"member1") == 1
        assert b"member1" in r.smembers(b"set_dest")

    @pytest.mark.onlynoncluster
    def test_lists_with_memoryview_keys(self, r):
        # Create memoryview objects for key names
        mv_b = memoryview(b"b")
        mv_key1 = memoryview(b"key1")
        mv_key2 = memoryview(b"key2")
        mv_scan_key1 = memoryview(b"scan_key1")
        mv_scan_key2 = memoryview(b"scan_key2")
        mv_expire_key = memoryview(b"expire_key")
        mv_list_src = memoryview(b"list_src")
        mv_list_dest = memoryview(b"list_dest")
        mv_set_src = memoryview(b"set_src")
        mv_set_dest = memoryview(b"set_dest")

        r.rpush(mv_b, b"1", b"2", b"3")
        assert r.lrange(mv_b, 0, -1) == [b"1", b"2", b"3"]
        # LPOS command with memoryview keys
        assert r.lpos(mv_b, b"2") == 1
        assert r.lpos(mv_b, b"2", rank=1) == 1
        assert r.lpos(mv_b, b"2", rank=2) is None
        # LCS command with memoryview keys
        r.set(mv_key1, b"ohmytext")
        r.set(mv_key2, b"mynewtext")
        assert r.lcs(mv_key1, mv_key2) == b"mytext"
        # TYPE command with memoryview keys
        assert r.type(mv_b) == b"list"
        assert r.type(mv_key1) == b"string"
        # SCAN command with memoryview keys
        r.set(mv_scan_key1, b"value1")
        r.set(mv_scan_key2, b"value2")
        cursor, keys = r.scan(match=memoryview(b"scan_key*"))
        assert cursor == 0
        assert set(keys) == {b"scan_key1", b"scan_key2"}
        # PEXPIRETIME command with memoryview keys
        r.set(mv_expire_key, b"value")
        r.pexpire(mv_expire_key, 10000)
        pexpiretime = r.pexpiretime(mv_expire_key)
        assert pexpiretime > 0
        # LMOVE command with memoryview keys (src and dest)
        r.rpush(mv_list_src, b"a", b"b", b"c")
        r.rpush(mv_list_dest, b"x")
        moved = r.lmove(
            mv_list_src,
            mv_list_dest,
            src=memoryview(b"LEFT"),
            dest=memoryview(b"RIGHT"),
        )
        assert moved == b"a"
        assert r.lrange(mv_list_dest, 0, -1) == [b"x", b"a"]
        # SMOVE command with memoryview keys (src and dst)
        r.sadd(mv_set_src, b"member1", b"member2")
        r.sadd(mv_set_dest, b"member3")
        assert r.smove(mv_set_src, mv_set_dest, b"member1") == 1
        assert b"member1" in r.smembers(mv_set_dest)

    # BLESS COMMANDS
    @skip_if_server_version_lt("8.11.0")
    def test_bless_set_get_clear(self, r):
        r.set("a", 1)
        assert r.bless_get("a") == []
        assert r.bless_set("a", "NO-EVICT") == 1
        assert r.bless_set("a", "NO-EVICT") == 0
        assert r.bless_get("a") == [b"NO-EVICT"]
        assert r.bless_clear("a", "NO-EVICT") == 1
        assert r.bless_clear("a", "NO-EVICT") == 0
        assert r.bless_get("a") == []

    @skip_if_server_version_lt("8.11.0")
    def test_bless_missing_key(self, r):
        with pytest.raises(exceptions.ResponseError):
            r.bless_get("a")
        with pytest.raises(exceptions.ResponseError):
            r.bless_set("a", "NO-EVICT")
        with pytest.raises(exceptions.ResponseError):
            r.bless_clear("a", "NO-EVICT")

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.11.0")
    def test_bless_scan(self, r):
        r.set("a", 1)
        r.set("b", 2)
        r.set("c", 3)
        assert r.bless_scan(0, "NO-EVICT") == (0, [])
        r.bless_set("a", "NO-EVICT")
        r.bless_set("b", "NO-EVICT")
        cursor, keys = r.bless_scan(0, "NO-EVICT")
        assert cursor == 0
        assert set(keys) == {b"a", b"b"}

        cursor, keys = 0, []
        while True:
            cursor, page = r.bless_scan(cursor, "NO-EVICT", count=1)
            keys.extend(page)
            if cursor == 0:
                break
        assert set(keys) == {b"a", b"b"}

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.11.0")
    def test_bless_scan_iter(self, r):
        r.set("a", 1)
        r.set("b", 2)
        r.set("c", 3)
        assert list(r.bless_scan_iter("NO-EVICT")) == []
        r.bless_set("a", "NO-EVICT")
        r.bless_set("b", "NO-EVICT")
        keys = list(r.bless_scan_iter("NO-EVICT"))
        assert set(keys) == {b"a", b"b"}
        keys = list(r.bless_scan_iter("NO-EVICT", count=1))
        assert set(keys) == {b"a", b"b"}

    # SCAN COMMANDS
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.0")
    def test_scan(self, r):
        r.set("a", 1)
        r.set("b", 2)
        r.set("c", 3)
        cursor, keys = r.scan()
        assert cursor == 0
        assert set(keys) == {b"a", b"b", b"c"}
        _, keys = r.scan(match="a")
        assert set(keys) == {b"a"}

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.0.0")
    def test_scan_type(self, r):
        r.sadd("a-set", 1)
        r.hset("a-hash", "foo", 2)
        r.lpush("a-list", "aux", 3)
        _, keys = r.scan(match="a*", _type="SET")
        assert set(keys) == {b"a-set"}

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.0")
    def test_scan_iter(self, r):
        r.set("a", 1)
        r.set("b", 2)
        r.set("c", 3)
        keys = list(r.scan_iter())
        assert set(keys) == {b"a", b"b", b"c"}
        keys = list(r.scan_iter(match="a"))
        assert set(keys) == {b"a"}

    @skip_if_server_version_lt("2.8.0")
    def test_sscan(self, r):
        r.sadd("a", 1, 2, 3)
        cursor, members = r.sscan("a")
        assert cursor == 0
        assert set(members) == {b"1", b"2", b"3"}
        _, members = r.sscan("a", match=b"1")
        assert set(members) == {b"1"}

    @skip_if_server_version_lt("2.8.0")
    def test_sscan_iter(self, r):
        r.sadd("a", 1, 2, 3)
        members = list(r.sscan_iter("a"))
        assert set(members) == {b"1", b"2", b"3"}
        members = list(r.sscan_iter("a", match=b"1"))
        assert set(members) == {b"1"}

    @skip_if_server_version_lt("2.8.0")
    def test_hscan(self, r):
        r.hset("a", mapping={"a": 1, "b": 2, "c": 3})
        cursor, dic = r.hscan("a")
        assert cursor == 0
        assert dic == {b"a": b"1", b"b": b"2", b"c": b"3"}
        _, dic = r.hscan("a", match="a")
        assert dic == {b"a": b"1"}
        _, dic = r.hscan("a", no_values=False)
        assert dic == {b"a": b"1", b"b": b"2", b"c": b"3"}
        _, dic = r.hscan("a_notset")
        assert dic == {}

    @skip_if_server_version_lt("7.3.240")
    def test_hscan_novalues(self, r):
        r.hset("a", mapping={"a": 1, "b": 2, "c": 3})
        cursor, keys = r.hscan("a", no_values=True)
        assert cursor == 0
        assert sorted(keys) == [b"a", b"b", b"c"]
        _, keys = r.hscan("a", match="a", no_values=True)
        assert keys == [b"a"]
        _, keys = r.hscan("a_notset", no_values=True)
        assert keys == []

    @skip_if_server_version_lt("2.8.0")
    def test_hscan_iter(self, r):
        r.hset("a", mapping={"a": 1, "b": 2, "c": 3})
        dic = dict(r.hscan_iter("a"))
        assert dic == {b"a": b"1", b"b": b"2", b"c": b"3"}
        dic = dict(r.hscan_iter("a", match="a"))
        assert dic == {b"a": b"1"}
        dic = dict(r.hscan_iter("a", no_values=False))
        assert dic == {b"a": b"1", b"b": b"2", b"c": b"3"}
        dic = dict(r.hscan_iter("a_notset"))
        assert dic == {}

    @skip_if_server_version_lt("7.3.240")
    def test_hscan_iter_novalues(self, r):
        r.hset("a", mapping={"a": 1, "b": 2, "c": 3})
        keys = list(r.hscan_iter("a", no_values=True))
        assert keys == [b"a", b"b", b"c"]
        keys = list(r.hscan_iter("a", match="a", no_values=True))
        assert keys == [b"a"]
        keys = list(r.hscan_iter("a_notset", no_values=True))
        assert keys == []

    @skip_if_server_version_lt("2.8.0")
    def test_zscan(self, r):
        r.zadd("a", {"a": 1, "b": 2, "c": 3})
        cursor, pairs = r.zscan("a")
        assert cursor == 0
        if expects_unified_shape(r):
            assert sorted(pairs) == [[b"a", 1.0], [b"b", 2.0], [b"c", 3.0]]
        else:
            assert set(pairs) == {(b"a", 1), (b"b", 2), (b"c", 3)}
        _, pairs = r.zscan("a", match="a")
        if expects_unified_shape(r):
            assert pairs == [[b"a", 1.0]]
        else:
            assert set(pairs) == {(b"a", 1)}

    @pytest.mark.fixed_client
    def test_zscan_score_cast_scientific_notation(self):
        """score_cast_func=int handles scientific notation scores (issue #4000)."""
        response = (b"0", [b"member1", b"1.7732526297292595e+18"])
        cursor, pairs = parse_zscan(response, score_cast_func=int)
        assert cursor == 0
        assert pairs == [(b"member1", 1773252629729259520)]

    @skip_if_server_version_lt("2.8.0")
    def test_zscan_iter(self, r):
        r.zadd("a", {"a": 1, "b": 2, "c": 3})
        pairs = list(r.zscan_iter("a"))
        if expects_unified_shape(r):
            assert sorted(pairs) == [[b"a", 1.0], [b"b", 2.0], [b"c", 3.0]]
        else:
            assert set(pairs) == {(b"a", 1), (b"b", 2), (b"c", 3)}
        pairs = list(r.zscan_iter("a", match="a"))
        if expects_unified_shape(r):
            assert pairs == [[b"a", 1.0]]
        else:
            assert set(pairs) == {(b"a", 1)}

    # SET COMMANDS
    def test_sadd(self, r):
        members = {b"1", b"2", b"3"}
        r.sadd("a", *members)
        assert set(r.smembers("a")) == members

    def test_scard(self, r):
        r.sadd("a", "1", "2", "3")
        assert r.scard("a") == 3

    @pytest.mark.onlynoncluster
    def test_sdiff(self, r):
        r.sadd("a", "1", "2", "3")
        assert r.sdiff("a", "b") == {b"1", b"2", b"3"}
        r.sadd("b", "2", "3")
        assert r.sdiff("a", "b") == {b"1"}

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.9.0")
    def test_sdiffcard(self, r):
        r.sadd("s0", "a", "b", "c", "d", "e")
        r.sadd("s1", "c", "d", "x")
        r.sadd("s2", "e", "y")
        # exact difference cardinality: {a, b}
        assert r.sdiffcard(3, ["s0", "s1", "s2"]) == 2
        # limited difference cardinality is capped at limit
        assert r.sdiffcard(3, ["s0", "s1", "s2"], limit=1) == 1
        # a missing subtrahend key does not affect the result
        assert r.sdiffcard(2, ["s0", "missing"]) == 5
        # a missing first key yields 0
        assert r.sdiffcard(2, ["missing", "s0"]) == 0

    @pytest.mark.onlynoncluster
    def test_sdiffstore(self, r):
        r.sadd("a", "1", "2", "3")
        assert r.sdiffstore("c", "a", "b") == 3
        assert r.smembers("c") == {b"1", b"2", b"3"}
        r.sadd("b", "2", "3")
        assert r.sdiffstore("c", "a", "b") == 1
        assert r.smembers("c") == {b"1"}

    @pytest.mark.onlynoncluster
    def test_sinter(self, r):
        r.sadd("a", "1", "2", "3")
        assert r.sinter("a", "b") == set()
        r.sadd("b", "2", "3")
        assert r.sinter("a", "b") == {b"2", b"3"}

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_sintercard(self, r):
        r.sadd("a", 1, 2, 3)
        r.sadd("b", 1, 2, 3)
        r.sadd("c", 1, 3, 4)
        assert r.sintercard(3, ["a", "b", "c"]) == 2
        assert r.sintercard(3, ["a", "b", "c"], limit=1) == 1

    @pytest.mark.onlynoncluster
    def test_sinterstore(self, r):
        r.sadd("a", "1", "2", "3")
        assert r.sinterstore("c", "a", "b") == 0
        assert r.smembers("c") == set()
        r.sadd("b", "2", "3")
        assert r.sinterstore("c", "a", "b") == 2
        assert r.smembers("c") == {b"2", b"3"}

    def test_sismember(self, r):
        r.sadd("a", "1", "2", "3")
        assert r.sismember("a", "1")
        assert r.sismember("a", "2")
        assert r.sismember("a", "3")
        assert not r.sismember("a", "4")

    def test_smembers(self, r):
        r.sadd("a", "1", "2", "3")
        assert set(r.smembers("a")) == {b"1", b"2", b"3"}

    @skip_if_server_version_lt("6.2.0")
    def test_smismember(self, r):
        r.sadd("a", "1", "2", "3")
        result_list = [True, False, True, True]
        assert r.smismember("a", "1", "4", "2", "3") == result_list
        assert r.smismember("a", ["1", "4", "2", "3"]) == result_list

    @pytest.mark.onlynoncluster
    def test_smove(self, r):
        r.sadd("a", "a1", "a2")
        r.sadd("b", "b1", "b2")
        assert r.smove("a", "b", "a1")
        assert r.smembers("a") == {b"a2"}
        assert r.smembers("b") == {b"b1", b"b2", b"a1"}

    def test_spop(self, r):
        s = [b"1", b"2", b"3"]
        r.sadd("a", *s)
        value = r.spop("a")
        assert value in s
        assert set(r.smembers("a")) == set(s) - {value}

    @skip_if_server_version_lt("3.2.0")
    def test_spop_multi_value(self, r):
        s = [b"1", b"2", b"3"]
        r.sadd("a", *s)
        values = r.spop("a", 2)
        assert len(values) == 2

        for value in values:
            assert value in s
        assert set(r.spop("a", 1)) == set(s) - set(values)

    def test_srandmember(self, r):
        s = [b"1", b"2", b"3"]
        r.sadd("a", *s)
        assert r.srandmember("a") in s

    @skip_if_server_version_lt("2.6.0")
    def test_srandmember_multi_value(self, r):
        s = [b"1", b"2", b"3"]
        r.sadd("a", *s)
        randoms = r.srandmember("a", number=2)
        assert len(randoms) == 2
        assert set(randoms).intersection(s) == set(randoms)

    def test_srem(self, r):
        r.sadd("a", "1", "2", "3", "4")
        assert r.srem("a", "5") == 0
        assert r.srem("a", "2", "4") == 2
        assert set(r.smembers("a")) == {b"1", b"3"}

    @pytest.mark.onlynoncluster
    def test_sunion(self, r):
        r.sadd("a", "1", "2")
        r.sadd("b", "2", "3")
        assert set(r.sunion("a", "b")) == {b"1", b"2", b"3"}

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.9.0")
    def test_sunioncard(self, r):
        r.sadd("s1", "a", "b", "c")
        r.sadd("s2", "c", "d")
        # exact union cardinality: {a, b, c, d}
        assert r.sunioncard(2, ["s1", "s2"]) == 4
        # approximate union cardinality (still an integer reply)
        assert r.sunioncard(2, ["s1", "s2"], approx=True) == 4
        # a missing key is treated as an empty set
        assert r.sunioncard(2, ["s1", "missing"]) == 3
        # limited union cardinality is capped at limit in exact mode
        assert r.sunioncard(2, ["s1", "s2"], limit=3) == 3
        # APPROX combined with LIMIT does not exceed limit
        assert r.sunioncard(2, ["s1", "s2"], limit=3, approx=True) <= 3

    @pytest.mark.onlynoncluster
    def test_sunionstore(self, r):
        r.sadd("a", "1", "2")
        r.sadd("b", "2", "3")
        assert r.sunionstore("c", "a", "b") == 3
        assert set(r.smembers("c")) == {b"1", b"2", b"3"}

    @skip_if_server_version_lt("8.11.240")
    def test_sexpire_basic(self, r):
        r.delete("test:set")
        r.sadd("test:set", "member1", "member2")
        assert r.sexpire("test:set", 1, "member1") == [1]
        # test_spexpire_basic waits for a real expiry; here the stored TTL is enough
        assert r.sttl("test:set", "member1", "member2") == [1, -1]
        assert 0 < (r.spttl("test:set", "member1"))[0] <= 1000

    @skip_if_server_version_lt("8.11.240")
    def test_sexpire_with_timedelta(self, r):
        r.delete("test:set")
        r.sadd("test:set", "member1", "member2")
        assert r.sexpire("test:set", datetime.timedelta(seconds=10), "member1") == [1]
        # test_sexpire_basic waits for a real expiry; here the stored TTL is enough
        assert 0 < r.spttl("test:set", "member1")[0] <= 10000
        assert r.sttl("test:set", "member2") == [-1]

    @skip_if_server_version_lt("8.11.240")
    def test_sexpire_conditions(self, r):
        r.delete("test:set")
        r.sadd("test:set", "member1")
        assert r.sexpire("test:set", 20, "member1", xx=True) == [0]
        assert r.sexpire("test:set", 20, "member1", nx=True) == [1]
        assert r.sexpire("test:set", 10, "member1", xx=True) == [1]
        assert r.sexpire("test:set", 20, "member1", nx=True) == [0]
        # the xx call lowered the TTL and the last nx call left it alone
        assert 0 < r.spttl("test:set", "member1")[0] <= 10000
        r.sexpire("test:set", 20, "member1")
        assert r.sexpire("test:set", 10, "member1", gt=True) == [0]
        assert r.sexpire("test:set", 10, "member1", lt=True) == [1]
        assert 0 < r.spttl("test:set", "member1")[0] <= 10000

    @skip_if_server_version_lt("8.11.240")
    def test_sexpire_no_expiration_counts_as_infinite(self, r):
        r.delete("test:set")
        r.sadd("test:set", "member1", "member2")
        # a member without an expiration never passes GT and always passes LT
        assert r.sexpire("test:set", 100, "member1", gt=True) == [0]
        assert r.sttl("test:set", "member1") == [-1]
        assert r.sexpire("test:set", 100, "member2", lt=True) == [1]
        assert 0 < r.sttl("test:set", "member2")[0] <= 100

    @skip_if_server_version_lt("8.11.240")
    def test_sexpire_nonexistent_key_or_member(self, r):
        r.delete("test:set")
        assert r.sexpire("test:set", 1, "member1", "member2") == [-2, -2]
        assert r.exists("test:set") == 0
        r.sadd("test:set", "member1")
        assert r.sexpire("test:set", 1, "nonexistent_member") == [-2]

    @skip_if_server_version_lt("8.11.240")
    def test_sexpire_multiple_members(self, r):
        r.delete("test:set")
        r.sadd("test:set", "member1", "member2", "member3")
        r.sexpire("test:set", 100, "member1")
        # the condition is evaluated per member, in the order of the request
        assert r.sexpire(
            "test:set", 50, "member1", "member2", "nonexistent", nx=True
        ) == [
            0,
            1,
            -2,
        ]
        ttls = r.sttl("test:set", "member1", "member2", "member3")
        assert 50 < ttls[0] <= 100
        assert 0 < ttls[1] <= 50
        assert ttls[2] == -1

    @skip_if_server_version_lt("8.11.240")
    def test_sexpire_multiple_condition_flags_error(self, r):
        r.delete("test:set")
        r.sadd("test:set", "member1")
        with pytest.raises(ValueError) as e:
            r.sexpire("test:set", 1, "member1", nx=True, xx=True)
        assert "Only one of" in str(e)

    @skip_if_server_version_lt("8.11.240")
    def test_sexpire_past_time_deletes_members_and_key(self, r):
        r.delete("test:set")
        r.sadd("test:set", "member1", "member2", "member3")
        assert r.sexpire("test:set", 0, "member1") == [2]
        assert r.smembers("test:set") == {b"member2", b"member3"}
        # when the last members go, the key goes with them
        assert r.spexpireat("test:set", 1, "member2", "member3") == [2, 2]
        assert r.exists("test:set") == 0

    @skip_if_server_version_lt("8.11.240")
    def test_spexpire_basic(self, r):
        r.delete("test:set")
        r.sadd("test:set", "member1", "member2")
        assert r.spexpire("test:set", 100, "member1") == [1]
        # poll rather than sleep for the full TTL
        deadline = time.monotonic() + 3.0
        while r.sismember("test:set", "member1"):
            assert time.monotonic() < deadline, "member1 did not expire"
            time.sleep(0.01)
        assert r.smembers("test:set") == {b"member2"}

    @skip_if_server_version_lt("8.11.240")
    def test_spexpire_with_timedelta(self, r):
        r.delete("test:set")
        r.sadd("test:set", "member1", "member2")
        assert r.spexpire(
            "test:set", datetime.timedelta(milliseconds=5000), "member1"
        ) == [1]
        assert 0 < r.spttl("test:set", "member1")[0] <= 5000
        assert r.spttl("test:set", "member2") == [-1]

    @skip_if_server_version_lt("8.11.240")
    def test_spexpire_conditions(self, r):
        r.delete("test:set")
        r.sadd("test:set", "member1")
        assert r.spexpire("test:set", 20000, "member1", xx=True) == [0]
        assert r.spexpire("test:set", 20000, "member1", nx=True) == [1]
        assert r.spexpire("test:set", 10000, "member1", gt=True) == [0]
        assert r.spexpire("test:set", 10000, "member1", lt=True) == [1]
        assert 0 < r.spttl("test:set", "member1")[0] <= 10000

    @skip_if_server_version_lt("8.11.240")
    def test_spexpire_multiple_condition_flags_error(self, r):
        r.delete("test:set")
        r.sadd("test:set", "member1")
        with pytest.raises(ValueError) as e:
            r.spexpire("test:set", 1000, "member1", gt=True, lt=True)
        assert "Only one of" in str(e)

    @skip_if_server_version_lt("8.11.240")
    def test_sexpireat_basic(self, r):
        r.delete("test:set")
        r.sadd("test:set", "member1", "member2")
        exp_time = int(
            (datetime.datetime.now() + datetime.timedelta(seconds=10)).timestamp()
        )
        assert r.sexpireat("test:set", exp_time, "member1") == [1]
        assert r.sexpiretime("test:set", "member1", "member2") == [exp_time, -1]

    @skip_if_server_version_lt("8.11.240")
    def test_sexpireat_with_datetime(self, r):
        r.delete("test:set")
        r.sadd("test:set", "member1", "member2")
        exp_time = (datetime.datetime.now() + datetime.timedelta(seconds=10)).replace(
            microsecond=0
        )
        assert r.sexpireat("test:set", exp_time, "member1") == [1]
        assert r.sexpiretime("test:set", "member1", "member2") == [
            int(exp_time.timestamp()),
            -1,
        ]

    @skip_if_server_version_lt("8.11.240")
    def test_sexpireat_conditions(self, r):
        r.delete("test:set")
        r.sadd("test:set", "member1")
        future_exp_time = int(
            (datetime.datetime.now() + datetime.timedelta(seconds=10)).timestamp()
        )
        past_exp_time = int(
            (datetime.datetime.now() - datetime.timedelta(seconds=10)).timestamp()
        )
        assert r.sexpireat("test:set", future_exp_time, "member1", xx=True) == [0]
        assert r.sexpireat("test:set", future_exp_time, "member1", nx=True) == [1]
        assert r.sexpireat("test:set", past_exp_time, "member1", gt=True) == [0]
        assert r.sexpireat("test:set", past_exp_time, "member1", lt=True) == [2]
        assert r.exists("test:set") == 0

    @skip_if_server_version_lt("8.11.240")
    def test_spexpireat_basic(self, r):
        r.delete("test:set")
        r.sadd("test:set", "member1", "member2")
        exp_time = int(
            (datetime.datetime.now() + datetime.timedelta(seconds=10)).timestamp()
            * 1000
        )
        assert r.spexpireat("test:set", exp_time, "member1") == [1]
        # the stored expire time is exact
        assert r.spexpiretime("test:set", "member1", "member2") == [exp_time, -1]

    @skip_if_server_version_lt("8.11.240")
    def test_spexpireat_with_datetime(self, r):
        r.delete("test:set")
        r.sadd("test:set", "member1", "member2")
        exp_time = datetime.datetime.now() + datetime.timedelta(seconds=10)
        assert r.spexpireat("test:set", exp_time, "member1") == [1]
        assert r.spexpiretime("test:set", "member1", "member2") == [
            int(exp_time.timestamp() * 1000),
            -1,
        ]

    @skip_if_server_version_lt("8.11.240")
    def test_sexpireat_multiple_condition_flags_error(self, r):
        r.delete("test:set")
        r.sadd("test:set", "member1")
        exp_time = int(
            (datetime.datetime.now() + datetime.timedelta(seconds=10)).timestamp()
        )
        with pytest.raises(ValueError) as e:
            r.sexpireat("test:set", exp_time, "member1", xx=True, gt=True)
        assert "Only one of" in str(e)

    @skip_if_server_version_lt("8.11.240")
    def test_spexpireat_conditions(self, r):
        r.delete("test:set")
        r.sadd("test:set", "member1")
        future_exp_time = int(
            (datetime.datetime.now() + datetime.timedelta(seconds=10)).timestamp()
            * 1000
        )
        past_exp_time = int(
            (datetime.datetime.now() - datetime.timedelta(seconds=10)).timestamp()
            * 1000
        )
        assert r.spexpireat("test:set", future_exp_time, "member1", xx=True) == [0]
        assert r.spexpireat("test:set", future_exp_time, "member1", nx=True) == [1]
        assert r.spexpireat("test:set", past_exp_time, "member1", gt=True) == [0]
        assert r.spexpireat("test:set", past_exp_time, "member1", lt=True) == [2]
        assert r.exists("test:set") == 0

    @skip_if_server_version_lt("8.11.240")
    def test_spexpireat_multiple_condition_flags_error(self, r):
        r.delete("test:set")
        r.sadd("test:set", "member1")
        exp_time = int(
            (datetime.datetime.now() + datetime.timedelta(seconds=10)).timestamp()
            * 1000
        )
        with pytest.raises(ValueError) as e:
            r.spexpireat("test:set", exp_time, "member1", nx=True, lt=True)
        assert "Only one of" in str(e)

    @skip_if_server_version_lt("8.11.240")
    def test_spersist_multiple_members(self, r):
        r.delete("test:set")
        r.sadd("test:set", "member1", "member2")
        r.sexpire("test:set", 5000, "member1")
        assert r.spersist("test:set", "member1", "member2", "member3") == [1, -1, -2]
        assert r.sttl("test:set", "member1") == [-1]

    @skip_if_server_version_lt("8.11.240")
    def test_spersist_nonexistent_key(self, r):
        r.delete("test:set")
        assert r.spersist("test:set", "member1", "member2") == [-2, -2]

    @skip_if_server_version_lt("8.11.240")
    def test_sttl_and_spttl_multiple_members(self, r):
        r.delete("test:set")
        r.sadd("test:set", "member1", "member2")
        r.spexpire("test:set", 1800, "member1")
        ttl = r.sttl("test:set", "member1", "member2", "member3")
        # STTL rounds up, so a live member never reports 0; the exact rounding is
        # pinned by test_sexpiretime_and_spexpiretime_multiple_members
        assert ttl[0] in (1, 2)
        assert ttl[1:] == [-1, -2]
        pttl = r.spttl("test:set", "member1", "member2", "member3")
        assert 0 < pttl[0] <= 1800
        assert pttl[1:] == [-1, -2]

    @skip_if_server_version_lt("8.11.240")
    def test_sttl_and_spttl_nonexistent_key(self, r):
        r.delete("test:set")
        assert r.sttl("test:set", "member1", "member2") == [-2, -2]
        assert r.spttl("test:set", "member1", "member2") == [-2, -2]

    @skip_if_server_version_lt("8.11.240")
    def test_sexpiretime_and_spexpiretime_multiple_members(self, r):
        r.delete("test:set")
        r.sadd("test:set", "member1", "member2")
        exp_time_ms = int(
            (datetime.datetime.now() + datetime.timedelta(minutes=30)).timestamp()
            * 1000
        )
        r.spexpireat("test:set", exp_time_ms, "member1")
        assert r.spexpiretime("test:set", "member1", "member2", "member3") == [
            exp_time_ms,
            -1,
            -2,
        ]
        # SEXPIRETIME rounds up to the next second
        assert r.sexpiretime("test:set", "member1", "member2", "member3") == [
            (exp_time_ms + 999) // 1000,
            -1,
            -2,
        ]

    @skip_if_server_version_lt("8.11.240")
    def test_sexpiretime_and_spexpiretime_nonexistent_key(self, r):
        r.delete("test:set")
        assert r.sexpiretime("test:set", "member1", "member2") == [-2, -2]
        assert r.spexpiretime("test:set", "member1", "member2") == [-2, -2]

    @skip_if_server_version_lt("8.11.240")
    def test_saddex_no_expiration(self, r):
        r.delete("test:set")
        assert r.saddex("test:set", "member1", "member2") == 1
        assert r.smembers("test:set") == {b"member1", b"member2"}
        assert r.sttl("test:set", "member1", "member2") == [-1, -1]

    @skip_if_server_version_lt("8.11.240")
    def test_saddex_nothing_changed(self, r):
        r.delete("test:set")
        r.sadd("test:set", "member1")
        # 0 when no member was added, updated or deleted
        assert r.saddex("test:set", "member1") == 0
        assert r.saddex("test:set", "member1", keepttl=True) == 0
        assert r.smembers("test:set") == {b"member1"}

    @skip_if_server_version_lt("8.11.240")
    def test_saddex_without_expiration_discards_existing_one(self, r):
        r.delete("test:set")
        assert r.saddex("test:set", "member1", ex=100) == 1
        assert r.saddex("test:set", "member1", "member2") == 1
        assert r.sttl("test:set", "member1", "member2") == [-1, -1]

    @skip_if_server_version_lt("8.11.240")
    def test_saddex_expiration_ex_and_keepttl(self, r):
        r.delete("test:set")
        assert r.saddex("test:set", "member1", "member2", ex=10) == 1
        ttls = r.sttl("test:set", "member1", "member2")
        for ttl in ttls:
            assert 0 < ttl <= 10
        # keepttl keeps the existing expiration; a new member gets none
        assert r.saddex("test:set", "member1", "member3", keepttl=True) == 1
        assert 0 < r.spttl("test:set", "member1")[0] <= 10000
        assert r.sttl("test:set", "member3") == [-1]

    @skip_if_server_version_lt("8.11.240")
    def test_saddex_expiration_px(self, r):
        r.delete("test:set")
        assert r.saddex("test:set", "member1", "member2", px=60000) == 1
        pttls = r.spttl("test:set", "member1", "member2")
        for pttl in pttls:
            assert 0 < pttl <= 60000

    @skip_if_server_version_lt("8.11.240")
    def test_saddex_expiration_digit_strings(self, r):
        r.delete("test:set")
        assert r.saddex("test:set", "member1", ex="60") == 1
        assert 0 < r.sttl("test:set", "member1")[0] <= 60
        assert r.saddex("test:set", "member2", px="500") == 1
        assert 0 < r.spttl("test:set", "member2")[0] <= 500

    @skip_if_server_version_lt("8.11.240")
    def test_saddex_expiration_exat_and_pxat(self, r):
        r.delete("test:set")
        exat = datetime.datetime.now() + datetime.timedelta(seconds=30)
        assert r.saddex("test:set", "member1", exat=exat) == 1
        assert r.sexpiretime("test:set", "member1") == [int(exat.timestamp())]
        pxat = int(
            (datetime.datetime.now() + datetime.timedelta(seconds=30)).timestamp()
            * 1000
        )
        assert r.saddex("test:set", "member2", pxat=pxat) == 1
        assert r.spexpiretime("test:set", "member2") == [pxat]

    @skip_if_server_version_lt("8.11.240")
    def test_saddex_mnx(self, r):
        r.delete("test:set")
        r.sadd("test:set", "member1")
        # MNX fails as a whole when any member exists
        assert (
            r.saddex(
                "test:set",
                "member1",
                "member2",
                ex=10,
                data_persist_option=SetDataPersistOptions.MNX,
            )
            == 0
        )
        assert r.smembers("test:set") == {b"member1"}
        assert r.sttl("test:set", "member1") == [-1]
        assert (
            r.saddex(
                "test:set",
                "member2",
                "member3",
                ex=10,
                data_persist_option=SetDataPersistOptions.MNX,
            )
            == 1
        )
        assert r.smembers("test:set") == {b"member1", b"member2", b"member3"}

    @skip_if_server_version_lt("8.11.240")
    def test_saddex_mxx(self, r):
        r.delete("test:set")
        # MXX on a key that does not exist creates nothing
        assert (
            r.saddex(
                "test:set", "member1", data_persist_option=SetDataPersistOptions.MXX
            )
            == 0
        )
        assert r.exists("test:set") == 0
        r.sadd("test:set", "member1", "member2")
        assert (
            r.saddex(
                "test:set",
                "member1",
                "member3",
                ex=10,
                data_persist_option=SetDataPersistOptions.MXX,
            )
            == 0
        )
        assert r.smembers("test:set") == {b"member1", b"member2"}
        assert (
            r.saddex(
                "test:set",
                "member1",
                "member2",
                ex=10,
                data_persist_option=SetDataPersistOptions.MXX,
            )
            == 1
        )
        ttls = r.sttl("test:set", "member1", "member2")
        for ttl in ttls:
            assert 0 < ttl <= 10

    @skip_if_server_version_lt("8.11.240")
    def test_saddex_past_expiration_removes_members(self, r):
        r.delete("test:set")
        r.sadd("test:set", "member1", "member2")
        # 1 because member1 was deleted, even though nothing was added
        assert r.saddex("test:set", "member1", "member3", pxat=1) == 1
        assert r.smembers("test:set") == {b"member2"}
        # 0 when there is nothing to delete; the key is not created
        r.delete("test:set")
        assert r.saddex("test:set", "member1", exat=1) == 0
        assert r.exists("test:set") == 0

    @skip_if_server_version_lt("8.11.240")
    def test_saddex_invalid_inputs(self, r):
        with pytest.raises(exceptions.DataError):
            r.saddex("test:set", "member1", ex=10.0)

        with pytest.raises(exceptions.DataError):
            r.saddex("test:set", "member1", ex=10, px=1000)

        with pytest.raises(exceptions.DataError):
            r.saddex("test:set", "member1", ex=10, keepttl=True)

        # the server rejects an empty member list
        with pytest.raises(exceptions.ResponseError):
            r.saddex("test:set")

    @skip_if_server_version_lt("8.11.240")
    def test_set_member_expiration_wrong_type(self, r):
        r.delete("test:set")
        r.set("test:set", "value")
        with pytest.raises(exceptions.ResponseError, match="WRONGTYPE"):
            r.sexpire("test:set", 10, "member1")
        with pytest.raises(exceptions.ResponseError, match="WRONGTYPE"):
            r.sttl("test:set", "member1")
        with pytest.raises(exceptions.ResponseError, match="WRONGTYPE"):
            r.saddex("test:set", "member1")

    @skip_if_server_version_lt("8.11.240")
    def test_set_member_expiration_pipeline(self, r):
        r.delete("test:set")
        with r.pipeline() as pipe:
            pipe.saddex("test:set", "member1", "member2", ex=100)
            pipe.sexpire("test:set", 50, "member1", "member3", lt=True)
            pipe.spersist("test:set", "member2")
            pipe.sttl("test:set", "member1", "member2", "member3")
            saddex, sexpire, spersist, sttl = pipe.execute()
        assert saddex == 1
        assert sexpire == [1, -2]
        assert spersist == [1]
        assert 0 < sttl[0] <= 50
        assert sttl[1:] == [-1, -2]

    @skip_if_server_version_lt("1.0.0")
    @skip_if_redis_enterprise()
    def test_debug_segfault(self, r):
        with pytest.raises(NotImplementedError):
            r.debug_segfault()

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("3.2.0")
    @skip_if_redis_enterprise()
    def test_script_debug(self, r):
        with pytest.raises(NotImplementedError):
            r.script_debug()

    # SORTED SET COMMANDS
    def test_zadd(self, r):
        mapping = {"a1": 1.0, "a2": 2.0, "a3": 3.0}
        r.zadd("a", mapping)
        assert_resp_response(
            r,
            r.zrange("a", 0, -1, withscores=True),
            [(b"a1", 1.0), (b"a2", 2.0), (b"a3", 3.0)],
            [[b"a1", 1.0], [b"a2", 2.0], [b"a3", 3.0]],
        )

        # error cases
        with pytest.raises(exceptions.DataError):
            r.zadd("a", {})

        # cannot use both nx and xx options
        with pytest.raises(exceptions.DataError):
            r.zadd("a", mapping, nx=True, xx=True)

        # cannot use the incr options with more than one value
        with pytest.raises(exceptions.DataError):
            r.zadd("a", mapping, incr=True)

    def test_zadd_nx(self, r):
        assert r.zadd("a", {"a1": 1}) == 1
        assert r.zadd("a", {"a1": 99, "a2": 2}, nx=True) == 1
        assert_resp_response(
            r,
            r.zrange("a", 0, -1, withscores=True),
            [(b"a1", 1.0), (b"a2", 2.0)],
            [[b"a1", 1.0], [b"a2", 2.0]],
        )

    def test_zadd_xx(self, r):
        assert r.zadd("a", {"a1": 1}) == 1
        assert r.zadd("a", {"a1": 99, "a2": 2}, xx=True) == 0
        assert_resp_response(
            r,
            r.zrange("a", 0, -1, withscores=True),
            [(b"a1", 99.0)],
            [[b"a1", 99.0]],
        )

    def test_zadd_ch(self, r):
        assert r.zadd("a", {"a1": 1}) == 1
        assert r.zadd("a", {"a1": 99, "a2": 2}, ch=True) == 2
        assert_resp_response(
            r,
            r.zrange("a", 0, -1, withscores=True),
            [(b"a2", 2.0), (b"a1", 99.0)],
            [[b"a2", 2.0], [b"a1", 99.0]],
        )

    def test_zadd_incr(self, r):
        assert r.zadd("a", {"a1": 1}) == 1
        assert r.zadd("a", {"a1": 4.5}, incr=True) == 5.5

    def test_zadd_incr_with_xx(self, r):
        # this asks zadd to incr 'a1' only if it exists, but it clearly
        # doesn't. Redis returns a null value in this case and so should
        # redis-py
        assert r.zadd("a", {"a1": 1}, xx=True, incr=True) is None

    @skip_if_server_version_lt("6.2.0")
    def test_zadd_gt_lt(self, r):
        r.zadd("a", {"a": 2})
        assert r.zadd("a", {"a": 5}, gt=True, ch=True) == 1
        assert r.zadd("a", {"a": 1}, gt=True, ch=True) == 0
        assert r.zadd("a", {"a": 5}, lt=True, ch=True) == 0
        assert r.zadd("a", {"a": 1}, lt=True, ch=True) == 1

        # cannot combine both nx and xx options and gt and lt options
        with pytest.raises(exceptions.DataError):
            r.zadd("a", {"a15": 15}, nx=True, lt=True)
        with pytest.raises(exceptions.DataError):
            r.zadd("a", {"a15": 15}, nx=True, gt=True)
        with pytest.raises(exceptions.DataError):
            r.zadd("a", {"a15": 15}, lt=True, gt=True)
        with pytest.raises(exceptions.DataError):
            r.zadd("a", {"a15": 15}, nx=True, xx=True)

    def test_zcard(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert r.zcard("a") == 3

    def test_zcount(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert r.zcount("a", "-inf", "+inf") == 3
        assert r.zcount("a", 1, 2) == 2
        assert r.zcount("a", "(" + str(1), 2) == 1
        assert r.zcount("a", 1, "(" + str(2)) == 1
        assert r.zcount("a", 10, 20) == 0

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    def test_zdiff(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        r.zadd("b", {"a1": 1, "a2": 2})
        assert r.zdiff(["a", "b"]) == [b"a3"]
        assert_resp_response(
            r,
            r.zdiff(["a", "b"], withscores=True),
            [b"a3", b"3"],
            [[b"a3", 3.0]],
        )

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    def test_zdiffstore(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        r.zadd("b", {"a1": 1, "a2": 2})
        assert r.zdiffstore("out", ["a", "b"])
        assert r.zrange("out", 0, -1) == [b"a3"]
        assert_resp_response(
            r,
            r.zrange("out", 0, -1, withscores=True),
            [(b"a3", 3.0)],
            [[b"a3", 3.0]],
        )

    def test_zincrby(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert r.zincrby("a", 1, "a2") == 3.0
        assert r.zincrby("a", 5, "a3") == 8.0
        assert r.zscore("a", "a2") == 3.0
        assert r.zscore("a", "a3") == 8.0

    @skip_if_server_version_lt("2.8.9")
    def test_zlexcount(self, r):
        r.zadd("a", {"a": 0, "b": 0, "c": 0, "d": 0, "e": 0, "f": 0, "g": 0})
        assert r.zlexcount("a", "-", "+") == 7
        assert r.zlexcount("a", "[b", "[f") == 5

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    @pytest.mark.parametrize(
        "keys_factory", [dict, UserDict, MappingProxyType, ChainMap]
    )
    @pytest.mark.parametrize(
        "command, expected",
        [
            ("zunion", [(b"right", -7.0), (b"shared", -1.0), (b"left", 6.0)]),
            ("zinter", [(b"shared", -1.0)]),
            ("zunionstore", [(b"right", -7.0), (b"shared", -1.0), (b"left", 6.0)]),
            ("zinterstore", [(b"shared", -1.0)]),
        ],
    )
    def test_zaggregate_mapping_weights(self, r, keys_factory, command, expected):
        # Mapping wrappers must retain weights and the resulting rank order.
        r.zadd("a", {"shared": 2, "left": 3})
        r.zadd("b", {"shared": 5, "right": 7})
        keys = keys_factory({"a": 2, "b": -1})
        if command.endswith("store"):
            assert getattr(r, command)("destination", keys) == len(expected)
            result = r.zrange("destination", 0, -1, withscores=True)
        else:
            result = getattr(r, command)(keys, withscores=True)
        assert_resp_response(r, result, expected, [list(row) for row in expected])

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    def test_zinter(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 1})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zinter(["a", "b", "c"]) == [b"a3", b"a1"]
        # invalid aggregation
        with pytest.raises(exceptions.DataError):
            r.zinter(["a", "b", "c"], aggregate="foo", withscores=True)
        # aggregate with SUM
        assert_resp_response(
            r,
            r.zinter(["a", "b", "c"], withscores=True),
            [(b"a3", 8), (b"a1", 9)],
            [[b"a3", 8], [b"a1", 9]],
        )
        # aggregate with MAX
        assert_resp_response(
            r,
            r.zinter(["a", "b", "c"], aggregate="MAX", withscores=True),
            [(b"a3", 5), (b"a1", 6)],
            [[b"a3", 5], [b"a1", 6]],
        )
        # aggregate with MIN
        assert_resp_response(
            r,
            r.zinter(["a", "b", "c"], aggregate="MIN", withscores=True),
            [(b"a1", 1), (b"a3", 1)],
            [[b"a1", 1], [b"a3", 1]],
        )
        # with weights
        assert_resp_response(
            r,
            r.zinter({"a": 1, "b": 2, "c": 3}, withscores=True),
            [(b"a3", 20), (b"a1", 23)],
            [[b"a3", 20], [b"a1", 23]],
        )

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.7.0")
    def test_zinter_count(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 1})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        # aggregate with COUNT (scores ignored, counts membership)
        assert_resp_response(
            r,
            r.zinter(["a", "b", "c"], aggregate="COUNT", withscores=True),
            [(b"a1", 3), (b"a3", 3)],
            [[b"a1", 3], [b"a3", 3]],
        )
        # COUNT with weights
        assert_resp_response(
            r,
            r.zinter({"a": 1, "b": 2, "c": 3}, aggregate="COUNT", withscores=True),
            [(b"a1", 6), (b"a3", 6)],
            [[b"a1", 6], [b"a3", 6]],
        )

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_zintercard(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 1})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zintercard(3, ["a", "b", "c"]) == 2
        assert r.zintercard(3, ["a", "b", "c"], limit=1) == 1

    @pytest.mark.onlynoncluster
    def test_zinterstore_sum(self, r):
        r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zinterstore("d", ["a", "b", "c"]) == 2
        assert_resp_response(
            r,
            r.zrange("d", 0, -1, withscores=True),
            [(b"a3", 8), (b"a1", 9)],
            [[b"a3", 8], [b"a1", 9]],
        )

    @pytest.mark.onlynoncluster
    def test_zinterstore_max(self, r):
        r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zinterstore("d", ["a", "b", "c"], aggregate="MAX") == 2
        assert_resp_response(
            r,
            r.zrange("d", 0, -1, withscores=True),
            [(b"a3", 5), (b"a1", 6)],
            [[b"a3", 5], [b"a1", 6]],
        )

    @pytest.mark.onlynoncluster
    def test_zinterstore_min(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        r.zadd("b", {"a1": 2, "a2": 3, "a3": 5})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zinterstore("d", ["a", "b", "c"], aggregate="MIN") == 2
        assert_resp_response(
            r,
            r.zrange("d", 0, -1, withscores=True),
            [(b"a1", 1), (b"a3", 3)],
            [[b"a1", 1], [b"a3", 3]],
        )

    @pytest.mark.onlynoncluster
    def test_zinterstore_with_weight(self, r):
        r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zinterstore("d", {"a": 1, "b": 2, "c": 3}) == 2
        assert_resp_response(
            r,
            r.zrange("d", 0, -1, withscores=True),
            [(b"a3", 20), (b"a1", 23)],
            [[b"a3", 20], [b"a1", 23]],
        )

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.7.0")
    def test_zinterstore_count(self, r):
        r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zinterstore("d", ["a", "b", "c"], aggregate="COUNT") == 2
        assert_resp_response(
            r,
            r.zrange("d", 0, -1, withscores=True),
            [(b"a1", 3), (b"a3", 3)],
            [[b"a1", 3], [b"a3", 3]],
        )

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.7.0")
    def test_zinterstore_count_with_weight(self, r):
        r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zinterstore("d", {"a": 1, "b": 2, "c": 3}, aggregate="COUNT") == 2
        assert_resp_response(
            r,
            r.zrange("d", 0, -1, withscores=True),
            [(b"a1", 6), (b"a3", 6)],
            [[b"a1", 6], [b"a3", 6]],
        )

    @skip_if_server_version_lt("4.9.0")
    def test_zpopmax(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert_resp_response(
            r,
            r.zpopmax("a"),
            [(b"a3", 3)],
            [b"a3", 3.0],
            unified_expected=[[b"a3", 3.0]],
        )
        # with count
        assert_resp_response(
            r,
            r.zpopmax("a", count=2),
            [(b"a2", 2), (b"a1", 1)],
            [[b"a2", 2], [b"a1", 1]],
        )

    @skip_if_server_version_lt("4.9.0")
    def test_zpopmin(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert_resp_response(
            r,
            r.zpopmin("a"),
            [(b"a1", 1)],
            [b"a1", 1.0],
            unified_expected=[[b"a1", 1.0]],
        )
        # with count
        assert_resp_response(
            r,
            r.zpopmin("a", count=2),
            [(b"a2", 2), (b"a3", 3)],
            [[b"a2", 2], [b"a3", 3]],
        )

    @skip_if_server_version_lt("6.2.0")
    def test_zrandemember(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert r.zrandmember("a") is not None
        assert len(r.zrandmember("a", 2)) == 2

        # with scores
        assert_resp_response(
            r,
            len(r.zrandmember("a", 2, withscores=True)),
            4,
            2,
        )
        # without duplications
        assert len(r.zrandmember("a", 10)) == 5
        # with duplications
        assert len(r.zrandmember("a", -10)) == 10

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("4.9.0")
    def test_bzpopmax(self, r):
        r.zadd("a", {"a1": 1, "a2": 2})
        r.zadd("b", {"b1": 10, "b2": 20})
        assert_resp_response(
            r, r.bzpopmax(["b", "a"], timeout=1), (b"b", b"b2", 20), [b"b", b"b2", 20]
        )
        assert_resp_response(
            r, r.bzpopmax(["b", "a"], timeout=1), (b"b", b"b1", 10), [b"b", b"b1", 10]
        )
        assert_resp_response(
            r, r.bzpopmax(["b", "a"], timeout=1), (b"a", b"a2", 2), [b"a", b"a2", 2]
        )
        assert_resp_response(
            r, r.bzpopmax(["b", "a"], timeout=1), (b"a", b"a1", 1), [b"a", b"a1", 1]
        )
        assert r.bzpopmax(["b", "a"], timeout=0.1) is None
        r.zadd("c", {"c1": 100})
        assert_resp_response(
            r, r.bzpopmax("c", timeout=1), (b"c", b"c1", 100), [b"c", b"c1", 100]
        )

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("4.9.0")
    def test_bzpopmin(self, r):
        r.zadd("a", {"a1": 1, "a2": 2})
        r.zadd("b", {"b1": 10, "b2": 20})
        assert_resp_response(
            r, r.bzpopmin(["b", "a"], timeout=1), (b"b", b"b1", 10), [b"b", b"b1", 10]
        )
        assert_resp_response(
            r, r.bzpopmin(["b", "a"], timeout=1), (b"b", b"b2", 20), [b"b", b"b2", 20]
        )
        assert_resp_response(
            r, r.bzpopmin(["b", "a"], timeout=1), (b"a", b"a1", 1), [b"a", b"a1", 1]
        )
        assert_resp_response(
            r, r.bzpopmin(["b", "a"], timeout=1), (b"a", b"a2", 2), [b"a", b"a2", 2]
        )
        assert r.bzpopmin(["b", "a"], timeout=0.1) is None
        r.zadd("c", {"c1": 100})
        assert_resp_response(
            r, r.bzpopmin("c", timeout=1), (b"c", b"c1", 100), [b"c", b"c1", 100]
        )

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_zmpop(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert_resp_response(
            r,
            r.zmpop("2", ["b", "a"], min=True, count=2),
            [b"a", [[b"a1", b"1"], [b"a2", b"2"]]],
            [b"a", [[b"a1", 1.0], [b"a2", 2.0]]],
        )
        with pytest.raises(redis.DataError):
            r.zmpop("2", ["b", "a"], count=2)
        r.zadd("b", {"b1": 10, "ab": 9, "b3": 8})
        assert_resp_response(
            r,
            r.zmpop("2", ["b", "a"], max=True),
            [b"b", [[b"b1", b"10"]]],
            [b"b", [[b"b1", 10.0]]],
        )

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_bzmpop(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert_resp_response(
            r,
            r.bzmpop(1, "2", ["b", "a"], min=True, count=2),
            [b"a", [[b"a1", b"1"], [b"a2", b"2"]]],
            [b"a", [[b"a1", 1.0], [b"a2", 2.0]]],
        )
        with pytest.raises(redis.DataError):
            r.bzmpop(1, "2", ["b", "a"], count=2)
        r.zadd("b", {"b1": 10, "ab": 9, "b3": 8})
        assert_resp_response(
            r,
            r.bzmpop(0, "2", ["b", "a"], max=True),
            [b"b", [[b"b1", b"10"]]],
            [b"b", [[b"b1", 10.0]]],
        )
        assert r.bzmpop(0.1, "2", ["foo", "bar"], max=True) is None

    def test_zrange(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert r.zrange("a", 0, 1) == [b"a1", b"a2"]
        assert r.zrange("a", 1, 2) == [b"a2", b"a3"]
        assert r.zrange("a", 0, 2) == [b"a1", b"a2", b"a3"]
        assert r.zrange("a", 0, 2, desc=True) == [b"a3", b"a2", b"a1"]

        # withscores
        assert_resp_response(
            r,
            r.zrange("a", 0, 1, withscores=True),
            [(b"a1", 1.0), (b"a2", 2.0)],
            [[b"a1", 1.0], [b"a2", 2.0]],
        )
        assert_resp_response(
            r,
            r.zrange("a", 1, 2, withscores=True),
            [(b"a2", 2.0), (b"a3", 3.0)],
            [[b"a2", 2.0], [b"a3", 3.0]],
        )

        # custom score cast function
        assert_resp_response(
            r,
            r.zrange("a", 0, 1, withscores=True, score_cast_func=safe_str),
            [(b"a1", "1"), (b"a2", "2")],
            [[b"a1", "1.0"], [b"a2", "2.0"]],
        )

    @pytest.mark.fixed_client
    def test_zrange_score_cast_scientific_notation(self):
        """score_cast_func=int handles scientific notation scores (issue #4000)."""
        # Simulates RESP2 response with large score in scientific notation
        response = [b"member1", b"1.7732526297292595e+18"]
        result = zset_score_pairs(response, withscores=True, score_cast_func=int)
        assert result == [(b"member1", 1773252629729259520)]
        # float cast is unaffected
        result = zset_score_pairs(response, withscores=True, score_cast_func=float)
        assert result == [(b"member1", 1.7732526297292595e18)]
        # safe_str still receives raw bytes, not float (no "1.0" regression)
        result = zset_score_pairs(
            [b"a", b"1"], withscores=True, score_cast_func=safe_str
        )
        assert result == [(b"a", "1")]

    def test_zrange_errors(self, r):
        with pytest.raises(exceptions.DataError):
            r.zrange("a", 0, 1, byscore=True, bylex=True)
        with pytest.raises(exceptions.DataError):
            r.zrange("a", 0, 1, bylex=True, withscores=True)
        with pytest.raises(exceptions.DataError):
            r.zrange("a", 0, 1, byscore=True, withscores=True, offset=4)
        with pytest.raises(exceptions.DataError):
            r.zrange("a", 0, 1, byscore=True, withscores=True, num=2)

    @skip_if_server_version_lt("6.2.0")
    def test_zrange_params(self, r):
        # bylex
        r.zadd("a", {"a": 0, "b": 0, "c": 0, "d": 0, "e": 0, "f": 0, "g": 0})
        assert r.zrange("a", "[aaa", "(g", bylex=True) == [b"b", b"c", b"d", b"e", b"f"]
        assert r.zrange("a", "[f", "+", bylex=True) == [b"f", b"g"]
        assert r.zrange("a", "+", "[f", desc=True, bylex=True) == [b"g", b"f"]
        assert r.zrange("a", "-", "+", bylex=True, offset=3, num=2) == [b"d", b"e"]
        assert r.zrange("a", "+", "-", desc=True, bylex=True, offset=3, num=2) == [
            b"d",
            b"c",
        ]

        # byscore
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert r.zrange("a", 2, 4, byscore=True, offset=1, num=2) == [b"a3", b"a4"]
        assert r.zrange("a", 4, 2, desc=True, byscore=True, offset=1, num=2) == [
            b"a3",
            b"a2",
        ]
        assert_resp_response(
            r,
            r.zrange("a", 2, 4, byscore=True, withscores=True),
            [(b"a2", 2.0), (b"a3", 3.0), (b"a4", 4.0)],
            [[b"a2", 2.0], [b"a3", 3.0], [b"a4", 4.0]],
        )
        assert_resp_response(
            r,
            r.zrange(
                "a", 4, 2, desc=True, byscore=True, withscores=True, score_cast_func=int
            ),
            [(b"a4", 4), (b"a3", 3), (b"a2", 2)],
            [[b"a4", 4], [b"a3", 3], [b"a2", 2]],
        )

        # rev
        assert r.zrange("a", 0, 1, desc=True) == [b"a5", b"a4"]

    def test_zrange_desc_sends_zrevrange_by_default(self):
        """Rank-only desc=True keeps ZREVRANGE unless use_rev is set."""
        r = redis.Redis()
        with mock.patch.object(r, "execute_command", return_value=[]) as execute:
            r.zrange("a", 0, -1, desc=True)
        execute.assert_called_once_with(
            "ZREVRANGE", "a", 0, -1, withscores=False, score_cast_func=float, keys=["a"]
        )

    def test_zrange_desc_default_withscores_sends_zrevrange(self):
        r = redis.Redis()
        with mock.patch.object(r, "execute_command", return_value=[]) as execute:
            r.zrange("a", 0, -1, desc=True, withscores=True, score_cast_func=int)
        execute.assert_called_once_with(
            "ZREVRANGE",
            "a",
            0,
            -1,
            b"WITHSCORES",
            withscores=True,
            score_cast_func=int,
            keys=["a"],
        )

    def test_zrange_desc_use_rev_sends_zrange_rev(self):
        r = redis.Redis()
        with mock.patch.object(r, "execute_command", return_value=[]) as execute:
            r.zrange("a", 0, -1, desc=True, use_rev=True)
        execute.assert_called_once_with(
            "ZRANGE",
            "a",
            0,
            -1,
            "REV",
            withscores=False,
            score_cast_func=float,
            keys=["a"],
        )

    def test_zrange_desc_use_rev_withscores_and_score_cast(self):
        def cast_score(value):
            return value

        r = redis.Redis()
        with mock.patch.object(r, "execute_command", return_value=[]) as execute:
            r.zrange(
                "a",
                0,
                -1,
                desc=True,
                withscores=True,
                score_cast_func=cast_score,
                use_rev=True,
            )
        execute.assert_called_once_with(
            "ZRANGE",
            "a",
            0,
            -1,
            "REV",
            "WITHSCORES",
            withscores=True,
            score_cast_func=cast_score,
            keys=["a"],
        )

    def test_zrange_use_rev_without_desc_sends_plain_zrange(self):
        """use_rev only affects the rank-based reverse path."""
        r = redis.Redis()
        with mock.patch.object(r, "execute_command", return_value=[]) as execute:
            r.zrange("a", 0, -1, use_rev=True)
        execute.assert_called_once_with(
            "ZRANGE", "a", 0, -1, withscores=False, score_cast_func=float, keys=["a"]
        )

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    def test_zrangestore(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert r.zrangestore("b", "a", 0, 1)
        assert r.zrange("b", 0, -1) == [b"a1", b"a2"]
        assert r.zrangestore("b", "a", 1, 2)
        assert r.zrange("b", 0, -1) == [b"a2", b"a3"]
        assert_resp_response(
            r,
            r.zrange("b", 0, -1, withscores=True),
            [(b"a2", 2), (b"a3", 3)],
            [[b"a2", 2], [b"a3", 3]],
        )
        # reversed order
        assert r.zrangestore("b", "a", 1, 2, desc=True)
        assert r.zrange("b", 0, -1) == [b"a1", b"a2"]
        # by score
        assert r.zrangestore("b", "a", 2, 1, byscore=True, offset=0, num=1, desc=True)
        assert r.zrange("b", 0, -1) == [b"a2"]
        # by lex
        assert r.zrangestore("b", "a", "[a2", "(a3", bylex=True, offset=0, num=1)
        assert r.zrange("b", 0, -1) == [b"a2"]

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    @pytest.mark.parametrize("dest", ["", b"", memoryview(b"")])
    def test_zrangestore_empty_destination(self, r, dest):
        r.zadd("source", {"a": 1, "b": 2, "c": 3})
        assert r.zrangestore(dest, "source", 0, 1) == 2
        assert r.zrange(dest, 0, -1) == [b"a", b"b"]
        assert r.zrange("source", 0, -1) == [b"a", b"b", b"c"]

    @skip_if_server_version_lt("2.8.9")
    def test_zrangebylex(self, r):
        r.zadd("a", {"a": 0, "b": 0, "c": 0, "d": 0, "e": 0, "f": 0, "g": 0})
        assert r.zrangebylex("a", "-", "[c") == [b"a", b"b", b"c"]
        assert r.zrangebylex("a", "-", "(c") == [b"a", b"b"]
        assert r.zrangebylex("a", "[aaa", "(g") == [b"b", b"c", b"d", b"e", b"f"]
        assert r.zrangebylex("a", "[f", "+") == [b"f", b"g"]
        assert r.zrangebylex("a", "-", "+", start=3, num=2) == [b"d", b"e"]

    @skip_if_server_version_lt("2.9.9")
    def test_zrevrangebylex(self, r):
        r.zadd("a", {"a": 0, "b": 0, "c": 0, "d": 0, "e": 0, "f": 0, "g": 0})
        assert r.zrevrangebylex("a", "[c", "-") == [b"c", b"b", b"a"]
        assert r.zrevrangebylex("a", "(c", "-") == [b"b", b"a"]
        assert r.zrevrangebylex("a", "(g", "[aaa") == [b"f", b"e", b"d", b"c", b"b"]
        assert r.zrevrangebylex("a", "+", "[f") == [b"g", b"f"]
        assert r.zrevrangebylex("a", "+", "-", start=3, num=2) == [b"d", b"c"]

    def test_zrangebyscore(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert r.zrangebyscore("a", 2, 4) == [b"a2", b"a3", b"a4"]
        # slicing with start/num
        assert r.zrangebyscore("a", 2, 4, start=1, num=2) == [b"a3", b"a4"]
        # withscores
        assert_resp_response(
            r,
            r.zrangebyscore("a", 2, 4, withscores=True),
            [(b"a2", 2.0), (b"a3", 3.0), (b"a4", 4.0)],
            [[b"a2", 2.0], [b"a3", 3.0], [b"a4", 4.0]],
        )
        assert_resp_response(
            r,
            r.zrangebyscore("a", 2, 4, withscores=True, score_cast_func=int),
            [(b"a2", 2), (b"a3", 3), (b"a4", 4)],
            [[b"a2", 2], [b"a3", 3], [b"a4", 4]],
        )
        # custom score cast function
        assert_resp_response(
            r,
            r.zrangebyscore("a", 2, 4, withscores=True, score_cast_func=safe_str),
            [(b"a2", "2"), (b"a3", "3"), (b"a4", "4")],
            [[b"a2", "2.0"], [b"a3", "3.0"], [b"a4", "4.0"]],
        )

    @pytest.mark.fixed_client
    def test_zrangebyscore_score_cast_scientific_notation(self):
        """score_cast_func=int handles scientific notation scores (issue #4000)."""
        response = [b"a1", b"1.7732526297292595e+18", b"a2", b"2.5"]
        result = zset_score_pairs(response, withscores=True, score_cast_func=int)
        assert result == [(b"a1", 1773252629729259520), (b"a2", 2)]

    def test_zrank(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert r.zrank("a", "a1") == 0
        assert r.zrank("a", "a2") == 1
        assert r.zrank("a", "a6") is None

    @skip_if_server_version_lt("7.2.0")
    def test_zrank_withscore(self, r: redis.Redis):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert r.zrank("a", "a1") == 0
        assert r.zrank("a", "a2") == 1
        assert r.zrank("a", "a6") is None
        assert_resp_response(r, r.zrank("a", "a3", withscore=True), [2, 3.0], [2, 3.0])
        assert r.zrank("a", "a6", withscore=True) is None

        # custom score cast function
        assert_resp_response(
            r,
            r.zrank("a", "a3", withscore=True, score_cast_func=safe_str),
            [2, "3"],
            [2, "3.0"],
        )

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.2.0")
    def test_zset_scores_decode_responses(self, decoded_r: redis.Redis):
        # Scores handed back as text must be str, as on a RESP2 connection.
        r = decoded_r
        identity = lambda x: x  # noqa: E731
        r.zadd("a", {"a1": 1, "a2": 2.5})
        r.zadd("b", {"a1": 1})
        assert_resp_response(
            r,
            r.zdiff(["a", "b"], withscores=True),
            ["a2", "2.5"],
            [["a2", 2.5]],
        )
        assert_resp_response(
            r,
            r.zrange("a", 0, -1, withscores=True, score_cast_func=identity),
            [("a1", "1"), ("a2", "2.5")],
            [["a1", 1.0], ["a2", 2.5]],
        )
        assert_resp_response(
            r,
            r.zrank("a", "a2", withscore=True, score_cast_func=identity),
            [1, "2.5"],
            [1, 2.5],
        )
        assert_resp_response(
            r,
            r.zmpop(1, ["b"], min=True),
            ["b", [["a1", "1"]]],
            ["b", [["a1", 1.0]]],
        )

    def test_zrem(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert r.zrem("a", "a2") == 1
        assert r.zrange("a", 0, -1) == [b"a1", b"a3"]
        assert r.zrem("a", "b") == 0
        assert r.zrange("a", 0, -1) == [b"a1", b"a3"]

    def test_zrem_multiple_keys(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert r.zrem("a", "a1", "a2") == 2
        assert r.zrange("a", 0, 5) == [b"a3"]

    @skip_if_server_version_lt("2.8.9")
    def test_zremrangebylex(self, r):
        r.zadd("a", {"a": 0, "b": 0, "c": 0, "d": 0, "e": 0, "f": 0, "g": 0})
        assert r.zremrangebylex("a", "-", "[c") == 3
        assert r.zrange("a", 0, -1) == [b"d", b"e", b"f", b"g"]
        assert r.zremrangebylex("a", "[f", "+") == 2
        assert r.zrange("a", 0, -1) == [b"d", b"e"]
        assert r.zremrangebylex("a", "[h", "+") == 0
        assert r.zrange("a", 0, -1) == [b"d", b"e"]

    def test_zremrangebyrank(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert r.zremrangebyrank("a", 1, 3) == 3
        assert r.zrange("a", 0, 5) == [b"a1", b"a5"]

    def test_zremrangebyscore(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert r.zremrangebyscore("a", 2, 4) == 3
        assert r.zrange("a", 0, -1) == [b"a1", b"a5"]
        assert r.zremrangebyscore("a", 2, 4) == 0
        assert r.zrange("a", 0, -1) == [b"a1", b"a5"]

    def test_zrevrange(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert r.zrevrange("a", 0, 1) == [b"a3", b"a2"]
        assert r.zrevrange("a", 1, 2) == [b"a2", b"a1"]

        # withscores
        assert_resp_response(
            r,
            r.zrevrange("a", 0, 1, withscores=True),
            [(b"a3", 3.0), (b"a2", 2.0)],
            [[b"a3", 3.0], [b"a2", 2.0]],
        )
        assert_resp_response(
            r,
            r.zrevrange("a", 1, 2, withscores=True),
            [(b"a2", 2.0), (b"a1", 1.0)],
            [[b"a2", 2.0], [b"a1", 1.0]],
        )

        # custom score cast function
        # should be applied to resp2 and resp3
        # responses
        assert_resp_response(
            r,
            r.zrevrange("a", 0, 1, withscores=True, score_cast_func=safe_str),
            [(b"a3", "3"), (b"a2", "2")],
            [[b"a3", "3.0"], [b"a2", "2.0"]],
        )

    def test_zrevrangebyscore(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert r.zrevrangebyscore("a", 4, 2) == [b"a4", b"a3", b"a2"]
        # slicing with start/num
        assert r.zrevrangebyscore("a", 4, 2, start=1, num=2) == [b"a3", b"a2"]

        # withscores
        assert_resp_response(
            r,
            r.zrevrangebyscore("a", 4, 2, withscores=True),
            [(b"a4", 4.0), (b"a3", 3.0), (b"a2", 2.0)],
            [[b"a4", 4.0], [b"a3", 3.0], [b"a2", 2.0]],
        )
        # custom score type cast function
        assert_resp_response(
            r,
            r.zrevrangebyscore("a", 4, 2, withscores=True, score_cast_func=int),
            [(b"a4", 4.0), (b"a3", 3.0), (b"a2", 2.0)],
            [[b"a4", 4.0], [b"a3", 3.0], [b"a2", 2.0]],
        )
        # custom score cast function
        assert_resp_response(
            r,
            r.zrevrangebyscore("a", 4, 2, withscores=True, score_cast_func=safe_str),
            [(b"a4", "4"), (b"a3", "3"), (b"a2", "2")],
            [[b"a4", "4.0"], [b"a3", "3.0"], [b"a2", "2.0"]],
        )

    def test_zrevrank(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert r.zrevrank("a", "a1") == 4
        assert r.zrevrank("a", "a2") == 3
        assert r.zrevrank("a", "a6") is None

    @skip_if_server_version_lt("7.2.0")
    def test_zrevrank_withscore(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert r.zrevrank("a", "a1") == 4
        assert r.zrevrank("a", "a2") == 3
        assert r.zrevrank("a", "a6") is None
        assert_resp_response(
            r, r.zrevrank("a", "a3", withscore=True), [2, 3.0], [2, 3.0]
        )
        assert r.zrevrank("a", "a6", withscore=True) is None

        # custom score cast function
        assert_resp_response(
            r,
            r.zrevrank("a", "a3", withscore=True, score_cast_func=safe_str),
            [2, "3"],
            [2, "3.0"],
        )

    @pytest.mark.fixed_client
    def test_zrevrank_score_cast_scientific_notation(self):
        """score_cast_func=int handles scientific notation scores (issue #4000)."""
        response = [2, b"1.7732526297292595e+18"]
        result = zset_score_for_rank(response, withscore=True, score_cast_func=int)
        assert result == [2, 1773252629729259520]

    def test_zscore(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert r.zscore("a", "a1") == 1.0
        assert r.zscore("a", "a2") == 2.0
        assert r.zscore("a", "a4") is None

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    def test_zunion(self, r):
        r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        # sum
        assert r.zunion(["a", "b", "c"]) == [b"a2", b"a4", b"a3", b"a1"]
        assert_resp_response(
            r,
            r.zunion(["a", "b", "c"], withscores=True),
            [(b"a2", 3), (b"a4", 4), (b"a3", 8), (b"a1", 9)],
            [[b"a2", 3], [b"a4", 4], [b"a3", 8], [b"a1", 9]],
        )
        # max
        assert_resp_response(
            r,
            r.zunion(["a", "b", "c"], aggregate="MAX", withscores=True),
            [(b"a2", 2), (b"a4", 4), (b"a3", 5), (b"a1", 6)],
            [[b"a2", 2], [b"a4", 4], [b"a3", 5], [b"a1", 6]],
        )
        # min
        assert_resp_response(
            r,
            r.zunion(["a", "b", "c"], aggregate="MIN", withscores=True),
            [(b"a1", 1), (b"a2", 1), (b"a3", 1), (b"a4", 4)],
            [[b"a1", 1], [b"a2", 1], [b"a3", 1], [b"a4", 4]],
        )
        # with weight
        assert_resp_response(
            r,
            r.zunion({"a": 1, "b": 2, "c": 3}, withscores=True),
            [(b"a2", 5), (b"a4", 12), (b"a3", 20), (b"a1", 23)],
            [[b"a2", 5], [b"a4", 12], [b"a3", 20], [b"a1", 23]],
        )
        # with custom score cast function
        assert_resp_response(
            r,
            r.zunion(["a", "b", "c"], withscores=True, score_cast_func=safe_str),
            [(b"a2", "3"), (b"a4", "4"), (b"a3", "8"), (b"a1", "9")],
            [[b"a2", "3.0"], [b"a4", "4.0"], [b"a3", "8.0"], [b"a1", "9.0"]],
        )

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.7.0")
    def test_zunion_count(self, r):
        r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        # aggregate with COUNT (scores ignored, counts membership)
        assert_resp_response(
            r,
            r.zunion(["a", "b", "c"], aggregate="COUNT", withscores=True),
            [(b"a4", 1), (b"a2", 2), (b"a1", 3), (b"a3", 3)],
            [[b"a4", 1], [b"a2", 2], [b"a1", 3], [b"a3", 3]],
        )
        # COUNT with weights
        assert_resp_response(
            r,
            r.zunion({"a": 1, "b": 2, "c": 3}, aggregate="COUNT", withscores=True),
            [(b"a2", 3), (b"a4", 3), (b"a1", 6), (b"a3", 6)],
            [[b"a2", 3], [b"a4", 3], [b"a1", 6], [b"a3", 6]],
        )

    @pytest.mark.onlynoncluster
    def test_zunionstore_sum(self, r):
        r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zunionstore("d", ["a", "b", "c"]) == 4
        assert_resp_response(
            r,
            r.zrange("d", 0, -1, withscores=True),
            [(b"a2", 3), (b"a4", 4), (b"a3", 8), (b"a1", 9)],
            [[b"a2", 3], [b"a4", 4], [b"a3", 8], [b"a1", 9]],
        )

    @pytest.mark.onlynoncluster
    def test_zunionstore_max(self, r):
        r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zunionstore("d", ["a", "b", "c"], aggregate="MAX") == 4
        assert_resp_response(
            r,
            r.zrange("d", 0, -1, withscores=True),
            [(b"a2", 2), (b"a4", 4), (b"a3", 5), (b"a1", 6)],
            [[b"a2", 2], [b"a4", 4], [b"a3", 5], [b"a1", 6]],
        )

    @pytest.mark.onlynoncluster
    def test_zunionstore_min(self, r):
        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 4})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zunionstore("d", ["a", "b", "c"], aggregate="MIN") == 4
        assert_resp_response(
            r,
            r.zrange("d", 0, -1, withscores=True),
            [(b"a1", 1), (b"a2", 2), (b"a3", 3), (b"a4", 4)],
            [[b"a1", 1], [b"a2", 2], [b"a3", 3], [b"a4", 4]],
        )

    @pytest.mark.onlynoncluster
    def test_zunionstore_with_weight(self, r):
        r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zunionstore("d", {"a": 1, "b": 2, "c": 3}) == 4
        assert_resp_response(
            r,
            r.zrange("d", 0, -1, withscores=True),
            [(b"a2", 5), (b"a4", 12), (b"a3", 20), (b"a1", 23)],
            [[b"a2", 5], [b"a4", 12], [b"a3", 20], [b"a1", 23]],
        )

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.7.0")
    def test_zunionstore_count(self, r):
        r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zunionstore("d", ["a", "b", "c"], aggregate="COUNT") == 4
        assert_resp_response(
            r,
            r.zrange("d", 0, -1, withscores=True),
            [(b"a4", 1), (b"a2", 2), (b"a1", 3), (b"a3", 3)],
            [[b"a4", 1], [b"a2", 2], [b"a1", 3], [b"a3", 3]],
        )

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.7.0")
    def test_zunionstore_count_with_weight(self, r):
        r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert r.zunionstore("d", {"a": 1, "b": 2, "c": 3}, aggregate="COUNT") == 4
        assert_resp_response(
            r,
            r.zrange("d", 0, -1, withscores=True),
            [(b"a2", 3), (b"a4", 3), (b"a1", 6), (b"a3", 6)],
            [[b"a2", 3], [b"a4", 3], [b"a1", 6], [b"a3", 6]],
        )

    @skip_if_server_version_lt("6.1.240")
    def test_zmscore(self, r):
        with pytest.raises(exceptions.DataError):
            r.zmscore("invalid_key", [])

        assert r.zmscore("invalid_key", ["invalid_member"]) == [None]

        r.zadd("a", {"a1": 1, "a2": 2, "a3": 3.5})
        assert r.zmscore("a", ["a1", "a2", "a3", "a4"]) == [1.0, 2.0, 3.5, None]

    # HYPERLOGLOG TESTS
    @skip_if_server_version_lt("2.8.9")
    def test_pfadd(self, r):
        members = {b"1", b"2", b"3"}
        assert r.pfadd("a", *members) == 1
        assert r.pfadd("a", *members) == 0
        assert r.pfcount("a") == len(members)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.9")
    def test_pfcount(self, r):
        members = {b"1", b"2", b"3"}
        r.pfadd("a", *members)
        assert r.pfcount("a") == len(members)
        members_b = {b"2", b"3", b"4"}
        r.pfadd("b", *members_b)
        assert r.pfcount("b") == len(members_b)
        assert r.pfcount("a", "b") == len(members_b.union(members))

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.9")
    def test_pfmerge(self, r):
        mema = {b"1", b"2", b"3"}
        memb = {b"2", b"3", b"4"}
        memc = {b"5", b"6", b"7"}
        r.pfadd("a", *mema)
        r.pfadd("b", *memb)
        r.pfadd("c", *memc)
        r.pfmerge("d", "c", "a")
        assert r.pfcount("d") == 6
        r.pfmerge("d", "b")
        assert r.pfcount("d") == 7

    # HASH COMMANDS
    def test_hget_and_hset(self, r):
        r.hset("a", mapping={"1": 1, "2": 2, "3": 3})
        assert r.hget("a", "1") == b"1"
        assert r.hget("a", "2") == b"2"
        assert r.hget("a", "3") == b"3"

        # field was updated, redis returns 0
        assert r.hset("a", "2", 5) == 0
        assert r.hget("a", "2") == b"5"

        # field is new, redis returns 1
        assert r.hset("a", "4", 4) == 1
        assert r.hget("a", "4") == b"4"

        # key inside of hash that doesn't exist returns null value
        assert r.hget("a", "b") is None

        # keys with bool(key) == False
        assert r.hset("a", 0, 10) == 1
        assert r.hset("a", "", 10) == 1

    def test_hgetex_zero_expiry_options_are_mutually_exclusive(self, r):
        with pytest.raises(DataError):
            r.hgetex("h", "f", ex=0, px=1)

    def test_hsetex_zero_expiry_options_are_mutually_exclusive(self, r):
        with pytest.raises(DataError):
            r.hsetex("h", "f", "v", ex=0, px=1)

    def test_hget_and_hset_with_encodable_fields_and_values(self, r):
        cases = (
            (b"field-bytes", b"value-bytes", b"value-bytes"),
            (
                bytearray(b"field-bytearray"),
                bytearray(b"value-bytearray"),
                b"value-bytearray",
            ),
            (
                memoryview(b"field-memoryview"),
                memoryview(b"value-memoryview"),
                b"value-memoryview",
            ),
            ("field-str", "value-str", b"value-str"),
            (42, 43, b"43"),
            (1.25, 2.5, b"2.5"),
        )

        for field, value, expected in cases:
            assert r.hset("encodable-hash", field, value) == 1
            assert r.hget("encodable-hash", field) == expected
            assert r.hmget("encodable-hash", field) == [expected]

        assert r.hmget("encodable-hash", bytearray(b"field-bytes")) == [b"value-bytes"]

    def test_hset_with_multi_key_values(self, r):
        r.hset("a", mapping={"1": 1, "2": 2, "3": 3})
        assert r.hget("a", "1") == b"1"
        assert r.hget("a", "2") == b"2"
        assert r.hget("a", "3") == b"3"

        r.hset("b", "foo", "bar", mapping={"1": 1, "2": 2})
        assert r.hget("b", "1") == b"1"
        assert r.hget("b", "2") == b"2"
        assert r.hget("b", "foo") == b"bar"

    def test_hset_with_key_values_passed_as_list(self, r):
        r.hset("a", items=["1", 1, "2", 2, "3", 3])
        assert r.hget("a", "1") == b"1"
        assert r.hget("a", "2") == b"2"
        assert r.hget("a", "3") == b"3"

        r.hset("b", "foo", "bar", items=["1", 1, "2", 2])
        assert r.hget("b", "1") == b"1"
        assert r.hget("b", "2") == b"2"
        assert r.hget("b", "foo") == b"bar"

    def test_hset_without_data(self, r):
        with pytest.raises(exceptions.DataError):
            r.hset("x")

    def test_hdel(self, r):
        r.hset("a", mapping={"1": 1, "2": 2, "3": 3})
        assert r.hdel("a", "2") == 1
        assert r.hget("a", "2") is None
        assert r.hdel("a", "1", "3") == 2
        assert r.hlen("a") == 0

    def test_hexists(self, r):
        r.hset("a", mapping={"1": 1, "2": 2, "3": 3})
        assert r.hexists("a", "1")
        assert not r.hexists("a", "4")

    def test_hgetall(self, r):
        h = {b"a1": b"1", b"a2": b"2", b"a3": b"3"}
        r.hset("a", mapping=h)
        assert r.hgetall("a") == h

    def test_hincrby(self, r):
        assert r.hincrby("a", "1") == 1
        assert r.hincrby("a", "1", amount=2) == 3
        assert r.hincrby("a", "1", amount=-2) == 1

    @skip_if_server_version_lt("2.6.0")
    def test_hincrbyfloat(self, r):
        assert r.hincrbyfloat("a", "1") == 1.0
        assert r.hincrbyfloat("a", "1") == 2.0
        assert r.hincrbyfloat("a", "1", 1.2) == 3.2

    def test_hkeys(self, r):
        h = {b"a1": b"1", b"a2": b"2", b"a3": b"3"}
        r.hset("a", mapping=h)
        local_keys = list(h.keys())
        remote_keys = r.hkeys("a")
        assert sorted(local_keys) == sorted(remote_keys)

    def test_hlen(self, r):
        r.hset("a", mapping={"1": 1, "2": 2, "3": 3})
        assert r.hlen("a") == 3

    def test_hmget(self, r):
        assert r.hset("a", mapping={"a": 1, "b": 2, "c": 3})
        assert r.hmget("a", "a", "b", "c") == [b"1", b"2", b"3"]

    def test_hmset(self, r):
        h = {b"a": b"1", b"b": b"2", b"c": b"3"}
        assert r.hmset("a", h)
        assert r.hgetall("a") == h

    def test_hsetnx(self, r):
        # Initially set the hash field
        assert r.hsetnx("a", "1", 1)
        assert r.hget("a", "1") == b"1"
        assert not r.hsetnx("a", "1", 2)
        assert r.hget("a", "1") == b"1"

    def test_hvals(self, r):
        h = {b"a1": b"1", b"a2": b"2", b"a3": b"3"}
        r.hset("a", mapping=h)
        local_vals = list(h.values())
        remote_vals = r.hvals("a")
        assert sorted(local_vals) == sorted(remote_vals)

    @skip_if_server_version_lt("3.2.0")
    def test_hstrlen(self, r):
        r.hset("a", mapping={"1": "22", "2": "333"})
        assert r.hstrlen("a", "1") == 2
        assert r.hstrlen("a", "2") == 3

    # SORT
    def test_sort_basic(self, r):
        r.rpush("a", "3", "2", "1", "4")
        assert r.sort("a") == [b"1", b"2", b"3", b"4"]

    def test_sort_limited(self, r):
        r.rpush("a", "3", "2", "1", "4")
        assert r.sort("a", start=1, num=2) == [b"2", b"3"]

    @pytest.mark.onlynoncluster
    def test_sort_by(self, r):
        r["score:1"] = 8
        r["score:2"] = 3
        r["score:3"] = 5
        r.rpush("a", "3", "2", "1")
        assert r.sort("a", by="score:*") == [b"2", b"3", b"1"]

    @pytest.mark.onlynoncluster
    def test_sort_get(self, r):
        r["user:1"] = "u1"
        r["user:2"] = "u2"
        r["user:3"] = "u3"
        r.rpush("a", "2", "3", "1")
        assert r.sort("a", get="user:*") == [b"u1", b"u2", b"u3"]

    @pytest.mark.onlynoncluster
    def test_sort_get_multi(self, r):
        r["user:1"] = "u1"
        r["user:2"] = "u2"
        r["user:3"] = "u3"
        r.rpush("a", "2", "3", "1")
        assert r.sort("a", get=("user:*", "#")) == [
            b"u1",
            b"1",
            b"u2",
            b"2",
            b"u3",
            b"3",
        ]

    @pytest.mark.onlynoncluster
    def test_sort_get_groups_two(self, r):
        r["user:1"] = "u1"
        r["user:2"] = "u2"
        r["user:3"] = "u3"
        r.rpush("a", "2", "3", "1")
        assert r.sort("a", get=("user:*", "#"), groups=True) == [
            (b"u1", b"1"),
            (b"u2", b"2"),
            (b"u3", b"3"),
        ]

    @pytest.mark.onlynoncluster
    def test_sort_groups_string_get(self, r):
        r["user:1"] = "u1"
        r["user:2"] = "u2"
        r["user:3"] = "u3"
        r.rpush("a", "2", "3", "1")
        with pytest.raises(exceptions.DataError):
            r.sort("a", get="user:*", groups=True)

    @pytest.mark.onlynoncluster
    def test_sort_groups_just_one_get(self, r):
        r["user:1"] = "u1"
        r["user:2"] = "u2"
        r["user:3"] = "u3"
        r.rpush("a", "2", "3", "1")
        with pytest.raises(exceptions.DataError):
            r.sort("a", get=["user:*"], groups=True)

    def test_sort_groups_no_get(self, r):
        r["user:1"] = "u1"
        r["user:2"] = "u2"
        r["user:3"] = "u3"
        r.rpush("a", "2", "3", "1")
        with pytest.raises(exceptions.DataError):
            r.sort("a", groups=True)

    @pytest.mark.onlynoncluster
    def test_sort_groups_three_gets(self, r):
        r["user:1"] = "u1"
        r["user:2"] = "u2"
        r["user:3"] = "u3"
        r["door:1"] = "d1"
        r["door:2"] = "d2"
        r["door:3"] = "d3"
        r.rpush("a", "2", "3", "1")
        assert r.sort("a", get=("user:*", "door:*", "#"), groups=True) == [
            (b"u1", b"d1", b"1"),
            (b"u2", b"d2", b"2"),
            (b"u3", b"d3", b"3"),
        ]

    def test_sort_desc(self, r):
        r.rpush("a", "2", "3", "1")
        assert r.sort("a", desc=True) == [b"3", b"2", b"1"]

    def test_sort_alpha(self, r):
        r.rpush("a", "e", "c", "b", "d", "a")
        assert r.sort("a", alpha=True) == [b"a", b"b", b"c", b"d", b"e"]

    @pytest.mark.onlynoncluster
    def test_sort_store(self, r):
        r.rpush("a", "2", "3", "1")
        assert r.sort("a", store="sorted_values") == 3
        assert r.lrange("sorted_values", 0, -1) == [b"1", b"2", b"3"]

    @pytest.mark.onlynoncluster
    def test_sort_all_options(self, r):
        r["user:1:username"] = "zeus"
        r["user:2:username"] = "titan"
        r["user:3:username"] = "hermes"
        r["user:4:username"] = "hercules"
        r["user:5:username"] = "apollo"
        r["user:6:username"] = "athena"
        r["user:7:username"] = "hades"
        r["user:8:username"] = "dionysus"

        r["user:1:favorite_drink"] = "yuengling"
        r["user:2:favorite_drink"] = "rum"
        r["user:3:favorite_drink"] = "vodka"
        r["user:4:favorite_drink"] = "milk"
        r["user:5:favorite_drink"] = "pinot noir"
        r["user:6:favorite_drink"] = "water"
        r["user:7:favorite_drink"] = "gin"
        r["user:8:favorite_drink"] = "apple juice"

        r.rpush("gods", "5", "8", "3", "1", "2", "7", "6", "4")
        num = r.sort(
            "gods",
            start=2,
            num=4,
            by="user:*:username",
            get="user:*:favorite_drink",
            desc=True,
            alpha=True,
            store="sorted",
        )
        assert num == 4
        assert r.lrange("sorted", 0, 10) == [b"vodka", b"milk", b"gin", b"apple juice"]

    @skip_if_server_version_lt("7.0.0")
    @pytest.mark.onlynoncluster
    def test_sort_ro(self, r):
        r["score:1"] = 8
        r["score:2"] = 3
        r["score:3"] = 5
        r.rpush("a", "3", "2", "1")
        assert r.sort_ro("a", by="score:*") == [b"2", b"3", b"1"]
        r.rpush("b", "2", "3", "1")
        assert r.sort_ro("b", desc=True) == [b"3", b"2", b"1"]

    def test_sort_issue_924(self, r):
        # Tests for issue https://github.com/andymccurdy/redis-py/issues/924
        r.execute_command("SADD", "issue#924", 1)
        r.execute_command("SORT", "issue#924")

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_addslots(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster("ADDSLOTS", 1) is True

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_count_failure_reports(self, mock_cluster_resp_int):
        assert isinstance(
            mock_cluster_resp_int.cluster("COUNT-FAILURE-REPORTS", "node"), int
        )

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_countkeysinslot(self, mock_cluster_resp_int):
        assert isinstance(mock_cluster_resp_int.cluster("COUNTKEYSINSLOT", 2), int)

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_delslots(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster("DELSLOTS", 1) is True

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_failover(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster("FAILOVER", 1) is True

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_forget(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster("FORGET", 1) is True

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_info(self, mock_cluster_resp_info):
        assert isinstance(mock_cluster_resp_info.cluster("info"), dict)

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_keyslot(self, mock_cluster_resp_int):
        assert isinstance(mock_cluster_resp_int.cluster("keyslot", "asdf"), int)

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_meet(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster("meet", "ip", "port", 1) is True

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_nodes(self, mock_cluster_resp_nodes):
        assert isinstance(mock_cluster_resp_nodes.cluster("nodes"), dict)

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_replicate(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster("replicate", "nodeid") is True

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_reset(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster("reset", "hard") is True

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_saveconfig(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster("saveconfig") is True

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_setslot(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster("setslot", 1, "IMPORTING", "nodeid") is True

    @pytest.mark.onlynoncluster
    @skip_if_redis_enterprise()
    def test_cluster_slaves(self, mock_cluster_resp_slaves):
        assert isinstance(mock_cluster_resp_slaves.cluster("slaves", "nodeid"), dict)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("3.0.0")
    @skip_if_server_version_gte("7.0.0")
    @skip_if_redis_enterprise()
    def test_readwrite(self, r):
        assert r.readwrite()

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("3.0.0")
    def test_readonly_invalid_cluster_state(self, r):
        with pytest.raises(exceptions.RedisError):
            r.readonly()

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("3.0.0")
    @skip_if_redis_enterprise()
    def test_readonly(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.readonly() is True

    # GEO COMMANDS
    @skip_if_server_version_lt("3.2.0")
    def test_geoadd(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )
        assert r.geoadd("barcelona", values) == 2
        assert r.zcard("barcelona") == 2

    @skip_if_server_version_lt("6.2.0")
    def test_geoadd_nx(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )
        assert r.geoadd("a", values) == 2
        values = (
            (2.1909389952632, 41.433791470673, "place1")
            + (2.1873744593677, 41.406342043777, "place2")
            + (2.1804738294738, 41.405647879212, "place3")
        )
        assert r.geoadd("a", values, nx=True) == 1
        assert r.zrange("a", 0, -1) == [b"place3", b"place2", b"place1"]

    @skip_if_server_version_lt("6.2.0")
    def test_geoadd_xx(self, r):
        values = (2.1909389952632, 41.433791470673, "place1")
        assert r.geoadd("a", values) == 1
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )
        assert r.geoadd("a", values, xx=True) == 0
        assert r.zrange("a", 0, -1) == [b"place1"]

    @skip_if_server_version_lt("6.2.0")
    def test_geoadd_ch(self, r):
        values = (2.1909389952632, 41.433791470673, "place1")
        assert r.geoadd("a", values) == 1
        values = (2.1909389952632, 31.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )
        assert r.geoadd("a", values, ch=True) == 2
        assert r.zrange("a", 0, -1) == [b"place1", b"place2"]

    @skip_if_server_version_lt("3.2.0")
    def test_geoadd_invalid_params(self, r):
        with pytest.raises(exceptions.RedisError):
            r.geoadd("barcelona", (1, 2))

    @skip_if_server_version_lt("3.2.0")
    def test_geodist(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )
        assert r.geoadd("barcelona", values) == 2
        assert r.geodist("barcelona", "place1", "place2") == 3067.4157

    @skip_if_server_version_lt("3.2.0")
    def test_geodist_units(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )
        r.geoadd("barcelona", values)
        assert r.geodist("barcelona", "place1", "place2", "km") == 3.0674

    @skip_if_server_version_lt("3.2.0")
    def test_geodist_missing_one_member(self, r):
        values = (2.1909389952632, 41.433791470673, "place1")
        r.geoadd("barcelona", values)
        assert r.geodist("barcelona", "place1", "missing_member", "km") is None

    @skip_if_server_version_lt("3.2.0")
    def test_geodist_invalid_units(self, r):
        with pytest.raises(exceptions.RedisError):
            assert r.geodist("x", "y", "z", "inches")

    @skip_if_server_version_lt("3.2.0")
    def test_geohash(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )
        r.geoadd("barcelona", values)
        assert_resp_response(
            r,
            r.geohash("barcelona", "place1", "place2", "place3"),
            ["sp3e9yg3kd0", "sp3e9cbc3t0", None],
            [b"sp3e9yg3kd0", b"sp3e9cbc3t0", None],
            ["sp3e9yg3kd0", "sp3e9cbc3t0", None],
        )

    @skip_unless_arch_bits(64)
    @skip_if_server_version_lt("3.2.0")
    def test_geopos(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )
        r.geoadd("barcelona", values)
        # redis uses 52 bits precision, hereby small errors may be introduced.
        assert_resp_response(
            r,
            r.geopos("barcelona", "place1", "place2"),
            [
                (2.19093829393386841, 41.43379028184083523),
                (2.18737632036209106, 41.40634178640635099),
            ],
            [
                [2.19093829393386841, 41.43379028184083523],
                [2.18737632036209106, 41.40634178640635099],
            ],
        )

    @skip_if_server_version_lt("4.0.0")
    def test_geopos_no_value(self, r):
        assert r.geopos("barcelona", "place1", "place2") == [None, None]

    @skip_if_server_version_lt("3.2.0")
    @skip_if_server_version_gte("4.0.0")
    def test_old_geopos_no_value(self, r):
        assert r.geopos("barcelona", "place1", "place2") == []

    @skip_if_server_version_lt("6.2.0")
    def test_geosearch(self, r):
        values = (
            (2.1909389952632, 41.433791470673, "place1")
            + (2.1873744593677, 41.406342043777, b"\x80place2")
            + (2.583333, 41.316667, "place3")
        )
        r.geoadd("barcelona", values)
        assert r.geosearch(
            "barcelona", longitude=2.191, latitude=41.433, radius=1000
        ) == [b"place1"]
        assert r.geosearch(
            "barcelona", longitude=2.187, latitude=41.406, radius=1000
        ) == [b"\x80place2"]
        assert r.geosearch(
            "barcelona", longitude=2.191, latitude=41.433, height=1000, width=1000
        ) == [b"place1"]
        assert r.geosearch("barcelona", member="place3", radius=100, unit="km") == [
            b"\x80place2",
            b"place1",
            b"place3",
        ]
        # test count
        assert r.geosearch(
            "barcelona", member="place3", radius=100, unit="km", count=2
        ) == [b"place3", b"\x80place2"]
        assert r.geosearch(
            "barcelona", member="place3", radius=100, unit="km", count=1, any=1
        )[0] in [b"place1", b"place3", b"\x80place2"]

    @skip_unless_arch_bits(64)
    @skip_if_server_version_lt("6.2.0")
    def test_geosearch_member(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            b"\x80place2",
        )

        r.geoadd("barcelona", values)
        assert r.geosearch("barcelona", member="place1", radius=4000) == [
            b"\x80place2",
            b"place1",
        ]
        assert r.geosearch("barcelona", member="place1", radius=10) == [b"place1"]

        assert r.geosearch(
            "barcelona",
            member="place1",
            radius=4000,
            withdist=True,
            withcoord=True,
            withhash=True,
        ) == [
            [
                b"\x80place2",
                3067.4157,
                3471609625421029,
                (2.187376320362091, 41.40634178640635),
            ],
            [
                b"place1",
                0.0,
                3471609698139488,
                (2.1909382939338684, 41.433790281840835),
            ],
        ]

    @skip_if_server_version_lt("6.2.0")
    def test_geosearch_sort(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )
        r.geoadd("barcelona", values)
        assert r.geosearch(
            "barcelona", longitude=2.191, latitude=41.433, radius=3000, sort="ASC"
        ) == [b"place1", b"place2"]
        assert r.geosearch(
            "barcelona", longitude=2.191, latitude=41.433, radius=3000, sort="DESC"
        ) == [b"place2", b"place1"]

    @skip_unless_arch_bits(64)
    @skip_if_server_version_lt("6.2.0")
    def test_geosearch_with(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )
        r.geoadd("barcelona", values)
        coord_tuple = (2.19093829393386841, 41.43379028184083523)

        # test a bunch of combinations to test the parse response
        # function.
        assert r.geosearch(
            "barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=1,
            unit="km",
            withdist=True,
            withcoord=True,
            withhash=True,
        ) == [[b"place1", 0.0881, 3471609698139488, coord_tuple]]
        assert r.geosearch(
            "barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=1,
            unit="km",
            withdist=True,
            withcoord=True,
        ) == [[b"place1", 0.0881, coord_tuple]]
        assert r.geosearch(
            "barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=1,
            unit="km",
            withhash=True,
            withcoord=True,
        ) == [[b"place1", 3471609698139488, coord_tuple]]
        # test no values.
        assert (
            r.geosearch(
                "barcelona",
                longitude=2,
                latitude=1,
                radius=1,
                unit="km",
                withdist=True,
                withcoord=True,
                withhash=True,
            )
            == []
        )

    @skip_if_server_version_lt("6.2.0")
    def test_geosearch_negative(self, r):
        # not specifying member nor longitude and latitude
        with pytest.raises(exceptions.DataError):
            assert r.geosearch("barcelona")
        # specifying member and longitude and latitude
        with pytest.raises(exceptions.DataError):
            assert r.geosearch("barcelona", member="Paris", longitude=2, latitude=1)
        # origin (0, 0) is a valid coordinate and must still conflict with member
        with pytest.raises(exceptions.DataError):
            r.geosearch("barcelona", member="Paris", longitude=0, latitude=0, radius=10)
        # specifying one of longitude and latitude
        with pytest.raises(exceptions.DataError):
            assert r.geosearch("barcelona", longitude=2)
        with pytest.raises(exceptions.DataError):
            assert r.geosearch("barcelona", latitude=2)

        # not specifying radius nor width and height
        with pytest.raises(exceptions.DataError):
            assert r.geosearch("barcelona", member="Paris")
        # specifying radius and width and height
        with pytest.raises(exceptions.DataError):
            assert r.geosearch("barcelona", member="Paris", radius=3, width=2, height=1)
        # specifying one of width and height
        with pytest.raises(exceptions.DataError):
            assert r.geosearch("barcelona", member="Paris", width=2)
        with pytest.raises(exceptions.DataError):
            assert r.geosearch("barcelona", member="Paris", height=2)

        # invalid sort
        with pytest.raises(exceptions.DataError):
            assert r.geosearch(
                "barcelona", member="Paris", width=2, height=2, sort="wrong"
            )

        # invalid unit
        with pytest.raises(exceptions.DataError):
            assert r.geosearch(
                "barcelona", member="Paris", width=2, height=2, unit="miles"
            )

        # use any without count
        with pytest.raises(exceptions.DataError):
            assert r.geosearch("barcelona", member="place3", radius=100, any=1)

    @pytest.mark.fixed_client
    def test_geosearch_forwards_zero_numeric_arguments(self):
        # GEOSEARCH zero arguments must be forwarded for server validation.
        # Truthiness guards dropped those values, so the
        # command on the wire omitted BYRADIUS/BYBOX/COUNT/FROMMEMBER or sent
        # both FROMMEMBER and FROMLONLAT. Runs without a server.
        client = redis.Redis()

        def wire_args(**kwargs):
            with mock.patch.object(client, "execute_command", return_value=[]) as m:
                client.geosearch("places", **kwargs)
                return list(m.call_args[0])

        origin_radius = wire_args(longitude=0, latitude=0, radius=0, unit="m")
        assert origin_radius[2:5] == [b"FROMLONLAT", 0, 0]
        assert origin_radius[-3:] == [b"BYRADIUS", 0, "m"]

        member_zero = wire_args(member=0, radius=10, unit="km")
        assert member_zero[2:4] == [b"FROMMEMBER", 0]
        assert member_zero[-3:] == [b"BYRADIUS", 10, "km"]

        box_zero_width = wire_args(longitude=1, latitude=2, width=0, height=1, unit="m")
        assert box_zero_width[-4:] == [b"BYBOX", 0, 1, "m"]

        count_zero = wire_args(longitude=1, latitude=2, radius=10, count=0, unit="m")
        assert count_zero[-2:] == [b"COUNT", 0]
        assert b"COUNT" not in wire_args(longitude=1, latitude=2, radius=10, unit="m")

        store_radius_zero = []
        with mock.patch.object(client, "execute_command", return_value=0) as m:
            client.geosearchstore("dest", "places", longitude=0, latitude=0, radius=0)
            store_radius_zero = list(m.call_args[0])
        assert store_radius_zero[:3] == ["GEOSEARCHSTORE", "dest", "places"]
        assert store_radius_zero[-3:] == [b"BYRADIUS", 0, "m"]

        with pytest.raises(exceptions.DataError):
            client.geosearch(
                "places", member="Paris", longitude=0, latitude=0, radius=10
            )

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    def test_geosearchstore(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("barcelona", values)
        r.geosearchstore(
            "places_barcelona",
            "barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=1000,
        )
        assert r.zrange("places_barcelona", 0, -1) == [b"place1"]

    @pytest.mark.onlynoncluster
    @skip_unless_arch_bits(64)
    @skip_if_server_version_lt("6.2.0")
    def test_geosearchstore_dist(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("barcelona", values)
        r.geosearchstore(
            "places_barcelona",
            "barcelona",
            longitude=2.191,
            latitude=41.433,
            radius=1000,
            storedist=True,
        )
        # instead of save the geo score, the distance is saved.
        assert r.zscore("places_barcelona", "place1") == 88.05060698409301

    @skip_if_server_version_lt("3.2.0")
    def test_georadius_Issue2609(self, r):
        # test for issue #2609 (Geo search functions don't work with execute_command)
        r.geoadd(name="my-key", values=[1, 2, "data"])
        assert r.execute_command("GEORADIUS", "my-key", 1, 2, 400, "m") == [b"data"]

    @skip_if_server_version_lt("3.2.0")
    def test_georadius(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            b"\x80place2",
        )

        r.geoadd("barcelona", values)
        assert r.georadius("barcelona", 2.191, 41.433, 1000) == [b"place1"]
        assert r.georadius("barcelona", 2.187, 41.406, 1000) == [b"\x80place2"]

    @skip_if_server_version_lt("3.2.0")
    def test_georadius_no_values(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("barcelona", values)
        assert r.georadius("barcelona", 1, 2, 1000) == []

    @skip_if_server_version_lt("3.2.0")
    def test_georadius_units(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("barcelona", values)
        assert r.georadius("barcelona", 2.191, 41.433, 1, unit="km") == [b"place1"]

    @skip_unless_arch_bits(64)
    @skip_if_server_version_lt("3.2.0")
    def test_georadius_with(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("barcelona", values)

        # test a bunch of combinations to test the parse response
        # function.
        assert r.georadius(
            "barcelona",
            2.191,
            41.433,
            1,
            unit="km",
            withdist=True,
            withcoord=True,
            withhash=True,
        ) == [
            [
                b"place1",
                0.0881,
                3471609698139488,
                (2.19093829393386841, 41.43379028184083523),
            ]
        ]

        assert r.georadius(
            "barcelona", 2.191, 41.433, 1, unit="km", withdist=True, withcoord=True
        ) == [[b"place1", 0.0881, (2.19093829393386841, 41.43379028184083523)]]

        assert r.georadius(
            "barcelona", 2.191, 41.433, 1, unit="km", withhash=True, withcoord=True
        ) == [
            [b"place1", 3471609698139488, (2.19093829393386841, 41.43379028184083523)]
        ]

        # test no values.
        assert (
            r.georadius(
                "barcelona",
                2,
                1,
                1,
                unit="km",
                withdist=True,
                withcoord=True,
                withhash=True,
            )
            == []
        )

    @skip_if_server_version_lt("6.2.0")
    def test_georadius_count(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("barcelona", values)
        assert r.georadius("barcelona", 2.191, 41.433, 3000, count=1) == [b"place1"]
        assert r.georadius("barcelona", 2.191, 41.433, 3000, count=1, any=True) == [
            b"place2"
        ]

    @skip_if_server_version_lt("3.2.0")
    def test_georadius_sort(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("barcelona", values)
        assert r.georadius("barcelona", 2.191, 41.433, 3000, sort="ASC") == [
            b"place1",
            b"place2",
        ]
        assert r.georadius("barcelona", 2.191, 41.433, 3000, sort="DESC") == [
            b"place2",
            b"place1",
        ]

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("3.2.0")
    def test_georadius_store(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("barcelona", values)
        r.georadius("barcelona", 2.191, 41.433, 1000, store="places_barcelona")
        assert r.zrange("places_barcelona", 0, -1) == [b"place1"]

    @pytest.mark.onlynoncluster
    @skip_unless_arch_bits(64)
    @skip_if_server_version_lt("3.2.0")
    def test_georadius_store_dist(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        r.geoadd("barcelona", values)
        r.georadius("barcelona", 2.191, 41.433, 1000, store_dist="places_barcelona")
        # instead of save the geo score, the distance is saved.
        assert r.zscore("places_barcelona", "place1") == 88.05060698409301

    @skip_unless_arch_bits(64)
    @skip_if_server_version_lt("3.2.0")
    def test_georadiusmember(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            b"\x80place2",
        )

        r.geoadd("barcelona", values)
        assert r.georadiusbymember("barcelona", "place1", 4000) == [
            b"\x80place2",
            b"place1",
        ]
        assert r.georadiusbymember("barcelona", "place1", 10) == [b"place1"]

        assert r.georadiusbymember(
            "barcelona", "place1", 4000, withdist=True, withcoord=True, withhash=True
        ) == [
            [
                b"\x80place2",
                3067.4157,
                3471609625421029,
                (2.187376320362091, 41.40634178640635),
            ],
            [
                b"place1",
                0.0,
                3471609698139488,
                (2.1909382939338684, 41.433790281840835),
            ],
        ]

    @skip_if_server_version_lt("6.2.0")
    def test_georadiusmember_count(self, r):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            b"\x80place2",
        )
        r.geoadd("barcelona", values)
        assert r.georadiusbymember("barcelona", "place1", 4000, count=1, any=True) == [
            b"\x80place2"
        ]

    @skip_if_server_version_lt("5.0.0")
    def test_xack(self, r):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        # xack on a stream that doesn't exist
        assert r.xack(stream, group, "0-0") == 0

        m1 = r.xadd(stream, {"one": "one"})
        m2 = r.xadd(stream, {"two": "two"})
        m3 = r.xadd(stream, {"three": "three"})

        # xack on a group that doesn't exist
        assert r.xack(stream, group, m1) == 0

        r.xgroup_create(stream, group, 0)
        r.xreadgroup(group, consumer, streams={stream: ">"})
        # xack returns the number of ack'd elements
        assert r.xack(stream, group, m1) == 1
        assert r.xack(stream, group, m2, m3) == 2

    @skip_if_server_version_lt("5.0.0")
    def test_xadd(self, r):
        stream = "stream"
        message_id = r.xadd(stream, {"foo": "bar"})
        assert re.match(rb"[0-9]+\-[0-9]+", message_id)

        # explicit message id
        message_id = b"9999999999999999999-0"
        assert message_id == r.xadd(stream, {"foo": "bar"}, id=message_id)

        # with maxlen, the list evicts the first message
        r.xadd(stream, {"foo": "bar"}, maxlen=2, approximate=False)
        assert r.xlen(stream) == 2

    @skip_if_server_version_lt("6.2.0")
    def test_xadd_nomkstream(self, r):
        # nomkstream option
        stream = "stream"
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"some": "other"}, nomkstream=False)
        assert r.xlen(stream) == 2
        r.xadd(stream, {"some": "other"}, nomkstream=True)
        assert r.xlen(stream) == 3

    @skip_if_server_version_lt("6.2.0")
    def test_xadd_minlen_and_limit(self, r):
        stream = "stream"

        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})

        # Future self: No limits without approximate, according to the api
        with pytest.raises(redis.ResponseError):
            assert r.xadd(stream, {"foo": "bar"}, maxlen=3, approximate=False, limit=2)

        # limit can not be provided without maxlen or minid
        with pytest.raises(redis.ResponseError):
            assert r.xadd(stream, {"foo": "bar"}, limit=2)

        # maxlen with a limit
        assert r.xadd(stream, {"foo": "bar"}, maxlen=3, approximate=True, limit=2)
        r.delete(stream)

        # maxlen and minid can not be provided together
        with pytest.raises(redis.DataError):
            assert r.xadd(stream, {"foo": "bar"}, maxlen=3, minid="sometestvalue")

        # minid with a limit
        m1 = r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        assert r.xadd(stream, {"foo": "bar"}, approximate=True, minid=m1, limit=3)

        # pure minid
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        m4 = r.xadd(stream, {"foo": "bar"})
        assert r.xadd(stream, {"foo": "bar"}, approximate=False, minid=m4)

        # minid approximate
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        m3 = r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        assert r.xadd(stream, {"foo": "bar"}, approximate=True, minid=m3)

    @skip_if_server_version_lt("7.0.0")
    def test_xadd_explicit_ms(self, r: redis.Redis):
        stream = "stream"
        message_id = r.xadd(stream, {"foo": "bar"}, "9999999999999999999-*")
        ms = message_id[: message_id.index(b"-")]
        assert ms == b"9999999999999999999"

    @skip_if_server_version_lt("7.0.0")
    def test_xautoclaim(self, r):
        stream = "stream"
        group = "group"
        consumer1 = "consumer1"
        consumer2 = "consumer2"

        message_id1 = r.xadd(stream, {"john": "wick"})
        message_id2 = r.xadd(stream, {"johny": "deff"})
        message = get_stream_message(r, stream, message_id1)
        r.xgroup_create(stream, group, 0)

        # trying to claim a message that isn't already pending doesn't
        # do anything
        response = r.xautoclaim(stream, group, consumer2, min_idle_time=0)
        assert response == [b"0-0", [], []]

        # read the group as consumer1 to initially claim the messages
        r.xreadgroup(group, consumer1, streams={stream: ">"})

        # claim one message as consumer2
        response = r.xautoclaim(stream, group, consumer2, min_idle_time=0, count=1)
        assert response[1] == [message]

        # reclaim the messages as consumer1, but use the justid argument
        # which only returns message ids
        assert r.xautoclaim(
            stream, group, consumer1, min_idle_time=0, start_id=0, justid=True
        ) == [message_id1, message_id2]
        assert r.xautoclaim(
            stream, group, consumer1, min_idle_time=0, start_id=message_id2, justid=True
        ) == [message_id2]

    @skip_if_server_version_lt("6.2.0")
    def test_xautoclaim_negative(self, r):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        with pytest.raises(redis.DataError):
            r.xautoclaim(stream, group, consumer, min_idle_time=-1)
        with pytest.raises(ValueError):
            r.xautoclaim(stream, group, consumer, min_idle_time="wrong")
        with pytest.raises(redis.DataError):
            r.xautoclaim(stream, group, consumer, min_idle_time=0, count=-1)

    @skip_if_server_version_lt("5.0.0")
    def test_xclaim(self, r):
        stream = "stream"
        group = "group"
        consumer1 = "consumer1"
        consumer2 = "consumer2"
        message_id = r.xadd(stream, {"john": "wick"})
        message = get_stream_message(r, stream, message_id)
        r.xgroup_create(stream, group, 0)

        # trying to claim a message that isn't already pending doesn't
        # do anything
        response = r.xclaim(
            stream, group, consumer2, min_idle_time=0, message_ids=(message_id,)
        )
        assert response == []

        # read the group as consumer1 to initially claim the messages
        r.xreadgroup(group, consumer1, streams={stream: ">"})

        # claim the message as consumer2
        response = r.xclaim(
            stream, group, consumer2, min_idle_time=0, message_ids=(message_id,)
        )
        assert response[0] == message

        # reclaim the message as consumer1, but use the justid argument
        # which only returns message ids
        assert r.xclaim(
            stream,
            group,
            consumer1,
            min_idle_time=0,
            message_ids=(message_id,),
            justid=True,
        ) == [message_id]

    @skip_if_server_version_lt("7.0.0")
    def test_xclaim_trimmed(self, r):
        # xclaim should not raise an exception if the item is not there
        stream = "stream"
        group = "group"

        r.xgroup_create(stream, group, id="$", mkstream=True)

        # add a couple of new items
        sid1 = r.xadd(stream, {"item": 0})
        sid2 = r.xadd(stream, {"item": 0})

        # read them from consumer1
        r.xreadgroup(group, "consumer1", {stream: ">"})

        # add a 3rd and trim the stream down to 2 items
        r.xadd(stream, {"item": 3}, maxlen=2, approximate=False)

        # xclaim them from consumer2
        # the item that is still in the stream should be returned
        item = r.xclaim(stream, group, "consumer2", 0, [sid1, sid2])
        assert len(item) == 1
        assert item[0][0] == sid2

    @skip_if_server_version_lt("5.0.0")
    def test_xdel(self, r):
        stream = "stream"

        # deleting from an empty stream doesn't do anything
        assert r.xdel(stream, 1) == 0

        m1 = r.xadd(stream, {"foo": "bar"})
        m2 = r.xadd(stream, {"foo": "bar"})
        m3 = r.xadd(stream, {"foo": "bar"})

        # xdel returns the number of deleted elements
        assert r.xdel(stream, m1) == 1
        assert r.xdel(stream, m2, m3) == 2

    @skip_if_server_version_lt("7.0.0")
    def test_xgroup_create(self, r):
        # tests xgroup_create and xinfo_groups
        stream = "stream"
        group = "group"
        r.xadd(stream, {"foo": "bar"})

        # no group is setup yet, no info to obtain
        assert r.xinfo_groups(stream) == []

        assert r.xgroup_create(stream, group, 0)
        expected = [
            {
                "name": group.encode(),
                "consumers": 0,
                "pending": 0,
                "last-delivered-id": b"0-0",
                "entries-read": None,
                "lag": 1,
            }
        ]
        assert r.xinfo_groups(stream) == expected

    @skip_if_server_version_lt("7.0.0")
    def test_xgroup_create_mkstream(self, r):
        # tests xgroup_create and xinfo_groups
        stream = "stream"
        group = "group"

        # an error is raised if a group is created on a stream that
        # doesn't already exist
        with pytest.raises(exceptions.ResponseError):
            r.xgroup_create(stream, group, 0)

        # however, with mkstream=True, the underlying stream is created
        # automatically
        assert r.xgroup_create(stream, group, 0, mkstream=True)
        expected = [
            {
                "name": group.encode(),
                "consumers": 0,
                "pending": 0,
                "last-delivered-id": b"0-0",
                "entries-read": None,
                "lag": 0,
            }
        ]
        assert r.xinfo_groups(stream) == expected

    @skip_if_server_version_lt("7.0.0")
    @skip_if_server_version_gte("8.2.2")
    def test_xgroup_create_entriesread(self, r: redis.Redis):
        stream = "stream"
        group = "group"
        r.xadd(stream, {"foo": "bar"})

        # no group is setup yet, no info to obtain
        assert r.xinfo_groups(stream) == []

        assert r.xgroup_create(stream, group, 0, entries_read=7)
        expected = [
            {
                "name": group.encode(),
                "consumers": 0,
                "pending": 0,
                "last-delivered-id": b"0-0",
                "entries-read": 7,
                "lag": 1,
            }
        ]
        assert r.xinfo_groups(stream) == expected

    @skip_if_server_version_lt("8.2.2")
    def test_xgroup_create_entriesread_fixed_max_entries_read(self, r: redis.Redis):
        stream = "stream"
        group = "group"
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo1": "bar1"})
        r.xadd(stream, {"foo2": "bar2"})

        # no group is setup yet, no info to obtain
        assert r.xinfo_groups(stream) == []

        assert r.xgroup_create(stream, group, 0, entries_read=7)
        # validate the entries-read is max the number of entries
        # in the stream and lag shows the entries between
        # last_delivered_id and entries_added
        expected = [
            {
                "name": group.encode(),
                "consumers": 0,
                "pending": 0,
                "last-delivered-id": b"0-0",
                "entries-read": 3,
                "lag": 3,
            }
        ]
        assert r.xinfo_groups(stream) == expected

    @skip_if_server_version_lt("5.0.0")
    def test_xgroup_delconsumer(self, r):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xgroup_create(stream, group, 0)

        # a consumer that hasn't yet read any messages doesn't do anything
        assert r.xgroup_delconsumer(stream, group, consumer) == 0

        # read all messages from the group
        r.xreadgroup(group, consumer, streams={stream: ">"})

        # deleting the consumer should return 2 pending messages
        assert r.xgroup_delconsumer(stream, group, consumer) == 2

    @skip_if_server_version_lt("6.2.0")
    def test_xgroup_createconsumer(self, r):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xgroup_create(stream, group, 0)
        assert r.xgroup_createconsumer(stream, group, consumer) == 1

        # read all messages from the group
        r.xreadgroup(group, consumer, streams={stream: ">"})

        # deleting the consumer should return 2 pending messages
        assert r.xgroup_delconsumer(stream, group, consumer) == 2

    @skip_if_server_version_lt("5.0.0")
    def test_xgroup_destroy(self, r):
        stream = "stream"
        group = "group"
        r.xadd(stream, {"foo": "bar"})

        # destroying a nonexistent group returns False
        assert not r.xgroup_destroy(stream, group)

        r.xgroup_create(stream, group, 0)
        assert r.xgroup_destroy(stream, group)

    @skip_if_server_version_lt("7.0.0")
    @skip_if_server_version_gte("8.2.2")
    def test_xgroup_setid(self, r):
        stream = "stream"
        group = "group"
        message_id = r.xadd(stream, {"foo": "bar"})

        r.xgroup_create(stream, group, 0)
        # advance the last_delivered_id to the message_id
        r.xgroup_setid(stream, group, message_id, entries_read=2)
        expected = [
            {
                "name": group.encode(),
                "consumers": 0,
                "pending": 0,
                "last-delivered-id": message_id,
                "entries-read": 2,
                "lag": -1,
            }
        ]
        assert r.xinfo_groups(stream) == expected

    @skip_if_server_version_lt("8.2.2")
    def test_xgroup_setid_fixed_max_entries_read(self, r):
        stream = "stream"
        group = "group"
        message_id = r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo1": "bar1"})

        r.xgroup_create(stream, group, 0)
        # advance the last_delivered_id to the message_id
        r.xgroup_setid(stream, group, message_id, entries_read=5)
        expected = [
            {
                "name": group.encode(),
                "consumers": 0,
                "pending": 0,
                "last-delivered-id": message_id,
                "entries-read": 2,
                "lag": 0,
            }
        ]
        assert r.xinfo_groups(stream) == expected

    @skip_if_server_version_lt("7.2.0")
    def test_xinfo_consumers(self, r):
        stream = "stream"
        group = "group"
        consumer1 = "consumer1"
        consumer2 = "consumer2"
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})

        r.xgroup_create(stream, group, 0)
        r.xreadgroup(group, consumer1, streams={stream: ">"}, count=1)
        r.xreadgroup(group, consumer2, streams={stream: ">"})
        info = r.xinfo_consumers(stream, group)
        assert len(info) == 2
        expected = [
            {"name": consumer1.encode(), "pending": 1},
            {"name": consumer2.encode(), "pending": 2},
        ]

        # we can't determine the idle and inactive time, so just make sure it's an int
        assert isinstance(info[0].pop("idle"), int)
        assert isinstance(info[1].pop("idle"), int)
        assert isinstance(info[0].pop("inactive"), int)
        assert isinstance(info[1].pop("inactive"), int)
        assert info == expected

    @skip_if_server_version_lt("7.0.0")
    def test_xinfo_stream(self, r):
        stream = "stream"
        m1 = r.xadd(stream, {"foo": "bar"})
        m2 = r.xadd(stream, {"foo": "bar"})
        info = r.xinfo_stream(stream)

        assert info["length"] == 2
        assert info["first-entry"] == get_stream_message(r, stream, m1)
        assert info["last-entry"] == get_stream_message(r, stream, m2)
        assert info["max-deleted-entry-id"] == b"0-0"
        assert info["entries-added"] == 2
        assert info["recorded-first-entry-id"] == m1

        r.xtrim(stream, 0)
        info = r.xinfo_stream(stream)
        assert info["length"] == 0
        assert info["first-entry"] is None
        assert info["last-entry"] is None

    @skip_if_server_version_lt("6.0.0")
    def test_xinfo_stream_full(self, r):
        stream = "stream"
        group = "group"
        m1 = r.xadd(stream, {"foo": "bar"})
        info = r.xinfo_stream(stream, full=True)
        assert info["length"] == 1
        assert len(info["groups"]) == 0

        r.xgroup_create(stream, group, 0)
        info = r.xinfo_stream(stream, full=True)
        assert info["length"] == 1
        assert_resp_response_in(
            r,
            m1,
            info["entries"],
            info["entries"].keys(),
        )
        assert len(info["groups"]) == 1

        r.xreadgroup(group, "consumer", streams={stream: ">"})
        info = r.xinfo_stream(stream, full=True)
        consumer = info["groups"][0]["consumers"][0]
        assert isinstance(consumer, dict)
        assert consumer["name"] == b"consumer"
        assert consumer["pel-count"] == 1

    @skip_if_server_version_lt("8.5.0")
    def test_xinfo_stream_idempotent_fields(self, r):
        stream = "stream"

        # Create stream with regular entry
        r.xadd(stream, {"foo": "bar"})
        info = r.xinfo_stream(stream)

        # Verify new idempotent producer fields are present with default values
        assert "idmp-duration" in info
        assert "idmp-maxsize" in info
        assert "pids-tracked" in info
        assert "iids-tracked" in info
        assert "iids-added" in info
        assert "iids-duplicates" in info

        # Default values (before any idempotent entries)
        assert info["pids-tracked"] == 0
        assert info["iids-tracked"] == 0
        assert info["iids-added"] == 0
        assert info["iids-duplicates"] == 0

        # Add idempotent entry
        r.xadd(stream, {"field1": "value1"}, idmpauto="producer1")
        info = r.xinfo_stream(stream)

        # After adding idempotent entry
        assert info["pids-tracked"] == 1  # One producer tracked
        assert info["iids-tracked"] == 1  # One iid tracked
        assert info["iids-added"] == 1  # One idempotent entry added
        assert info["iids-duplicates"] == 0  # No duplicates yet

        # Add duplicate entry
        r.xadd(stream, {"field1": "value1"}, idmpauto="producer1")
        info = r.xinfo_stream(stream)

        # After duplicate
        assert info["pids-tracked"] == 1  # Still one producer
        assert info["iids-tracked"] == 1  # Still one iid (duplicate doesn't add new)
        assert info["iids-added"] == 1  # Still one unique entry
        assert info["iids-duplicates"] == 1  # One duplicate detected

        # Add entry from different producer
        r.xadd(stream, {"field2": "value2"}, idmpauto="producer2")
        info = r.xinfo_stream(stream)

        # After second producer
        assert info["pids-tracked"] == 2  # Two producers tracked
        assert info["iids-tracked"] == 2  # Two iids tracked
        assert info["iids-added"] == 2  # Two unique entries
        assert info["iids-duplicates"] == 1  # Still one duplicate

    @skip_if_server_version_lt("5.0.0")
    def test_xlen(self, r):
        stream = "stream"
        assert r.xlen(stream) == 0
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        assert r.xlen(stream) == 2

    @skip_if_server_version_lt("8.7.2")
    def test_xnack_silent(self, r):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        m1 = r.xadd(stream, {"foo": "bar"})
        m2 = r.xadd(stream, {"foo": "bar"})
        r.xgroup_create(stream, group, 0)
        r.xreadgroup(group, consumer, streams={stream: ">"})
        # SILENT mode returns count of NACKed messages
        result = r.xnack(stream, group, "SILENT", m1, m2)
        assert result == 2

    @skip_if_server_version_lt("8.7.2")
    def test_xnack_fail(self, r):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        m1 = r.xadd(stream, {"foo": "bar"})
        r.xgroup_create(stream, group, 0)
        r.xreadgroup(group, consumer, streams={stream: ">"})
        # FAIL mode returns count of NACKed messages
        result = r.xnack(stream, group, "FAIL", m1)
        assert result == 1

    @skip_if_server_version_lt("8.7.2")
    def test_xnack_fatal(self, r):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        m1 = r.xadd(stream, {"foo": "bar"})
        r.xgroup_create(stream, group, 0)
        r.xreadgroup(group, consumer, streams={stream: ">"})
        # FATAL mode returns count of NACKed messages
        result = r.xnack(stream, group, "FATAL", m1)
        assert result == 1

    @skip_if_server_version_lt("8.7.2")
    def test_xnack_multiple_ids(self, r):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        m1 = r.xadd(stream, {"foo": "bar"})
        m2 = r.xadd(stream, {"foo": "bar"})
        m3 = r.xadd(stream, {"foo": "bar"})
        r.xgroup_create(stream, group, 0)
        r.xreadgroup(group, consumer, streams={stream: ">"})
        result = r.xnack(stream, group, "FAIL", m1, m2, m3)
        assert result == 3

    @skip_if_server_version_lt("8.7.2")
    def test_xnack_some_ids_not_in_pel(self, r):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        m1 = r.xadd(stream, {"foo": "bar"})
        m2 = r.xadd(stream, {"foo": "bar"})
        r.xgroup_create(stream, group, 0)
        r.xreadgroup(group, consumer, streams={stream: ">"})
        # Only m1 and m2 are in PEL; "999999-0" is not
        result = r.xnack(stream, group, "FAIL", m1, m2, "999999-0")
        assert result == 2

    @skip_if_server_version_lt("8.7.2")
    def test_xnack_retrycount(self, r):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        m1 = r.xadd(stream, {"foo": "bar"})
        r.xgroup_create(stream, group, 0)
        r.xreadgroup(group, consumer, streams={stream: ">"})
        # Explicit retrycount overrides mode's counter adjustment
        result = r.xnack(stream, group, "FAIL", m1, retrycount=5)
        assert result == 1

    @skip_if_server_version_lt("8.7.2")
    def test_xnack_force(self, r):
        stream = "stream"
        group = "group"
        m1 = r.xadd(stream, {"foo": "bar"})
        r.xgroup_create(stream, group, 0)
        # FORCE creates unowned PEL entries for IDs not already in PEL
        result = r.xnack(stream, group, "FAIL", m1, force=True)
        assert result == 1

    @skip_if_server_version_lt("8.7.2")
    def test_xnack_invalid_mode(self, r):
        stream = "stream"
        group = "group"
        m1 = r.xadd(stream, {"foo": "bar"})
        r.xgroup_create(stream, group, 0)
        with pytest.raises(redis.DataError):
            r.xnack(stream, group, "INVALID", m1)

    @skip_if_server_version_lt("8.7.2")
    def test_xnack_no_ids(self, r):
        stream = "stream"
        group = "group"
        r.xadd(stream, {"foo": "bar"})
        r.xgroup_create(stream, group, 0)
        with pytest.raises(redis.DataError):
            r.xnack(stream, group, "FAIL")

    @skip_if_server_version_lt("8.7.2")
    def test_xnack_negative_retrycount(self, r):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        m1 = r.xadd(stream, {"foo": "bar"})
        r.xgroup_create(stream, group, 0)
        r.xreadgroup(group, consumer, streams={stream: ">"})
        with pytest.raises(redis.DataError):
            r.xnack(stream, group, "FAIL", m1, retrycount=-1)

    @skip_if_server_version_lt("5.0.0")
    def test_xpending(self, r):
        stream = "stream"
        group = "group"
        consumer1 = "consumer1"
        consumer2 = "consumer2"
        m1 = r.xadd(stream, {"foo": "bar"})
        m2 = r.xadd(stream, {"foo": "bar"})
        r.xgroup_create(stream, group, 0)

        # xpending on a group that has no consumers yet
        expected = {"pending": 0, "min": None, "max": None, "consumers": []}
        assert r.xpending(stream, group) == expected

        # read 1 message from the group with each consumer
        r.xreadgroup(group, consumer1, streams={stream: ">"}, count=1)
        r.xreadgroup(group, consumer2, streams={stream: ">"}, count=1)

        expected = {
            "pending": 2,
            "min": m1,
            "max": m2,
            "consumers": [
                {"name": consumer1.encode(), "pending": 1},
                {"name": consumer2.encode(), "pending": 1},
            ],
        }
        assert r.xpending(stream, group) == expected

    @skip_if_server_version_lt("5.0.0")
    def test_xpending_range(self, r):
        stream = "stream"
        group = "group"
        consumer1 = "consumer1"
        consumer2 = "consumer2"
        m1 = r.xadd(stream, {"foo": "bar"})
        m2 = r.xadd(stream, {"foo": "bar"})
        r.xgroup_create(stream, group, 0)

        # xpending range on a group that has no consumers yet
        assert r.xpending_range(stream, group, min="-", max="+", count=5) == []

        # read 1 message from the group with each consumer
        r.xreadgroup(group, consumer1, streams={stream: ">"}, count=1)
        r.xreadgroup(group, consumer2, streams={stream: ">"}, count=1)

        response = r.xpending_range(stream, group, min="-", max="+", count=5)
        assert len(response) == 2
        assert response[0]["message_id"] == m1
        assert response[0]["consumer"] == consumer1.encode()
        assert response[1]["message_id"] == m2
        assert response[1]["consumer"] == consumer2.encode()

        # test with consumer name
        response = r.xpending_range(
            stream, group, min="-", max="+", count=5, consumername=consumer1
        )
        assert response[0]["message_id"] == m1
        assert response[0]["consumer"] == consumer1.encode()

    @skip_if_server_version_lt("6.2.0")
    def test_xpending_range_idle(self, r):
        stream = "stream"
        group = "group"
        consumer1 = "consumer1"
        consumer2 = "consumer2"
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xgroup_create(stream, group, 0)

        # read 1 message from the group with each consumer
        r.xreadgroup(group, consumer1, streams={stream: ">"}, count=1)
        r.xreadgroup(group, consumer2, streams={stream: ">"}, count=1)

        response = r.xpending_range(stream, group, min="-", max="+", count=5)
        assert len(response) == 2
        response = r.xpending_range(stream, group, min="-", max="+", count=5, idle=1000)
        assert len(response) == 0

    def test_xpending_range_negative(self, r):
        stream = "stream"
        group = "group"
        with pytest.raises(redis.DataError):
            r.xpending_range(stream, group, min="-", max="+", count=None)
        with pytest.raises(ValueError):
            r.xpending_range(stream, group, min="-", max="+", count="one")
        with pytest.raises(redis.DataError):
            r.xpending_range(stream, group, min="-", max="+", count=-1)
        with pytest.raises(ValueError):
            r.xpending_range(stream, group, min="-", max="+", count=5, idle="one")
        with pytest.raises(redis.exceptions.ResponseError):
            r.xpending_range(stream, group, min="-", max="+", count=5, idle=1.5)
        with pytest.raises(redis.DataError):
            r.xpending_range(stream, group, min="-", max="+", count=5, idle=-1)
        with pytest.raises(redis.DataError):
            r.xpending_range(stream, group, min=None, max=None, count=None, idle=0)
        with pytest.raises(redis.DataError):
            r.xpending_range(
                stream, group, min=None, max=None, count=None, consumername=0
            )

    @skip_if_server_version_lt("5.0.0")
    @pytest.mark.parametrize("consumer", ["", b"", "consumer1", b"consumer1"])
    def test_xpending_range_consumer_filter(self, r, consumer):
        stream, group = "stream", "group"
        first = r.xadd(stream, {"foo": "bar"})
        second = r.xadd(stream, {"foo": "baz"})
        r.xgroup_create(stream, group, 0)
        r.xreadgroup(group, consumer, streams={stream: ">"}, count=1)
        r.xreadgroup(group, "other", streams={stream: ">"}, count=1)

        pending = r.xpending_range(stream, group, "-", "+", 5)
        assert [item["message_id"] for item in pending] == [first, second]

        filtered = r.xpending_range(stream, group, "-", "+", 5, consumername=consumer)
        assert [item["message_id"] for item in filtered] == [first]
        expected_consumer = consumer.encode() if isinstance(consumer, str) else consumer
        assert filtered[0]["consumer"] == expected_consumer

    @skip_if_server_version_lt("5.0.0")
    def test_xrange(self, r):
        stream = "stream"
        m1 = r.xadd(stream, {"foo": "bar"})
        m2 = r.xadd(stream, {"foo": "bar"})
        m3 = r.xadd(stream, {"foo": "bar"})
        m4 = r.xadd(stream, {"foo": "bar"})

        def get_ids(results):
            return [result[0] for result in results]

        results = r.xrange(stream, min=m1)
        assert get_ids(results) == [m1, m2, m3, m4]

        results = r.xrange(stream, min=m2, max=m3)
        assert get_ids(results) == [m2, m3]

        results = r.xrange(stream, max=m3)
        assert get_ids(results) == [m1, m2, m3]

        results = r.xrange(stream, max=m2, count=1)
        assert get_ids(results) == [m1]

    @skip_if_server_version_lt("5.0.0")
    def test_xread(self, r):
        stream = "stream"
        m1 = r.xadd(stream, {"foo": "bar"})
        m2 = r.xadd(stream, {"bing": "baz"})

        stream_name = stream.encode()
        expected_entries = [
            get_stream_message(r, stream, m1),
            get_stream_message(r, stream, m2),
        ]
        # xread starting at 0 returns both messages
        assert_resp_response(
            r,
            r.xread(streams={stream: 0}),
            [[stream_name, expected_entries]],
            {stream_name: [expected_entries]},
            {stream_name: expected_entries},
        )

        expected_entries = [get_stream_message(r, stream, m1)]
        # xread starting at 0 and count=1 returns only the first message
        assert_resp_response(
            r,
            r.xread(streams={stream: 0}, count=1),
            [[stream_name, expected_entries]],
            {stream_name: [expected_entries]},
            {stream_name: expected_entries},
        )

        expected_entries = [get_stream_message(r, stream, m2)]
        # xread starting at m1 returns only the second message
        assert_resp_response(
            r,
            r.xread(streams={stream: m1}),
            [[stream_name, expected_entries]],
            {stream_name: [expected_entries]},
            {stream_name: expected_entries},
        )

        # xread starting at the last message returns an empty list
        assert_resp_response(r, r.xread(streams={stream: m2}), [], {})

    def _total_stream_entries(self, r, response):
        """Count entries across all streams regardless of response shape."""
        shape = expected_response_shape(r)
        if shape == "legacy_resp2":
            return sum(len(item[1]) for item in response)
        elif shape == "legacy_resp3":
            return sum(len(entries[0]) for entries in response.values())
        else:
            return sum(len(entries) for entries in response.values())

    def test_xread_max_count_max_size_validation(self, r):
        with pytest.raises(DataError):
            r.xread(streams={"stream": 0}, max_count=0)
        with pytest.raises(DataError):
            r.xread(streams={"stream": 0}, max_count=-1)
        with pytest.raises(DataError):
            r.xread(streams={"stream": 0}, max_size=0)
        # max_count must be >= count when both are provided
        with pytest.raises(DataError):
            r.xread(streams={"stream": 0}, count=5, max_count=3)

    @skip_if_server_version_lt("8.9.0")
    def test_xread_with_max_count(self, r):
        stream = "stream"
        for i in range(5):
            r.xadd(stream, {"f": i})

        # max_count caps the total number of returned entries
        res = r.xread(streams={stream: 0}, max_count=2)
        assert self._total_stream_entries(r, res) == 2

    @skip_if_server_version_lt("8.9.0")
    def test_xread_with_max_size(self, r):
        stream = "stream"
        r.xadd(stream, {"f": "v"})

        # a generous soft cap returns the available entry
        res = r.xread(streams={stream: 0}, max_size=65536)
        assert self._total_stream_entries(r, res) == 1

        # max_size is a soft cap: a single available entry that exceeds the cap
        # is still returned rather than suppressed
        res = r.xread(streams={stream: 0}, max_size=1)
        assert self._total_stream_entries(r, res) == 1

    @skip_if_server_version_lt("8.9.0")
    def test_xread_max_count_cumulative_across_streams(self, r):
        stream_1 = "stream1:{maxcount}"
        stream_2 = "stream2:{maxcount}"
        for i in range(3):
            r.xadd(stream_1, {"f": i})
        for i in range(3):
            r.xadd(stream_2, {"f": i})

        # count is per-stream (up to 3 each), max_count caps the cumulative
        # reply; streams are served in caller order, so stream_1 contributes 3
        # and stream_2 contributes 1
        res = r.xread(streams={stream_1: 0, stream_2: 0}, count=3, max_count=4)
        assert self._total_stream_entries(r, res) == 4

    def test_xreadgroup_max_count_max_size_validation(self, r):
        with pytest.raises(DataError):
            r.xreadgroup("g", "c", streams={"stream": ">"}, max_count=0)
        with pytest.raises(DataError):
            r.xreadgroup("g", "c", streams={"stream": ">"}, max_size=-1)
        with pytest.raises(DataError):
            r.xreadgroup("g", "c", streams={"stream": ">"}, count=5, max_count=3)

    @skip_if_server_version_lt("8.9.0")
    def test_xreadgroup_with_max_count(self, r):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        for i in range(5):
            r.xadd(stream, {"f": i})
        r.xgroup_create(stream, group, 0)

        # max_count caps the total number of returned entries
        res = r.xreadgroup(group, consumer, streams={stream: ">"}, max_count=2)
        assert self._total_stream_entries(r, res) == 2
        r.xgroup_destroy(stream, group)

    @skip_if_server_version_lt("8.9.0")
    def test_xreadgroup_max_count_cumulative_across_streams(self, r):
        stream_1 = "stream1:{grpmaxcount}"
        stream_2 = "stream2:{grpmaxcount}"
        group = "group"
        consumer = "consumer"
        for i in range(3):
            r.xadd(stream_1, {"f": i})
        for i in range(3):
            r.xadd(stream_2, {"f": i})
        r.xgroup_create(stream_1, group, 0)
        r.xgroup_create(stream_2, group, 0)

        res = r.xreadgroup(
            group,
            consumer,
            streams={stream_1: ">", stream_2: ">"},
            count=3,
            max_count=4,
        )
        assert self._total_stream_entries(r, res) == 4
        r.xgroup_destroy(stream_1, group)
        r.xgroup_destroy(stream_2, group)

    @skip_if_server_version_lt("5.0.0")
    def test_xreadgroup(self, r):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        m1 = r.xadd(stream, {"foo": "bar"})
        m2 = r.xadd(stream, {"bing": "baz"})
        r.xgroup_create(stream, group, 0)

        stream_name = stream.encode()
        expected_entries = [
            get_stream_message(r, stream, m1),
            get_stream_message(r, stream, m2),
        ]

        # xread starting at 0 returns both messages
        assert_resp_response(
            r,
            r.xreadgroup(group, consumer, streams={stream: ">"}),
            [[stream_name, expected_entries]],
            {stream_name: [expected_entries]},
            {stream_name: expected_entries},
        )

        r.xgroup_destroy(stream, group)
        r.xgroup_create(stream, group, 0)

        expected_entries = [get_stream_message(r, stream, m1)]

        # xread with count=1 returns only the first message
        assert_resp_response(
            r,
            r.xreadgroup(group, consumer, streams={stream: ">"}, count=1),
            [[stream_name, expected_entries]],
            {stream_name: [expected_entries]},
            {stream_name: expected_entries},
        )

        r.xgroup_destroy(stream, group)

        # create the group using $ as the last id meaning subsequent reads
        # will only find messages added after this
        r.xgroup_create(stream, group, "$")

        # xread starting after the last message returns an empty message list
        assert_resp_response(
            r, r.xreadgroup(group, consumer, streams={stream: ">"}), [], {}
        )

        # xreadgroup with noack does not have any items in the PEL
        r.xgroup_destroy(stream, group)
        r.xgroup_create(stream, group, "0")
        res = r.xreadgroup(group, consumer, streams={stream: ">"}, noack=True)
        empty_res = r.xreadgroup(group, consumer, streams={stream: "0"})
        shape = expected_response_shape(r)
        if shape == "legacy_resp2":
            assert len(res[0][1]) == 2
            # now there should be nothing pending
            assert len(empty_res[0][1]) == 0
        elif shape == "legacy_resp3":
            assert len(res[stream_name][0]) == 2
            # now there should be nothing pending
            assert len(empty_res[stream_name][0]) == 0
        else:
            assert len(res[stream_name]) == 2
            # now there should be nothing pending
            assert len(empty_res[stream_name]) == 0

        r.xgroup_destroy(stream, group)
        r.xgroup_create(stream, group, "0")
        # delete all the messages in the stream
        expected_entries = [(m1, {}), (m2, {})]
        r.xreadgroup(group, consumer, streams={stream: ">"})
        r.xtrim(stream, 0)
        assert_resp_response(
            r,
            r.xreadgroup(group, consumer, streams={stream: "0"}),
            [[stream_name, expected_entries]],
            {stream_name: [expected_entries]},
            {stream_name: expected_entries},
        )

    def _validate_xreadgroup_with_claim_min_idle_time_response(
        self, r, response, expected_entries
    ):
        # validate the number of streams
        assert len(response) == len(expected_entries)

        expected_streams = expected_entries.keys()
        for str_index, expected_stream in enumerate(expected_streams):
            expected_entries_per_stream = expected_entries[expected_stream]

            shape = expected_response_shape(r)
            if shape == "legacy_resp2":
                actual_entries_per_stream = response[str_index][1]
                actual_stream = response[str_index][0]
                assert actual_stream == expected_stream
            elif shape == "legacy_resp3":
                actual_entries_per_stream = response[expected_stream][0]
            else:
                actual_entries_per_stream = response[expected_stream]

            # validate the number of entries
            assert len(actual_entries_per_stream) == len(expected_entries_per_stream)

            for i, entry in enumerate(actual_entries_per_stream):
                message = (entry[0], entry[1])
                message_idle = int(entry[2])
                message_delivered_count = int(entry[3])

                expected_idle = expected_entries_per_stream[i]["min_idle_time"]

                assert message == expected_entries_per_stream[i]["msg"]
                if expected_idle == 0:
                    assert message_idle == 0
                    assert message_delivered_count == 0
                else:
                    assert message_idle >= expected_idle
                    assert message_delivered_count > 0

    @skip_if_server_version_lt("8.3.224")
    def test_xreadgroup_with_claim_min_idle_time(self, r):
        stream = "stream"
        group = "group"
        consumer_1 = "c1"
        stream_name = stream.encode()

        try:
            # before test cleanup
            r.xgroup_destroy(stream, group)
        except ResponseError:
            pass

        m1 = r.xadd(stream, {"key_m1": "val_m1"})
        m2 = r.xadd(stream, {"key_m2": "val_m2"})

        r.xgroup_create(stream, group, 0)

        # read all the messages - this will save the msgs in PEL
        r.xreadgroup(group, consumer_1, streams={stream: ">"})

        # wait for 100ms - so that the messages would have been in the PEL for long enough
        time.sleep(0.1)

        m3 = r.xadd(stream, {"key_m3": "val_m3"})
        m4 = r.xadd(stream, {"key_m4": "val_m4"})

        expected_entries = {
            stream_name: [
                {"msg": get_stream_message(r, stream, m1), "min_idle_time": 100},
                {"msg": get_stream_message(r, stream, m2), "min_idle_time": 100},
                {"msg": get_stream_message(r, stream, m3), "min_idle_time": 0},
                {"msg": get_stream_message(r, stream, m4), "min_idle_time": 0},
            ]
        }

        res = r.xreadgroup(
            group, consumer_1, streams={stream: ">"}, claim_min_idle_time=100
        )

        self._validate_xreadgroup_with_claim_min_idle_time_response(
            r, res, expected_entries
        )

        # validate PEL still holds the messages until they are acknowledged
        response = r.xpending_range(stream, group, min="-", max="+", count=5)
        assert len(response) == 4

        # add 2 more messages
        m5 = r.xadd(stream, {"key_m5": "val_m5"})
        m6 = r.xadd(stream, {"key_m6": "val_m6"})

        expected_entries = [
            get_stream_message(r, stream, m5),
            get_stream_message(r, stream, m6),
        ]
        # read all the messages - this will save the msgs in PEL
        res = r.xreadgroup(group, consumer_1, streams={stream: ">"})
        assert_resp_response(
            r,
            res,
            [[stream_name, expected_entries]],
            {stream_name: [expected_entries]},
            {stream_name: expected_entries},
        )

        # add 2 more messages
        m7 = r.xadd(stream, {"key_m7": "val_m7"})
        m8 = r.xadd(stream, {"key_m8": "val_m8"})
        # Use a threshold safely above this test's elapsed time so only
        # newly-delivered messages are returned.
        expected_entries = {
            stream_name: [
                {"msg": get_stream_message(r, stream, m7), "min_idle_time": 0},
                {"msg": get_stream_message(r, stream, m8), "min_idle_time": 0},
            ]
        }
        res = r.xreadgroup(
            group, consumer_1, streams={stream: ">"}, claim_min_idle_time=60_000
        )
        self._validate_xreadgroup_with_claim_min_idle_time_response(
            r, res, expected_entries
        )

    @skip_if_server_version_lt("8.3.224")
    def test_xreadgroup_with_claim_min_idle_time_multiple_streams_same_slots(self, r):
        stream_1 = "stream1:{same_slot:42}"
        stream_2 = "stream2:{same_slot:42}"
        group = "group"
        consumer_1 = "c1"

        stream_1_name = stream_1.encode()
        stream_2_name = stream_2.encode()

        # before test cleanup
        try:
            r.xgroup_destroy(stream_1, group)
        except ResponseError:
            pass
        try:
            r.xgroup_destroy(stream_2, group)
        except ResponseError:
            pass

        m1_s1 = r.xadd(stream_1, {"key_m1_s1": "val_m1"})
        m2_s1 = r.xadd(stream_1, {"key_m2_s1": "val_m2"})
        r.xgroup_create(stream_1, group, 0)

        m1_s2 = r.xadd(stream_2, {"key_m1_s2": "val_m1"})
        m2_s2 = r.xadd(stream_2, {"key_m2_s2": "val_m2"})
        r.xgroup_create(stream_2, group, 0)

        # read all the messages - this will save the msgs in PEL
        r.xreadgroup(group, consumer_1, streams={stream_1: ">", stream_2: ">"})

        # wait for 100ms - so that the messages would have been in the PEL for long enough
        time.sleep(0.1)

        m3_s1 = r.xadd(stream_1, {"key_m3_s1": "val_m3"})
        m4_s2 = r.xadd(stream_2, {"key_m4_s2": "val_m4"})

        expected_entries = {
            stream_1_name: [
                {"msg": get_stream_message(r, stream_1, m1_s1), "min_idle_time": 100},
                {"msg": get_stream_message(r, stream_1, m2_s1), "min_idle_time": 100},
                {"msg": get_stream_message(r, stream_1, m3_s1), "min_idle_time": 0},
            ],
            stream_2_name: [
                {"msg": get_stream_message(r, stream_2, m1_s2), "min_idle_time": 100},
                {"msg": get_stream_message(r, stream_2, m2_s2), "min_idle_time": 100},
                {"msg": get_stream_message(r, stream_2, m4_s2), "min_idle_time": 0},
            ],
        }

        res = r.xreadgroup(
            group,
            consumer_1,
            streams={stream_1: ">", stream_2: ">"},
            claim_min_idle_time=100,
        )

        self._validate_xreadgroup_with_claim_min_idle_time_response(
            r, res, expected_entries
        )

        # validate PEL still holds the messages until they are acknowledged
        response = r.xpending_range(stream_1, group, min="-", max="+", count=5)
        assert len(response) == 3
        response = r.xpending_range(stream_2, group, min="-", max="+", count=5)
        assert len(response) == 3

        # add 2 more messages
        r.xadd(stream_1, {"key_m5": "val_m5"})
        r.xadd(stream_2, {"key_m6": "val_m6"})

        # read all the messages - this will save the msgs in PEL
        r.xreadgroup(group, consumer_1, streams={stream_1: ">", stream_2: ">"})

        # add 2 more messages
        m7 = r.xadd(stream_1, {"key_m7": "val_m7"})
        m8 = r.xadd(stream_2, {"key_m8": "val_m8"})
        # Use a threshold safely above this test's elapsed time so only
        # newly-delivered messages are returned.
        expected_entries = {
            stream_1_name: [
                {"msg": get_stream_message(r, stream_1, m7), "min_idle_time": 0}
            ],
            stream_2_name: [
                {"msg": get_stream_message(r, stream_2, m8), "min_idle_time": 0}
            ],
        }
        res = r.xreadgroup(
            group,
            consumer_1,
            streams={stream_1: ">", stream_2: ">"},
            claim_min_idle_time=60_000,
        )
        self._validate_xreadgroup_with_claim_min_idle_time_response(
            r, res, expected_entries
        )

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.3.224")
    def test_xreadgroup_with_claim_min_idle_time_multiple_streams(self, r):
        stream_1 = "stream1"
        stream_2 = "stream2"
        group = "group"
        consumer_1 = "c1"

        stream_1_name = stream_1.encode()
        stream_2_name = stream_2.encode()

        # before test cleanup
        try:
            r.xgroup_destroy(stream_1, group)
        except ResponseError:
            pass
        try:
            r.xgroup_destroy(stream_2, group)
        except ResponseError:
            pass

        m1_s1 = r.xadd(stream_1, {"key_m1_s1": "val_m1"})
        m2_s1 = r.xadd(stream_1, {"key_m2_s1": "val_m2"})
        r.xgroup_create(stream_1, group, 0)

        m1_s2 = r.xadd(stream_2, {"key_m1_s2": "val_m1"})
        m2_s2 = r.xadd(stream_2, {"key_m2_s2": "val_m2"})
        r.xgroup_create(stream_2, group, 0)

        # read all the messages - this will save the msgs in PEL
        r.xreadgroup(group, consumer_1, streams={stream_1: ">", stream_2: ">"})

        # wait for 100ms - so that the messages would have been in the PEL for long enough
        time.sleep(0.1)

        m3_s1 = r.xadd(stream_1, {"key_m3_s1": "val_m3"})
        m4_s2 = r.xadd(stream_2, {"key_m4_s2": "val_m4"})

        expected_entries = {
            stream_1_name: [
                {"msg": get_stream_message(r, stream_1, m1_s1), "min_idle_time": 100},
                {"msg": get_stream_message(r, stream_1, m2_s1), "min_idle_time": 100},
                {"msg": get_stream_message(r, stream_1, m3_s1), "min_idle_time": 0},
            ],
            stream_2_name: [
                {"msg": get_stream_message(r, stream_2, m1_s2), "min_idle_time": 100},
                {"msg": get_stream_message(r, stream_2, m2_s2), "min_idle_time": 100},
                {"msg": get_stream_message(r, stream_2, m4_s2), "min_idle_time": 0},
            ],
        }

        res = r.xreadgroup(
            group,
            consumer_1,
            streams={stream_1: ">", stream_2: ">"},
            claim_min_idle_time=100,
        )

        self._validate_xreadgroup_with_claim_min_idle_time_response(
            r, res, expected_entries
        )

        # validate PEL still holds the messages until they are acknowledged
        response = r.xpending_range(stream_1, group, min="-", max="+", count=5)
        assert len(response) == 3
        response = r.xpending_range(stream_2, group, min="-", max="+", count=5)
        assert len(response) == 3

        # add 2 more messages
        r.xadd(stream_1, {"key_m5": "val_m5"})
        r.xadd(stream_2, {"key_m6": "val_m6"})

        # read all the messages - this will save the msgs in PEL
        r.xreadgroup(group, consumer_1, streams={stream_1: ">", stream_2: ">"})

        # add 2 more messages
        m7 = r.xadd(stream_1, {"key_m7": "val_m7"})
        m8 = r.xadd(stream_2, {"key_m8": "val_m8"})
        # Use a threshold safely above this test's elapsed time so only
        # newly-delivered messages are returned.
        expected_entries = {
            stream_1_name: [
                {"msg": get_stream_message(r, stream_1, m7), "min_idle_time": 0}
            ],
            stream_2_name: [
                {"msg": get_stream_message(r, stream_2, m8), "min_idle_time": 0}
            ],
        }
        res = r.xreadgroup(
            group,
            consumer_1,
            streams={stream_1: ">", stream_2: ">"},
            claim_min_idle_time=60_000,
        )
        self._validate_xreadgroup_with_claim_min_idle_time_response(
            r, res, expected_entries
        )

    @skip_if_server_version_lt("5.0.0")
    def test_xrevrange(self, r):
        stream = "stream"
        m1 = r.xadd(stream, {"foo": "bar"})
        m2 = r.xadd(stream, {"foo": "bar"})
        m3 = r.xadd(stream, {"foo": "bar"})
        m4 = r.xadd(stream, {"foo": "bar"})

        def get_ids(results):
            return [result[0] for result in results]

        results = r.xrevrange(stream, max=m4)
        assert get_ids(results) == [m4, m3, m2, m1]

        results = r.xrevrange(stream, max=m3, min=m2)
        assert get_ids(results) == [m3, m2]

        results = r.xrevrange(stream, min=m3)
        assert get_ids(results) == [m4, m3]

        results = r.xrevrange(stream, min=m2, count=1)
        assert get_ids(results) == [m4]

    @skip_if_server_version_lt("5.0.0")
    def test_xtrim(self, r):
        stream = "stream"

        # trimming an empty key doesn't do anything
        assert r.xtrim(stream, 1000) == 0

        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})

        # trimming an amount large than the number of messages
        # doesn't do anything
        assert r.xtrim(stream, 5, approximate=False) == 0

        # 1 message is trimmed
        assert r.xtrim(stream, 3, approximate=False) == 1

    @skip_if_server_version_lt("6.2.4")
    def test_xtrim_minlen_and_length_args(self, r):
        stream = "stream"

        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})

        # Future self: No limits without approximate, according to the api
        with pytest.raises(redis.ResponseError):
            assert r.xtrim(stream, 3, approximate=False, limit=2)

        # maxlen with a limit
        assert r.xtrim(stream, 3, approximate=True, limit=2) == 0
        r.delete(stream)

        with pytest.raises(redis.DataError):
            assert r.xtrim(stream, maxlen=3, minid="sometestvalue")

        # minid with a limit
        m1 = r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        assert r.xtrim(stream, None, approximate=True, minid=m1, limit=3) == 0

        # pure minid
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        m4 = r.xadd(stream, {"foo": "bar"})
        assert r.xtrim(stream, None, approximate=False, minid=m4) == 7

        # minid approximate
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        m3 = r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        assert r.xtrim(stream, None, approximate=True, minid=m3) == 0

    @skip_if_server_version_lt("8.1.224")
    def test_xdelex(self, r):
        stream = "stream"

        m1 = r.xadd(stream, {"foo": "bar"})
        m2 = r.xadd(stream, {"foo": "bar"})
        m3 = r.xadd(stream, {"foo": "bar"})
        m4 = r.xadd(stream, {"foo": "bar"})

        # Test XDELEX with default ref_policy (KEEPREF)
        result = r.xdelex(stream, m1)
        assert result == [1]

        # Test XDELEX with explicit KEEPREF
        result = r.xdelex(stream, m2, ref_policy="KEEPREF")
        assert result == [1]

        # Test XDELEX with DELREF
        result = r.xdelex(stream, m3, ref_policy="DELREF")
        assert result == [1]

        # Test XDELEX with ACKED
        result = r.xdelex(stream, m4, ref_policy="ACKED")
        assert result == [1]

        # Test with non-existent ID
        result = r.xdelex(stream, "999999-0", ref_policy="KEEPREF")
        assert result == [-1]

        # Test with multiple IDs
        m5 = r.xadd(stream, {"foo": "bar"})
        m6 = r.xadd(stream, {"foo": "bar"})
        result = r.xdelex(stream, m5, m6, ref_policy="KEEPREF")
        assert result == [1, 1]  # Both entries deleted

        # Test error cases
        with pytest.raises(redis.DataError):
            r.xdelex(stream, "123-0", ref_policy="INVALID")

        with pytest.raises(redis.DataError):
            r.xdelex(stream)  # No IDs provided

    @skip_if_server_version_lt("8.1.224")
    def test_xackdel(self, r):
        stream = "stream"
        group = "group"
        consumer = "consumer"

        m1 = r.xadd(stream, {"foo": "bar"})
        m2 = r.xadd(stream, {"foo": "bar"})
        m3 = r.xadd(stream, {"foo": "bar"})
        m4 = r.xadd(stream, {"foo": "bar"})
        r.xgroup_create(stream, group, 0)

        r.xreadgroup(group, consumer, streams={stream: ">"})

        # Test XACKDEL with default ref_policy (KEEPREF)
        result = r.xackdel(stream, group, m1)
        assert result == [1]

        # Test XACKDEL with explicit KEEPREF
        result = r.xackdel(stream, group, m2, ref_policy="KEEPREF")
        assert result == [1]

        # Test XACKDEL with DELREF
        result = r.xackdel(stream, group, m3, ref_policy="DELREF")
        assert result == [1]

        # Test XACKDEL with ACKED
        result = r.xackdel(stream, group, m4, ref_policy="ACKED")
        assert result == [1]

        # Test with non-existent ID
        result = r.xackdel(stream, group, "999999-0", ref_policy="KEEPREF")
        assert result == [-1]

        # Test error cases
        with pytest.raises(redis.DataError):
            r.xackdel(stream, group, m1, ref_policy="INVALID")

        with pytest.raises(redis.DataError):
            r.xackdel(stream, group)  # No IDs provided

    @skip_if_server_version_lt("8.1.224")
    def test_xtrim_with_options(self, r):
        stream = "stream"

        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})

        # Test XTRIM with KEEPREF ref_policy
        assert r.xtrim(stream, maxlen=2, approximate=False, ref_policy="KEEPREF") == 2

        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})

        # Test XTRIM with DELREF ref_policy
        assert r.xtrim(stream, maxlen=2, approximate=False, ref_policy="DELREF") == 2

        r.xadd(stream, {"foo": "bar"})
        r.xadd(stream, {"foo": "bar"})

        # Test XTRIM with ACKED ref_policy
        assert r.xtrim(stream, maxlen=2, approximate=False, ref_policy="ACKED") == 2

        # Test error case
        with pytest.raises(redis.DataError):
            r.xtrim(stream, maxlen=2, ref_policy="INVALID")

    @skip_if_server_version_lt("8.1.224")
    def test_xadd_with_options(self, r):
        stream = "stream"

        # Test XADD with KEEPREF ref_policy
        r.xadd(
            stream, {"foo": "bar"}, maxlen=2, approximate=False, ref_policy="KEEPREF"
        )
        r.xadd(
            stream, {"foo": "bar"}, maxlen=2, approximate=False, ref_policy="KEEPREF"
        )
        r.xadd(
            stream, {"foo": "bar"}, maxlen=2, approximate=False, ref_policy="KEEPREF"
        )
        assert r.xlen(stream) == 2

        # Test XADD with DELREF ref_policy
        r.xadd(stream, {"foo": "bar"}, maxlen=2, approximate=False, ref_policy="DELREF")
        assert r.xlen(stream) == 2

        # Test XADD with ACKED ref_policy
        r.xadd(stream, {"foo": "bar"}, maxlen=2, approximate=False, ref_policy="ACKED")
        assert r.xlen(stream) == 2

        # Test error case
        with pytest.raises(redis.DataError):
            r.xadd(stream, {"foo": "bar"}, ref_policy="INVALID")

    @skip_if_server_version_lt("8.5.0")
    def test_xadd_idmpauto(self, r):
        stream = "stream"

        # XADD with IDMPAUTO - first write
        message_id1 = r.xadd(stream, {"field1": "value1"}, idmpauto="producer1")

        # Test XADD with IDMPAUTO - duplicate write returns same ID
        message_id2 = r.xadd(stream, {"field1": "value1"}, idmpauto="producer1")
        assert message_id1 == message_id2

        # Test XADD with IDMPAUTO - different content creates new entry
        message_id3 = r.xadd(stream, {"field1": "value2"}, idmpauto="producer1")
        assert message_id3 != message_id1

        # Test XADD with IDMPAUTO - different producer creates new entry
        message_id4 = r.xadd(stream, {"field1": "value1"}, idmpauto="producer2")
        assert message_id4 != message_id1

        # Verify stream has 3 entries (2 unique from producer1, 1 from producer2)
        assert r.xlen(stream) == 3

    @skip_if_server_version_lt("8.5.0")
    def test_xadd_idmp(self, r):
        stream = "stream"

        # XADD with IDMP - first write
        message_id1 = r.xadd(stream, {"field1": "value1"}, idmp=("producer1", b"msg1"))

        # Test XADD with IDMP - duplicate write returns same ID
        message_id2 = r.xadd(stream, {"field1": "value1"}, idmp=("producer1", b"msg1"))
        assert message_id1 == message_id2

        # Test XADD with IDMP - different iid creates new entry
        message_id3 = r.xadd(stream, {"field1": "value1"}, idmp=("producer1", b"msg2"))
        assert message_id3 != message_id1

        # Test XADD with IDMP - different producer creates new entry
        message_id4 = r.xadd(stream, {"field1": "value1"}, idmp=("producer2", b"msg1"))
        assert message_id4 != message_id1

        # Test XADD with IDMP - shorter binary iid
        r.xadd(stream, {"field1": "value1"}, idmp=("producer1", b"\x01"))

        # Verify stream has 4 entries
        assert r.xlen(stream) == 4

    @skip_if_server_version_lt("8.5.0")
    def test_xadd_idmp_validation(self, r):
        stream = "stream"

        # Test error: both idmpauto and idmp specified
        with pytest.raises(redis.DataError):
            r.xadd(
                stream,
                {"foo": "bar"},
                idmpauto="producer1",
                idmp=("producer1", b"msg1"),
            )

        # Test error: idmpauto with explicit id
        with pytest.raises(redis.DataError):
            r.xadd(stream, {"foo": "bar"}, id="1234567890-0", idmpauto="producer1")

        # Test error: idmp with explicit id
        with pytest.raises(redis.DataError):
            r.xadd(
                stream, {"foo": "bar"}, id="1234567890-0", idmp=("producer1", b"msg1")
            )

        # Test error: idmp not a tuple
        with pytest.raises(redis.DataError):
            r.xadd(stream, {"foo": "bar"}, idmp="invalid")

        # Test error: idmp tuple with wrong number of elements
        with pytest.raises(redis.DataError):
            r.xadd(stream, {"foo": "bar"}, idmp=("producer1",))

        # Test error: idmp tuple with wrong number of elements
        with pytest.raises(redis.DataError):
            r.xadd(stream, {"foo": "bar"}, idmp=("producer1", b"msg1", "extra"))

    @skip_if_server_version_lt("8.5.0")
    def test_xcfgset_idmp_duration(self, r):
        stream = "stream"

        # Create stream first
        r.xadd(stream, {"foo": "bar"})

        # Test XCFGSET with IDMP-DURATION only
        assert r.xcfgset(stream, idmp_duration=120) == b"OK"

        # Test with minimum value
        assert r.xcfgset(stream, idmp_duration=1) == b"OK"

        # Test with maximum value
        assert r.xcfgset(stream, idmp_duration=300) == b"OK"

    @skip_if_server_version_lt("8.5.0")
    def test_xcfgset_idmp_maxsize(self, r):
        stream = "stream"

        # Create stream first
        r.xadd(stream, {"foo": "bar"})

        # Test XCFGSET with IDMP-MAXSIZE only
        assert r.xcfgset(stream, idmp_maxsize=5000) == b"OK"

        # Test with minimum value
        assert r.xcfgset(stream, idmp_maxsize=1) == b"OK"

        # Test with maximum value
        assert r.xcfgset(stream, idmp_maxsize=10000) == b"OK"

    @skip_if_server_version_lt("8.5.0")
    def test_xcfgset_both_parameters(self, r):
        stream = "stream"

        # Create stream first
        r.xadd(stream, {"foo": "bar"})

        # Test XCFGSET with both IDMP-DURATION and IDMP-MAXSIZE
        assert r.xcfgset(stream, idmp_duration=120, idmp_maxsize=5000) == b"OK"

        # Test with different values
        assert r.xcfgset(stream, idmp_duration=60, idmp_maxsize=10000) == b"OK"

    @skip_if_server_version_lt("8.5.0")
    def test_xcfgset_validation(self, r):
        stream = "stream"

        # Test error: no parameters provided
        with pytest.raises(redis.DataError):
            r.xcfgset(stream)

        # Test error: idmp_duration too small
        with pytest.raises(redis.DataError):
            r.xcfgset(stream, idmp_duration=0)

        # Test error: idmp_duration too large
        with pytest.raises(redis.DataError):
            r.xcfgset(stream, idmp_duration=301)

        # Test error: idmp_duration not an integer
        with pytest.raises(redis.DataError):
            r.xcfgset(stream, idmp_duration="invalid")

        # Test error: idmp_maxsize too small
        with pytest.raises(redis.DataError):
            r.xcfgset(stream, idmp_maxsize=0)

        # Test error: idmp_maxsize too large
        with pytest.raises(redis.DataError):
            r.xcfgset(stream, idmp_maxsize=1000001)

        # Test error: idmp_maxsize not an integer
        with pytest.raises(redis.DataError):
            r.xcfgset(stream, idmp_maxsize="invalid")

    def test_bitfield_operations(self, r):
        # comments show affected bits
        bf = r.bitfield("a")
        resp = (
            bf.set("u8", 8, 255)  # 00000000 11111111
            .get("u8", 0)  # 00000000
            .get("u4", 8)  # 1111
            .get("u4", 12)  # 1111
            .get("u4", 13)  # 111 0
            .execute()
        )
        assert resp == [0, 0, 15, 15, 14]

        # .set() returns the previous value...
        resp = (
            bf.set("u8", 4, 1)  # 0000 0001
            .get("u16", 0)  # 00000000 00011111
            .set("u16", 0, 0)  # 00000000 00000000
            .execute()
        )
        assert resp == [15, 31, 31]

        # incrby adds to the value
        resp = (
            bf.incrby("u8", 8, 254)  # 00000000 11111110
            .incrby("u8", 8, 1)  # 00000000 11111111
            .get("u16", 0)  # 00000000 11111111
            .execute()
        )
        assert resp == [254, 255, 255]

        # Verify overflow protection works as a method:
        r.delete("a")
        resp = (
            bf.set("u8", 8, 254)  # 00000000 11111110
            .overflow("fail")
            .incrby("u8", 8, 2)  # incrby 2 would overflow, None returned
            .incrby("u8", 8, 1)  # 00000000 11111111
            .incrby("u8", 8, 1)  # incrby 1 would overflow, None returned
            .get("u16", 0)  # 00000000 11111111
            .execute()
        )
        assert resp == [0, None, 255, None, 255]

        # Verify overflow protection works as arg to incrby:
        r.delete("a")
        resp = (
            bf.set("u8", 8, 255)  # 00000000 11111111
            .incrby("u8", 8, 1)  # 00000000 00000000  wrap default
            .set("u8", 8, 255)  # 00000000 11111111
            .incrby("u8", 8, 1, "FAIL")  # 00000000 11111111  fail
            .incrby("u8", 8, 1)  # 00000000 11111111  still fail
            .get("u16", 0)  # 00000000 11111111
            .execute()
        )
        assert resp == [0, 0, 0, None, None, 255]

        # test default default_overflow
        r.delete("a")
        bf = r.bitfield("a", default_overflow="FAIL")
        resp = (
            bf.set("u8", 8, 255)  # 00000000 11111111
            .incrby("u8", 8, 1)  # 00000000 11111111  fail default
            .get("u16", 0)  # 00000000 11111111
            .execute()
        )
        assert resp == [0, None, 255]

    @skip_if_server_version_lt("6.0.0")
    def test_bitfield_ro(self, r: redis.Redis):
        bf = r.bitfield("a")
        resp = bf.set("u8", 8, 255).execute()
        assert resp == [0]

        resp = r.bitfield_ro("a", "u8", 0)
        assert resp == [0]

        items = [("u4", 8), ("u4", 12), ("u4", 13)]
        resp = r.bitfield_ro("a", "u8", 0, items)
        assert resp == [0, 15, 15, 14]

    @skip_if_server_version_lt("4.0.0")
    def test_memory_help(self, r):
        with pytest.raises(NotImplementedError):
            r.memory_help()

    @skip_if_server_version_lt("4.0.0")
    def test_memory_doctor(self, r):
        with pytest.raises(NotImplementedError):
            r.memory_doctor()

    @skip_if_server_version_lt("4.0.0")
    @skip_if_redis_enterprise()
    def test_memory_malloc_stats(self, r):
        if skip_if_redis_enterprise().args[0] is True:
            with pytest.raises(redis.exceptions.ResponseError):
                assert r.memory_malloc_stats()
            return

        assert r.memory_malloc_stats()

    @skip_if_server_version_lt("4.0.0")
    @skip_if_redis_enterprise()
    def test_memory_stats(self, r):
        # put a key into the current db to make sure that "db.<current-db>"
        # has data
        r.set("foo", "bar")

        if skip_if_redis_enterprise().args[0] is True:
            with pytest.raises(redis.exceptions.ResponseError):
                stats = r.memory_stats()
            return

        stats = r.memory_stats()
        assert isinstance(stats, dict)
        for key, value in stats.items():
            if key.startswith("db."):
                assert not isinstance(value, list)

    @pytest.mark.fixed_client
    @skip_if_server_version_lt("4.0.0")
    @skip_if_redis_enterprise()
    @pytest.mark.parametrize("protocol", [2, 3])
    def test_memory_stats_unified_responses(self, request, protocol):
        r = _get_client(redis.Redis, request, protocol=protocol, legacy_responses=False)
        r.set("foo", "bar")
        stats = r.memory_stats()
        # Redis replies with doubles for these, which RESP2 sends as bulk strings.
        for key in ("dataset.percentage", "peak.percentage", "fragmentation"):
            assert type(stats[key]) is float
        assert type(stats["peak.allocated"]) is int
        for key, value in stats.items():
            if key.startswith("db.") and isinstance(value, dict):
                assert all(type(v) is int for v in value.values())

    @skip_if_server_version_lt("4.0.0")
    def test_memory_usage(self, r):
        r.set("foo", "bar")
        assert isinstance(r.memory_usage("foo"), int)

    @skip_if_server_version_lt("7.0.0")
    def test_latency_histogram_not_implemented(self, r: redis.Redis):
        with pytest.raises(NotImplementedError):
            r.latency_histogram()

    def test_latency_graph_not_implemented(self, r: redis.Redis):
        with pytest.raises(NotImplementedError):
            r.latency_graph()

    def test_latency_doctor_not_implemented(self, r: redis.Redis):
        with pytest.raises(NotImplementedError):
            r.latency_doctor()

    # Redis Enterprise restricts the LATENCY admin subcommands.
    @skip_if_redis_enterprise()
    def test_latency_history(self, r: redis.Redis):
        assert r.latency_history("command") == []

    @skip_if_redis_enterprise()
    def test_latency_latest(self, r: redis.Redis):
        assert r.latency_latest() == []

    @skip_if_redis_enterprise()
    def test_latency_reset(self, r: redis.Redis):
        assert r.latency_reset() == 0

    @pytest.mark.redismod
    @skip_if_server_version_lt("4.0.0")
    @skip_if_redis_enterprise()
    def test_module_list(self, r):
        assert isinstance(r.module_list(), list)
        for x in r.module_list():
            assert isinstance(x, dict)

    @skip_if_server_version_lt("2.8.13")
    @skip_if_redis_enterprise()
    def test_command_count(self, r):
        res = r.command_count()
        assert isinstance(res, int)
        assert res >= 100

    @skip_if_server_version_lt("7.0.0")
    def test_command_docs(self, r):
        with pytest.raises(NotImplementedError):
            r.command_docs("set")

    @skip_if_server_version_lt("7.0.0")
    @skip_if_redis_enterprise()
    def test_command_list(self, r: redis.Redis):
        assert len(r.command_list()) > 300
        assert len(r.command_list(module="fakemod")) == 0
        assert len(r.command_list(category="list")) > 15
        assert b"lpop" in r.command_list(pattern="l*")
        with pytest.raises(redis.ResponseError):
            r.command_list(category="list", pattern="l*")

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.13")
    @skip_if_redis_enterprise()
    def test_command_getkeys(self, r):
        res = r.command_getkeys("MSET", "a", "b", "c", "d", "e", "f")
        assert_resp_response(r, res, ["a", "c", "e"], [b"a", b"c", b"e"])
        res = r.command_getkeys(
            "EVAL",
            '"not consulted"',
            "3",
            "key1",
            "key2",
            "key3",
            "arg1",
            "arg2",
            "arg3",
            "argN",
        )
        assert_resp_response(
            r, res, ["key1", "key2", "key3"], [b"key1", b"key2", b"key3"]
        )

    @skip_if_server_version_lt("2.8.13")
    def test_command(self, r):
        res = r.command()
        assert len(res) >= 100
        cmds = list(res.keys())
        assert "set" in cmds
        assert "get" in cmds

    @skip_if_server_version_lt("7.0.0")
    @skip_if_redis_enterprise()
    def test_command_getkeysandflags(self, r: redis.Redis):
        res = r.command_getkeysandflags("LMOVE", "mylist1", "mylist2", "left", "left")
        assert res == [
            [b"mylist1", [b"RW", b"access", b"delete"]],
            [b"mylist2", [b"RW", b"insert"]],
        ]

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("4.0.0")
    @skip_if_redis_enterprise()
    def test_module(self, stack_r):
        with pytest.raises(redis.exceptions.ModuleError) as excinfo:
            stack_r.module_load("/some/fake/path")
        assert "Error loading the extension." in str(excinfo.value)

        with pytest.raises(redis.exceptions.ModuleError) as excinfo:
            stack_r.module_load("/some/fake/path", "arg1", "arg2", "arg3", "arg4")
        assert "Error loading the extension." in str(excinfo.value)

    @pytest.mark.redismod
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    @skip_if_redis_enterprise()
    def test_module_loadex(self, stack_r: redis.Redis):
        with pytest.raises(redis.exceptions.ModuleError) as excinfo:
            stack_r.module_loadex("/some/fake/path")
        assert "Error loading the extension." in str(excinfo.value)

        with pytest.raises(redis.exceptions.ModuleError) as excinfo:
            stack_r.module_loadex(
                "/some/fake/path", ["name", "value"], ["arg1", "arg2"]
            )
        assert "Error loading the extension." in str(excinfo.value)

    @skip_if_server_version_lt("2.6.0")
    def test_restore(self, r):
        # standard restore
        key = "foo"
        r.set(key, "bar")
        dumpdata = r.dump(key)
        r.delete(key)
        assert r.restore(key, 0, dumpdata)
        assert r.get(key) == b"bar"

        # overwrite restore
        with pytest.raises(redis.exceptions.ResponseError):
            assert r.restore(key, 0, dumpdata)
        r.set(key, "a new value!")
        assert r.restore(key, 0, dumpdata, replace=True)
        assert r.get(key) == b"bar"

        # ttl check
        key2 = "another"
        r.set(key2, "blee!")
        dumpdata = r.dump(key2)
        r.delete(key2)
        assert r.restore(key2, 0, dumpdata)
        assert r.ttl(key2) == -1

    @skip_if_server_version_lt("5.0.0")
    def test_restore_idletime(self, r):
        key = "yayakey"
        r.set(key, "blee!")
        dumpdata = r.dump(key)
        r.delete(key)
        assert r.restore(key, 0, dumpdata, idletime=5)
        assert r.get(key) == b"blee!"

    @skip_if_server_version_lt("5.0.0")
    def test_restore_frequency(self, r):
        key = "yayakey"
        r.set(key, "blee!")
        dumpdata = r.dump(key)
        r.delete(key)
        assert r.restore(key, 0, dumpdata, frequency=5)
        assert r.get(key) == b"blee!"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("5.0.0")
    @skip_if_redis_enterprise()
    def test_replicaof(self, r):
        with pytest.raises(redis.ResponseError):
            assert r.replicaof("NO ONE")
        assert r.replicaof("NO", "ONE")

    def test_shutdown(self, r: redis.Redis):
        r.execute_command = mock.MagicMock()
        r.execute_command("SHUTDOWN", "NOSAVE")
        r.execute_command.assert_called_once_with("SHUTDOWN", "NOSAVE")

    @skip_if_server_version_lt("7.0.0")
    def test_shutdown_with_params(self, r: redis.Redis):
        r.execute_command = mock.MagicMock()
        r.execute_command("SHUTDOWN", "SAVE", "NOW", "FORCE")
        r.execute_command.assert_called_once_with("SHUTDOWN", "SAVE", "NOW", "FORCE")
        r.execute_command("SHUTDOWN", "ABORT")
        r.execute_command.assert_called_with("SHUTDOWN", "ABORT")

    @pytest.mark.replica
    @pytest.mark.xfail(strict=False)
    @skip_if_server_version_lt("2.8.0")
    @skip_if_redis_enterprise()
    def test_sync(self, r):
        r.flushdb()
        time.sleep(1)
        r2 = redis.Redis(port=6380, decode_responses=False)
        res = r2.sync()
        assert b"REDIS" in res

    @pytest.mark.replica
    @skip_if_server_version_lt("2.8.0")
    @skip_if_redis_enterprise()
    def test_psync(self, r):
        r2 = redis.Redis(port=6380, decode_responses=False)
        res = r2.psync(r2.client_id(), 1)
        assert b"FULLRESYNC" in res

    @pytest.mark.onlynoncluster
    # Timing-sensitive regression test that needs a co-located low-latency server;
    # it is unreliable against a remote managed Redis Enterprise endpoint.
    @skip_if_redis_enterprise()
    def test_interrupted_command(self, r: redis.Redis):
        """
        Regression test for issue #1128:  An Un-handled BaseException
        will leave the socket with un-read response to a previous
        command.
        """

        ok = False

        def helper():
            with pytest.raises(CancelledError):
                # blocking pop
                with patch.object(
                    r.connection._parser, "read_response", side_effect=CancelledError
                ):
                    r.brpop(["nonexist"])
            # if all is well, we can continue.
            r.set("status", "down")  # should not hang
            nonlocal ok
            ok = True

        thread = threading.Thread(target=helper)
        thread.start()
        thread.join(0.1)
        try:
            assert not thread.is_alive()
            assert ok
        finally:
            # disconnect here so that fixture cleanup can proceed
            r.connection.disconnect()


@pytest.mark.onlynoncluster
class TestBinarySave:
    def test_binary_get_set(self, r):
        assert r.set(" foo bar ", "123")
        assert r.get(" foo bar ") == b"123"

        assert r.set(" foo\r\nbar\r\n ", "456")
        assert r.get(" foo\r\nbar\r\n ") == b"456"

        assert r.set(" \r\n\t\x07\x13 ", "789")
        assert r.get(" \r\n\t\x07\x13 ") == b"789"

        assert sorted(r.keys("*")) == [
            b" \r\n\t\x07\x13 ",
            b" foo\r\nbar\r\n ",
            b" foo bar ",
        ]

        assert r.delete(" foo bar ")
        assert r.delete(" foo\r\nbar\r\n ")
        assert r.delete(" \r\n\t\x07\x13 ")

    def test_binary_lists(self, r):
        mapping = {
            b"foo bar": [b"1", b"2", b"3"],
            b"foo\r\nbar\r\n": [b"4", b"5", b"6"],
            b"foo\tbar\x07": [b"7", b"8", b"9"],
        }
        # fill in lists
        for key, value in mapping.items():
            r.rpush(key, *value)

        # check that KEYS returns all the keys as they are
        assert sorted(r.keys("*")) == sorted(mapping.keys())

        # check that it is possible to get list content by key name
        for key, value in mapping.items():
            assert r.lrange(key, 0, -1) == value

    @skip_if_redis_enterprise()
    def test_large_responses(self, r):
        "The PythonParser has some special cases for return values > 1MB"
        # load up 5MB of data into a key
        data = "".join([ascii_letters] * (5000000 // len(ascii_letters)))
        r["a"] = data
        assert r["a"] == data.encode()

    def test_floating_point_encoding(self, r):
        """
        High precision floating point values sent to the server should keep
        precision.
        """
        timestamp = 1349673917.939762
        r.zadd("a", {"a1": timestamp})
        assert r.zscore("a", "a1") == timestamp


@pytest.mark.fixed_client
class TestInfoParsing:
    """INFO reply parsing on canned replies; no server needed."""

    def test_22_info(self):
        """
        Older Redis versions contained 'allocation_stats' in INFO that
        was the cause of a number of bugs when parsing.
        """
        info = (
            "allocation_stats:6=1,7=1,8=7141,9=180,10=92,11=116,12=5330,"
            "13=123,14=3091,15=11048,16=225842,17=1784,18=814,19=12020,"
            "20=2530,21=645,22=15113,23=8695,24=142860,25=318,26=3303,"
            "27=20561,28=54042,29=37390,30=1884,31=18071,32=31367,33=160,"
            "34=169,35=201,36=10155,37=1045,38=15078,39=22985,40=12523,"
            "41=15588,42=265,43=1287,44=142,45=382,46=945,47=426,48=171,"
            "49=56,50=516,51=43,52=41,53=46,54=54,55=75,56=647,57=332,"
            "58=32,59=39,60=48,61=35,62=62,63=32,64=221,65=26,66=30,"
            "67=36,68=41,69=44,70=26,71=144,72=169,73=24,74=37,75=25,"
            "76=42,77=21,78=126,79=374,80=27,81=40,82=43,83=47,84=46,"
            "85=114,86=34,87=37,88=7240,89=34,90=38,91=18,92=99,93=20,"
            "94=18,95=17,96=15,97=22,98=18,99=69,100=17,101=22,102=15,"
            "103=29,104=39,105=30,106=70,107=22,108=21,109=26,110=52,"
            "111=45,112=33,113=67,114=41,115=44,116=48,117=53,118=54,"
            "119=51,120=75,121=44,122=57,123=44,124=66,125=56,126=52,"
            "127=81,128=108,129=70,130=50,131=51,132=53,133=45,134=62,"
            "135=12,136=13,137=7,138=15,139=21,140=11,141=20,142=6,143=7,"
            "144=11,145=6,146=16,147=19,148=1112,149=1,151=83,154=1,"
            "155=1,156=1,157=1,160=1,161=1,162=2,166=1,169=1,170=1,171=2,"
            "172=1,174=1,176=2,177=9,178=34,179=73,180=30,181=1,185=3,"
            "187=1,188=1,189=1,192=1,196=1,198=1,200=1,201=1,204=1,205=1,"
            "207=1,208=1,209=1,214=2,215=31,216=78,217=28,218=5,219=2,"
            "220=1,222=1,225=1,227=1,234=1,242=1,250=1,252=1,253=1,"
            ">=256=203"
        )
        parsed = parse_info(info)
        assert "allocation_stats" in parsed
        assert "6" in parsed["allocation_stats"]
        assert ">=256" in parsed["allocation_stats"]
