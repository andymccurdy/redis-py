"""
Tests async overrides of commands from their mixins
"""

import asyncio
import binascii
import datetime
import re
import sys
import warnings
from collections import ChainMap, UserDict
from string import ascii_letters
from types import MappingProxyType
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from redis import DataError, RedisClusterException, ResponseError

from redis import exceptions
from redis._parsers.helpers import (
    _RedisCallbacks,
    get_response_callbacks,
    parse_info,
)
from redis.client import EMPTY_RESPONSE, NEVER_DECODE
from redis.commands.core import (
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

import redis.asyncio as redis
from tests.conftest import (
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
from tests.test_asyncio.test_utils import redis_server_time

if sys.version_info >= (3, 11, 3):
    from asyncio import timeout as async_timeout
else:
    from async_timeout import timeout as async_timeout

REDIS_6_VERSION = "5.9.0"

ClientT = redis.Redis | redis.RedisCluster


@pytest.mark.fixed_client
@pytest.mark.asyncio
async def test_server_deprecated_commands_do_not_emit_python_warnings():
    client = redis.Redis()
    try:
        with patch.object(
            client, "execute_command", new_callable=AsyncMock
        ) as execute_command:
            execute_command.return_value = True
            with warnings.catch_warnings():
                warnings.simplefilter("error", DeprecationWarning)
                assert await client.setex("a", 60, "1")
                assert await client.hmset("a", {"field": "value"})

            execute_command.assert_any_await("SETEX", "a", 60, "1")
            execute_command.assert_any_await("HMSET", "a", "field", "value")
    finally:
        await client.aclose()


@pytest_asyncio.fixture()
async def r_teardown(r: redis.Redis):
    """
    A special fixture which removes the provided names from the database after use
    """
    usernames = []

    def factory(username):
        usernames.append(username)
        return r

    yield factory
    try:
        client_info = await r.client_info()
    except exceptions.NoPermissionError:
        client_info = {}
    if "default" != client_info.get("user", ""):
        await r.auth("", "default")
    for username in usernames:
        await r.acl_deluser(username)


@pytest_asyncio.fixture()
async def slowlog(r: redis.Redis):
    current_config = await r.config_get()
    old_slower_than_value = current_config["slowlog-log-slower-than"]
    old_max_legnth_value = current_config["slowlog-max-len"]

    await r.config_set("slowlog-log-slower-than", 0)
    await r.config_set("slowlog-max-len", 128)

    yield

    await r.config_set("slowlog-log-slower-than", old_slower_than_value)
    await r.config_set("slowlog-max-len", old_max_legnth_value)


async def get_stream_message(client: redis.Redis, stream: str, message_id: str):
    """Fetch a stream message and format it as a (message_id, fields) pair"""
    response = await client.xrange(stream, min=message_id, max=message_id)
    assert len(response) == 1
    return response[0]


# RESPONSE CALLBACKS
@pytest.mark.onlynoncluster
class TestResponseCallbacks:
    """Tests for the response callback system"""

    async def test_response_callbacks(self, r: redis.Redis):
        kwargs = r.connection_pool.connection_kwargs
        callbacks = get_response_callbacks(
            kwargs.get("protocol"), kwargs.get("legacy_responses", True)
        )
        assert r.response_callbacks == callbacks
        assert id(r.response_callbacks) != id(_RedisCallbacks)
        r.set_response_callback("GET", lambda x: "static")
        await r.set("a", "foo")
        assert await r.get("a") == "static"

    async def test_case_insensitive_command_names(self, r: redis.Redis):
        assert r.response_callbacks["ping"] == r.response_callbacks["PING"]


class TestRedisCommands:
    async def test_command_on_invalid_key_type(self, r: redis.Redis):
        await r.lpush("a", "1")
        with pytest.raises(redis.ResponseError):
            await r.get("a")

    # SERVER INFORMATION
    @skip_if_server_version_lt(REDIS_6_VERSION)
    async def test_acl_cat_no_category(self, r: redis.Redis):
        categories = await r.acl_cat()
        assert isinstance(categories, list)
        assert "read" in categories or b"read" in categories

    @pytest.mark.redismod
    @skip_if_server_version_lt("7.9.0")
    async def test_acl_cat_contain_modules_no_category(self, r: redis.Redis):
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
        categories = await r.acl_cat()
        assert isinstance(categories, list)
        for module_cat in modules_list:
            assert module_cat in categories or module_cat.encode() in categories

    @skip_if_server_version_lt(REDIS_6_VERSION)
    async def test_acl_cat_with_category(self, r: redis.Redis):
        commands = await r.acl_cat("read")
        assert isinstance(commands, list)
        assert "get" in commands or b"get" in commands

    @pytest.mark.redismod
    @skip_if_server_version_lt("7.9.0")
    async def test_acl_modules_cat_with_category(self, r: redis.Redis):
        search_commands = await r.acl_cat("search")
        assert isinstance(search_commands, list)
        assert "FT.SEARCH" in search_commands or b"FT.SEARCH" in search_commands

        bloom_commands = await r.acl_cat("bloom")
        assert isinstance(bloom_commands, list)
        assert "bf.add" in bloom_commands or b"bf.add" in bloom_commands

        json_commands = await r.acl_cat("json")
        assert isinstance(json_commands, list)
        assert "json.get" in json_commands or b"json.get" in json_commands

        cuckoo_commands = await r.acl_cat("cuckoo")
        assert isinstance(cuckoo_commands, list)
        assert "cf.insert" in cuckoo_commands or b"cf.insert" in cuckoo_commands

        cms_commands = await r.acl_cat("cms")
        assert isinstance(cms_commands, list)
        assert "cms.query" in cms_commands or b"cms.query" in cms_commands

        topk_commands = await r.acl_cat("topk")
        assert isinstance(topk_commands, list)
        assert "topk.list" in topk_commands or b"topk.list" in topk_commands

        tdigest_commands = await r.acl_cat("tdigest")
        assert isinstance(tdigest_commands, list)
        assert "tdigest.rank" in tdigest_commands or b"tdigest.rank" in tdigest_commands

        timeseries_commands = await r.acl_cat("timeseries")
        assert isinstance(timeseries_commands, list)
        assert "ts.range" in timeseries_commands or b"ts.range" in timeseries_commands

    @skip_if_server_version_lt(REDIS_6_VERSION)
    async def test_acl_deluser(self, r_teardown):
        username = "redis-py-user"
        r = r_teardown(username)

        assert await r.acl_deluser(username) == 0
        assert await r.acl_setuser(username, enabled=False, reset=True)
        assert await r.acl_deluser(username) == 1

    @skip_if_server_version_lt(REDIS_6_VERSION)
    async def test_acl_genpass(self, r: redis.Redis):
        password = await r.acl_genpass()
        assert isinstance(password, (str, bytes))

    @skip_if_server_version_lt("7.0.0")
    async def test_acl_getuser_setuser(self, r_teardown):
        username = "redis-py-user"
        r = r_teardown(username)
        # test enabled=False
        assert await r.acl_setuser(username, enabled=False, reset=True)
        acl = await r.acl_getuser(username)
        assert acl["categories"] == ["-@all"]
        assert acl["commands"] == []
        assert acl["keys"] == []
        assert acl["passwords"] == []
        assert "off" in acl["flags"]
        assert acl["enabled"] is False

        # test nopass=True
        assert await r.acl_setuser(username, enabled=True, reset=True, nopass=True)
        acl = await r.acl_getuser(username)
        assert acl["categories"] == ["-@all"]
        assert acl["commands"] == []
        assert acl["keys"] == []
        assert acl["passwords"] == []
        assert "on" in acl["flags"]
        assert "nopass" in acl["flags"]
        assert acl["enabled"] is True

        # test all args
        assert await r.acl_setuser(
            username,
            enabled=True,
            reset=True,
            passwords=["+pass1", "+pass2"],
            categories=["+set", "+@hash", "-geo"],
            commands=["+get", "+mget", "-hset"],
            keys=["cache:*", "objects:*"],
        )
        acl = await r.acl_getuser(username)
        assert set(acl["categories"]) == {"-@all", "+@set", "+@hash", "-@geo"}
        assert set(acl["commands"]) == {"+get", "+mget", "-hset"}
        assert acl["enabled"] is True
        assert "on" in acl["flags"]
        assert set(acl["keys"]) == {"~cache:*", "~objects:*"}
        assert len(acl["passwords"]) == 2

        # test reset=False keeps existing ACL and applies new ACL on top
        assert await r.acl_setuser(
            username,
            enabled=True,
            reset=True,
            passwords=["+pass1"],
            categories=["+@set"],
            commands=["+get"],
            keys=["cache:*"],
        )
        assert await r.acl_setuser(
            username,
            enabled=True,
            passwords=["+pass2"],
            categories=["+@hash"],
            commands=["+mget"],
            keys=["objects:*"],
        )
        acl = await r.acl_getuser(username)
        assert set(acl["commands"]) == {"+get", "+mget"}
        assert acl["enabled"] is True
        assert "on" in acl["flags"]
        assert set(acl["keys"]) == {"~cache:*", "~objects:*"}
        assert len(acl["passwords"]) == 2

        # test removal of passwords
        assert await r.acl_setuser(
            username, enabled=True, reset=True, passwords=["+pass1", "+pass2"]
        )
        assert len((await r.acl_getuser(username))["passwords"]) == 2
        assert await r.acl_setuser(username, enabled=True, passwords=["-pass2"])
        assert len((await r.acl_getuser(username))["passwords"]) == 1

        # Resets and tests that hashed passwords are set properly.
        hashed_password = (
            "5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8"
        )
        assert await r.acl_setuser(
            username, enabled=True, reset=True, hashed_passwords=["+" + hashed_password]
        )
        acl = await r.acl_getuser(username)
        assert acl["passwords"] == [hashed_password]

        # test removal of hashed passwords
        assert await r.acl_setuser(
            username,
            enabled=True,
            reset=True,
            hashed_passwords=["+" + hashed_password],
            passwords=["+pass1"],
        )
        assert len((await r.acl_getuser(username))["passwords"]) == 2
        assert await r.acl_setuser(
            username, enabled=True, hashed_passwords=["-" + hashed_password]
        )
        assert len((await r.acl_getuser(username))["passwords"]) == 1

    @skip_if_server_version_lt(REDIS_6_VERSION)
    async def test_acl_list(self, r_teardown):
        username = "redis-py-user"
        r = r_teardown(username)
        start = await r.acl_list()
        assert await r.acl_setuser(username, enabled=False, reset=True)
        users = await r.acl_list()
        assert len(users) == len(start) + 1

    @skip_if_server_version_lt(REDIS_6_VERSION)
    @pytest.mark.onlynoncluster
    async def test_acl_log(self, r_teardown, create_redis):
        username = "redis-py-user"
        r = r_teardown(username)
        await r.acl_setuser(
            username,
            enabled=True,
            reset=True,
            commands=["+get", "+set", "+select"],
            keys=["cache:*"],
            nopass=True,
        )
        await r.acl_log_reset()

        user_client = await create_redis(username=username)

        # Valid operation and key
        assert await user_client.set("cache:0", 1)
        assert await user_client.get("cache:0") == b"1"

        # Invalid key
        with pytest.raises(exceptions.NoPermissionError):
            await user_client.get("violated_cache:0")

        # Invalid operation
        with pytest.raises(exceptions.NoPermissionError):
            await user_client.hset("cache:0", "hkey", "hval")

        assert isinstance(await r.acl_log(), list)
        assert len(await r.acl_log()) == 3
        assert len(await r.acl_log(count=1)) == 1
        assert isinstance((await r.acl_log())[0], dict)
        expected = (await r.acl_log(count=1))[0]
        assert_resp_response_in(r, "client-info", expected, expected.keys())
        assert await r.acl_log_reset()

    @skip_if_server_version_lt(REDIS_6_VERSION)
    async def test_acl_setuser_categories_without_prefix_fails(self, r_teardown):
        username = "redis-py-user"
        r = r_teardown(username)

        with pytest.raises(exceptions.DataError):
            await r.acl_setuser(username, categories=["list"])

    @skip_if_server_version_lt(REDIS_6_VERSION)
    async def test_acl_setuser_commands_without_prefix_fails(self, r_teardown):
        username = "redis-py-user"
        r = r_teardown(username)

        with pytest.raises(exceptions.DataError):
            await r.acl_setuser(username, commands=["get"])

    @skip_if_server_version_lt(REDIS_6_VERSION)
    async def test_acl_setuser_add_passwords_and_nopass_fails(self, r_teardown):
        username = "redis-py-user"
        r = r_teardown(username)

        with pytest.raises(exceptions.DataError):
            await r.acl_setuser(username, passwords="+mypass", nopass=True)

    @skip_if_server_version_lt(REDIS_6_VERSION)
    async def test_acl_users(self, r: redis.Redis):
        users = await r.acl_users()
        assert isinstance(users, list)
        assert len(users) > 0

    @skip_if_server_version_lt(REDIS_6_VERSION)
    async def test_acl_whoami(self, r: redis.Redis):
        username = await r.acl_whoami()
        assert isinstance(username, (str, bytes))

    @pytest.mark.redismod
    @skip_if_server_version_lt("7.9.0")
    async def test_acl_modules_commands(self, r_teardown):
        username = "redis-py-user"
        password = "pass-for-test-user"

        r = r_teardown(username)
        await r.flushdb()

        await r.ft().create_index((TextField("txt"),))
        await r.hset("doc1", mapping={"txt": "foo baz"})
        await r.hset("doc2", mapping={"txt": "foo bar"})

        await r.acl_setuser(
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

        await r.auth(password, username)

        assert await r.ft().search(Query("foo ~bar"))
        with pytest.raises(exceptions.NoPermissionError):
            await r.ft().dropindex()

        await r.json().set("foo", Path.root_path(), "bar")
        assert await r.json().get("foo") == "bar"
        with pytest.raises(exceptions.NoPermissionError):
            await r.json().clear("foo")

        assert await r.bf().create("bloom", 0.01, 1000)
        assert await r.cf().create("cuckoo", 1000)
        assert await r.cms().initbydim("cmsDim", 100, 5)
        assert await r.topk().reserve("topk", 5, 100, 5, 0.9)
        assert await r.tdigest().create("to-tDigest", 10)
        with pytest.raises(exceptions.NoPermissionError):
            await r.bf().info("bloom")

        assert await r.ts().create(1, labels={"Redis": "Labs"})
        with pytest.raises(exceptions.NoPermissionError):
            await r.ts().info(1)

    @pytest.mark.redismod
    @skip_if_server_version_lt("7.9.0")
    async def test_acl_modules_category_commands(self, r_teardown):
        username = "redis-py-user"
        password = "pass-for-test-user"

        r = r_teardown(username)
        await r.flushdb()

        # validate modules categories acl config
        await r.acl_setuser(
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
        await r.ft().create_index((TextField("txt"),))
        await r.hset("doc1", mapping={"txt": "foo baz"})
        await r.hset("doc2", mapping={"txt": "foo bar"})

        await r.auth(password, username)

        assert await r.ft().search(Query("foo ~bar"))
        assert await r.ft().dropindex()

        assert await r.json().set("foo", Path.root_path(), "bar")
        assert await r.json().get("foo") == "bar"

        assert await r.bf().create("bloom", 0.01, 1000)
        assert await r.bf().info("bloom")
        assert await r.cf().create("cuckoo", 1000)
        assert await r.cms().initbydim("cmsDim", 100, 5)
        assert await r.topk().reserve("topk", 5, 100, 5, 0.9)
        assert await r.tdigest().create("to-tDigest", 10)

        assert await r.ts().create(1, labels={"Redis": "Labs"})
        assert await r.ts().info(1)

    @pytest.mark.onlynoncluster
    async def test_client_list(self, r: redis.Redis):
        clients = await r.client_list()
        assert isinstance(clients[0], dict)
        assert "addr" in clients[0]

    @pytest.mark.onlynoncluster
    async def test_client_list_iter(self, r: redis.Redis):
        clients = list(await r.client_list_iter())
        assert isinstance(clients[0], dict)
        assert "addr" in clients[0]

    @skip_if_server_version_lt("5.0.0")
    async def test_client_list_type(self, r: redis.Redis):
        with pytest.raises(exceptions.RedisError):
            await r.client_list(_type="not a client type")
        for client_type in ["normal", "master", "replica", "pubsub"]:
            clients = await r.client_list(_type=client_type)
            assert isinstance(clients, list)

    @skip_if_server_version_lt("5.0.0")
    @pytest.mark.onlynoncluster
    async def test_client_id(self, r: redis.Redis):
        assert await r.client_id() > 0

    @skip_if_server_version_lt("5.0.0")
    @pytest.mark.onlynoncluster
    async def test_client_unblock(self, r: redis.Redis):
        myid = await r.client_id()
        assert not await r.client_unblock(myid)
        assert not await r.client_unblock(myid, error=True)
        assert not await r.client_unblock(myid, error=False)

    @skip_if_server_version_lt("2.6.9")
    @pytest.mark.onlynoncluster
    async def test_client_getname(self, r: redis.Redis):
        assert await r.client_getname() is None

    @skip_if_server_version_lt("2.6.9")
    @pytest.mark.onlynoncluster
    async def test_client_setname(self, r: redis.Redis):
        assert await r.client_setname("redis_py_test")
        assert_resp_response(
            r,
            await r.client_getname(),
            "redis_py_test",
            b"redis_py_test",
            "redis_py_test",
        )

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    async def test_client_info_name_with_equals(self, r: redis.Redis):
        # A client name may contain "=", which the "key=value" CLIENT INFO
        # format made easy to mis-split. Check the name survives the round trip
        # through the real server rather than only the unit-tested parser.
        await r.client_setname("test=name")
        info = await r.client_info()
        assert info["name"] == "test=name"

    @skip_if_server_version_lt("7.2.0")
    async def test_client_setinfo(self, r: redis.Redis):
        from redis.utils import get_lib_version

        await r.ping()
        info = await r.client_info()
        assert info["lib-name"] == "redis-py"
        assert info["lib-ver"] == get_lib_version()
        assert await r.client_setinfo("lib-name", "test")
        assert await r.client_setinfo("lib-ver", "123")
        info = await r.client_info()
        assert info["lib-name"] == "test"
        assert info["lib-ver"] == "123"

        # Test deprecated lib_name/lib_version parameters
        with pytest.warns(DeprecationWarning):
            r2 = redis.Redis(lib_name="test2", lib_version="1234")
        info = await r2.client_info()
        assert info["lib-name"] == "test2"
        assert info["lib-ver"] == "1234"
        await r2.aclose()

    @pytest.mark.fixed_client
    @skip_if_server_version_lt("7.2.0")
    # Exercises a default localhost client, which cannot target a remote managed
    # Redis Enterprise endpoint.
    @skip_if_redis_enterprise()
    async def test_client_setinfo_with_driver_info(self):
        from redis import DriverInfo
        from redis.utils import get_lib_version

        info = DriverInfo().add_upstream_driver("celery", "5.4.1")
        r2 = redis.Redis(driver_info=info)
        await r2.ping()
        client_info = await r2.client_info()
        assert client_info["lib-name"] == "redis-py(celery_v5.4.1)"
        assert client_info["lib-ver"] == get_lib_version()
        await r2.aclose()

    @skip_if_server_version_lt("2.6.9")
    @pytest.mark.onlynoncluster
    async def test_client_kill(self, r: redis.Redis, r2):
        await r.client_setname("redis-py-c1")
        await r2.client_setname("redis-py-c2")
        clients = [
            client
            for client in await r.client_list()
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 2

        clients_by_name = {client.get("name"): client for client in clients}

        client_addr = clients_by_name["redis-py-c2"].get("addr")
        assert await r.client_kill(client_addr) is True

        clients = [
            client
            for client in await r.client_list()
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 1
        assert clients[0].get("name") == "redis-py-c1"

    @skip_if_server_version_lt("2.8.12")
    async def test_client_kill_filter_invalid_params(self, r: redis.Redis):
        # empty
        with pytest.raises(exceptions.DataError):
            await r.client_kill_filter()

        # invalid skipme
        with pytest.raises(exceptions.DataError):
            await r.client_kill_filter(skipme="yeah")  # type: ignore

        # invalid type
        with pytest.raises(exceptions.DataError):
            await r.client_kill_filter(_type="caster")  # type: ignore

    async def test_client_kill_filter_accepts_replica_type(self, r: redis.Redis):
        with patch.object(r, "execute_command", AsyncMock(return_value=1)) as command:
            assert await r.client_kill_filter(_type="REPLICA") == 1

        command.assert_awaited_once_with("CLIENT KILL", b"TYPE", "REPLICA")

    @skip_if_server_version_lt("2.8.12")
    @pytest.mark.onlynoncluster
    async def test_client_kill_filter_by_id(self, r: redis.Redis, r2):
        await r.client_setname("redis-py-c1")
        await r2.client_setname("redis-py-c2")
        clients = [
            client
            for client in await r.client_list()
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 2

        clients_by_name = {client.get("name"): client for client in clients}

        client_2_id = clients_by_name["redis-py-c2"].get("id")
        resp = await r.client_kill_filter(_id=client_2_id)
        assert resp == 1

        clients = [
            client
            for client in await r.client_list()
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 1
        assert clients[0].get("name") == "redis-py-c1"

    @skip_if_server_version_lt("2.8.12")
    @pytest.mark.onlynoncluster
    async def test_client_kill_filter_by_addr(self, r: redis.Redis, r2):
        await r.client_setname("redis-py-c1")
        await r2.client_setname("redis-py-c2")
        clients = [
            client
            for client in await r.client_list()
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 2

        clients_by_name = {client.get("name"): client for client in clients}

        client_2_addr = clients_by_name["redis-py-c2"].get("addr")
        resp = await r.client_kill_filter(addr=client_2_addr)
        assert resp == 1

        clients = [
            client
            for client in await r.client_list()
            if client.get("name") in ["redis-py-c1", "redis-py-c2"]
        ]
        assert len(clients) == 1
        assert clients[0].get("name") == "redis-py-c1"

    @skip_if_server_version_lt("2.6.9")
    async def test_client_list_after_client_setname(self, r: redis.Redis):
        await r.client_setname("redis_py_test")
        clients = await r.client_list()
        # we don't know which client ours will be
        assert "redis_py_test" in [c["name"] for c in clients]

    @skip_if_server_version_lt("2.9.50")
    @pytest.mark.onlynoncluster
    async def test_client_pause(self, r: redis.Redis):
        assert await r.client_pause(1)
        assert await r.client_pause(timeout=1)
        with pytest.raises(exceptions.RedisError):
            await r.client_pause(timeout="not an integer")

    @skip_if_server_version_lt("7.2.0")
    @pytest.mark.onlynoncluster
    async def test_client_no_touch(self, r: redis.Redis):
        assert await r.client_no_touch("ON") == b"OK"
        assert await r.client_no_touch("OFF") == b"OK"
        with pytest.raises(TypeError):
            await r.client_no_touch()

    async def test_config_get(self, r: redis.Redis):
        data = await r.config_get()
        assert "maxmemory" in data
        assert data["maxmemory"].isdigit()

    @pytest.mark.onlynoncluster
    async def test_config_resetstat(self, r: redis.Redis):
        await r.ping()
        prior_commands_processed = int((await r.info())["total_commands_processed"])
        assert prior_commands_processed >= 1
        await r.config_resetstat()
        reset_commands_processed = int((await r.info())["total_commands_processed"])
        assert reset_commands_processed < prior_commands_processed

    async def test_config_set(self, r: redis.Redis):
        await r.config_set("timeout", 70)
        assert (await r.config_get())["timeout"] == "70"
        assert await r.config_set("timeout", 0)
        assert (await r.config_get())["timeout"] == "0"

    @pytest.mark.redismod
    @skip_if_server_version_lt("7.9.0")
    async def test_config_get_for_modules(self, r: redis.Redis):
        search_module_configs = await r.config_get("search-*")
        assert "search-timeout" in search_module_configs

        ts_module_configs = await r.config_get("ts-*")
        assert "ts-retention-policy" in ts_module_configs

        bf_module_configs = await r.config_get("bf-*")
        assert "bf-error-rate" in bf_module_configs

        cf_module_configs = await r.config_get("cf-*")
        assert "cf-initial-size" in cf_module_configs

    @pytest.mark.redismod
    @skip_if_server_version_lt("7.9.0")
    async def test_config_set_for_search_module(self, r: redis.Redis):
        config = await r.config_get("*")
        initial_default_search_dialect = config["search-default-dialect"]
        try:
            default_dialect_new = "3"
            assert await r.config_set("search-default-dialect", default_dialect_new)
            assert (await r.config_get("*"))[
                "search-default-dialect"
            ] == default_dialect_new
            search_config = await r.ft().config_get("*")
            if expects_resp2_shape(r) or expects_resp3_shape(r):
                dialect = search_config[b"DEFAULT_DIALECT"].decode()
            elif expects_unified_shape(r):
                dialect = search_config["DEFAULT_DIALECT"]
            assert dialect == default_dialect_new
        except AssertionError as ex:
            raise ex
        finally:
            assert await r.config_set(
                "search-default-dialect", initial_default_search_dialect
            )
        with pytest.raises(exceptions.ResponseError):
            await r.config_set("search-max-doctablesize", 2000000)

    @pytest.mark.onlynoncluster
    async def test_dbsize(self, r: redis.Redis):
        await r.set("a", "foo")
        await r.set("b", "bar")
        assert await r.dbsize() == 2

    @pytest.mark.onlynoncluster
    async def test_echo(self, r: redis.Redis):
        assert await r.echo("foo bar") == b"foo bar"

    @pytest.mark.onlynoncluster
    async def test_info(self, r: redis.Redis):
        await r.set("a", "foo")
        await r.set("b", "bar")
        info = await r.info()
        assert isinstance(info, dict)
        assert "arch_bits" in info.keys()
        assert "redis_version" in info.keys()

    @pytest.mark.onlynoncluster
    async def test_info_with_custom_callback(self, decoded_r):
        decoded_r.set_response_callback("INFO", lambda response: response)
        assert "redis_version:" in await decoded_r.info("server")

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    @skip_if_redis_enterprise()
    async def test_info_with_non_utf8_errorstat(
        self, create_redis, r: redis.Redis, decoded_r: redis.Redis
    ):
        await r.config_resetstat()
        with pytest.raises(redis.ResponseError):
            await r.eval(b"return redis.error_reply('\xff\xfeBAD oops')", 0)
        try:
            info = await decoded_r.info("errorstats")
            assert info["errorstat_\ufffd\ufffdBAD"] == {"count": 1}
            assert "redis_version" in await decoded_r.info()

            client = await create_redis(
                flushdb=False,
                decode_responses=True,
                encoding_errors="backslashreplace",
            )
            info = await client.info("errorstats")
            assert info["errorstat_\\xff\\xfeBAD"] == {"count": 1}
        finally:
            await r.config_resetstat()

    @pytest.mark.onlynoncluster
    async def test_lastsave(self, r: redis.Redis):
        assert isinstance(await r.lastsave(), datetime.datetime)

    async def test_object(self, r: redis.Redis):
        await r.set("a", "foo")
        assert isinstance(await r.object("refcount", "a"), int)
        assert isinstance(await r.object("idletime", "a"), int)
        assert await r.object("encoding", "a") in (b"raw", b"embstr")
        assert await r.object("idletime", "invalid-key") is None

    async def test_ping(self, r: redis.Redis):
        assert await r.ping()

    @skip_if_server_version_lt("7.0.0")
    @skip_if_redis_enterprise()
    async def test_command_getkeysandflags(self, r: redis.Redis):
        res = await r.command_getkeysandflags(
            "LMOVE", "mylist1", "mylist2", "left", "left"
        )
        assert res == [
            [b"mylist1", [b"RW", b"access", b"delete"]],
            [b"mylist2", [b"RW", b"insert"]],
        ]

    @pytest.mark.onlynoncluster
    async def test_slowlog_get(self, r: redis.Redis, slowlog):
        assert await r.slowlog_reset()
        unicode_string = chr(3456) + "abcd" + chr(3421)
        await r.get(unicode_string)
        slowlog = await r.slowlog_get()
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

    @pytest.mark.onlynoncluster
    async def test_slowlog_get_limit(self, r: redis.Redis, slowlog):
        assert await r.slowlog_reset()
        await r.get("foo")
        slowlog = await r.slowlog_get(1)
        assert isinstance(slowlog, list)
        # only one command, based on the number we passed to slowlog_get()
        assert len(slowlog) == 1

    @pytest.mark.onlynoncluster
    async def test_slowlog_length(self, r: redis.Redis, slowlog):
        await r.get("foo")
        assert isinstance(await r.slowlog_len(), int)

    @skip_if_server_version_lt("2.6.0")
    async def test_time(self, r: redis.Redis):
        t = await r.time()
        assert len(t) == 2
        assert isinstance(t[0], int)
        assert isinstance(t[1], int)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    async def test_hotkeys_start_basic(self, r: redis.Redis):
        """Test basic HOTKEYS START command with CPU metric"""
        # Reset any previous session
        try:
            await r.hotkeys_stop()
        except Exception:
            pass

        # Start collection with CPU metric
        result = await r.hotkeys_start(count=10, metrics=[HotkeysMetricsTypes.CPU])
        assert result == b"OK"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    async def test_hotkeys_start_with_all_metrics(self, r: redis.Redis):
        """Test HOTKEYS START with both CPU and NET metrics"""
        try:
            await r.hotkeys_stop()
        except Exception:
            pass

        result = await r.hotkeys_start(
            count=5, metrics=[HotkeysMetricsTypes.CPU, HotkeysMetricsTypes.NET]
        )
        assert result == b"OK"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    async def test_hotkeys_start_with_duration(self, r: redis.Redis):
        """Test HOTKEYS START with duration parameter"""
        try:
            await r.hotkeys_stop()
        except Exception:
            pass

        result = await r.hotkeys_start(
            count=10, metrics=[HotkeysMetricsTypes.CPU], duration=60
        )
        assert result == b"OK"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    async def test_hotkeys_start_with_sample_ratio(self, r: redis.Redis):
        """Test HOTKEYS START with sample ratio"""
        try:
            await r.hotkeys_stop()
        except Exception:
            pass

        result = await r.hotkeys_start(
            count=10, metrics=[HotkeysMetricsTypes.CPU], sample_ratio=10
        )
        assert result == b"OK"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    async def test_hotkeys_start_with_slots_fail_on_non_cluster_setup(
        self, r: redis.Redis
    ):
        """Test HOTKEYS START with specific hash slots"""
        try:
            await r.hotkeys_stop()
        except Exception:
            pass
        with pytest.raises(Exception):
            await r.hotkeys_start(
                count=10, metrics=[HotkeysMetricsTypes.CPU], slots=[0, 100, 200]
            )

    @pytest.mark.onlynoncluster
    async def test_hotkeys_start_empty_slots_omits_slots(self, r: redis.Redis):
        """Test HOTKEYS START does not send SLOTS for an empty slots list"""
        with patch.object(r, "execute_command", AsyncMock(return_value=b"OK")) as cmd:
            await r.hotkeys_start(metrics=[HotkeysMetricsTypes.CPU], slots=[])
        cmd.assert_called_once_with("HOTKEYS", "START", "METRICS", 1, "CPU")

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    async def test_hotkeys_start_with_all_parameters(self, r: redis.Redis):
        """Test HOTKEYS START with all optional parameters"""
        try:
            await r.hotkeys_stop()
        except Exception:
            pass

        result = await r.hotkeys_start(
            count=5,
            metrics=[HotkeysMetricsTypes.CPU, HotkeysMetricsTypes.NET],
            duration=30,
            sample_ratio=5,
        )
        assert result == b"OK"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    async def test_hotkeys_stop(self, r: redis.Redis):
        """Test HOTKEYS STOP command"""
        try:
            await r.hotkeys_stop()
        except Exception:
            pass

        # Start a collection session
        await r.hotkeys_start(count=10, metrics=[HotkeysMetricsTypes.CPU])

        # Stop the session
        result = await r.hotkeys_stop()
        assert result == b"OK"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    async def test_hotkeys_reset(self, r: redis.Redis):
        """Test HOTKEYS RESET command"""
        try:
            await r.hotkeys_stop()
        except Exception:
            pass

        # Start a session and generate some data
        await r.hotkeys_start(count=10, metrics=[HotkeysMetricsTypes.CPU])

        # Perform some operations to generate hotkeys data
        for i in range(5):
            await r.set(f"resetkey{i}", f"value{i}")
            await r.get(f"resetkey{i}")

        # Stop the session
        await r.hotkeys_stop()

        # Get results before reset - should have data
        result_before = await r.hotkeys_get()
        assert isinstance(result_before, list)
        for res_elem in result_before:
            assert isinstance(res_elem, dict)
            assert b"tracking-active" in res_elem

        # Reset the results
        result = await r.hotkeys_reset()
        assert result == b"OK"

        # No session exists after reset
        assert await r.hotkeys_get() is None

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    async def test_hotkeys_get_no_session(self, r: redis.Redis):
        """Test HOTKEYS GET when there is no collection session"""
        try:
            await r.hotkeys_stop()
        except Exception:
            pass
        await r.hotkeys_reset()

        assert await r.hotkeys_get() is None

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    async def test_hotkeys_get_ongoing_session(self, r: redis.Redis):
        """Test HOTKEYS GET during an ongoing collection session"""
        try:
            await r.hotkeys_stop()
        except Exception:
            pass

        # Start a collection session
        await r.hotkeys_start(
            count=10, metrics=[HotkeysMetricsTypes.CPU, HotkeysMetricsTypes.NET]
        )

        # Perform some operations to generate hotkeys data
        for i in range(10):
            await r.set(f"key{i}", f"value{i}")
            await r.get(f"key{i}")

        # Get the results
        result = await r.hotkeys_get()

        # Verify the response structure
        assert isinstance(result, list)
        for res_elem in result:
            assert isinstance(res_elem, dict)
            # Check tracking-active is 1 (ongoing session)
            assert res_elem[b"tracking-active"] == 1

        # Stop the session
        await r.hotkeys_stop()

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    async def test_hotkeys_get_terminated_session(self, r: redis.Redis):
        """Test HOTKEYS GET after stopping a collection session"""
        try:
            await r.hotkeys_stop()
        except Exception:
            pass

        # Start a collection session
        await r.hotkeys_start(count=10, metrics=[HotkeysMetricsTypes.CPU])

        # Perform some operations
        for i in range(5):
            await r.set(f"testkey{i}", f"testvalue{i}")

        # Stop the session
        await r.hotkeys_stop()

        # Get the results
        result = await r.hotkeys_get()

        # Verify the response structure
        assert isinstance(result, list)
        for res_elem in result:
            assert isinstance(res_elem, dict)
            # Check tracking-active is 0 (terminated session)
            assert res_elem[b"tracking-active"] == 0

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    async def test_hotkeys_get_all_fields(self, r: redis.Redis):
        """Test HOTKEYS GET returns all documented fields"""
        try:
            await r.hotkeys_stop()
        except Exception:
            pass

        # Start a collection session with all parameters
        await r.hotkeys_start(
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
        await pipe.execute()

        # Stop the session
        await r.hotkeys_stop()

        # Get the results
        result = await r.hotkeys_get()

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

        for elem in result:
            for field in expected_fields:
                assert field in elem, (
                    f"Field '{field}' is missing from HOTKEYS GET response"
                )

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.5.240")
    @skip_if_redis_enterprise()
    async def test_hotkeys_get_all_fields_decoded(self, decoded_r: redis.Redis):
        """Test HOTKEYS GET returns all documented fields"""
        try:
            await decoded_r.hotkeys_stop()
        except Exception:
            pass

        # Start a collection session with all parameters
        await decoded_r.hotkeys_start(
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
        await pipe.execute()

        # Stop the session
        await decoded_r.hotkeys_stop()

        # Get the results
        result = await decoded_r.hotkeys_get()

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

    async def test_never_decode_option(self, r: redis.Redis):
        opts = {NEVER_DECODE: []}
        await r.delete("a")
        assert await r.execute_command("EXISTS", "a", **opts) == 0

    async def test_empty_response_option(self, r: redis.Redis):
        opts = {EMPTY_RESPONSE: []}
        await r.delete("a")
        assert await r.execute_command("EXISTS", "a", **opts) == 0

    # BASIC KEY COMMANDS
    async def test_append(self, r: redis.Redis):
        assert await r.append("a", "a1") == 2
        assert await r.get("a") == b"a1"
        assert await r.append("a", "a2") == 4
        assert await r.get("a") == b"a1a2"

    @skip_if_server_version_lt("2.6.0")
    async def test_bitcount(self, r: redis.Redis):
        await r.setbit("a", 5, True)
        assert await r.bitcount("a") == 1
        await r.setbit("a", 6, True)
        assert await r.bitcount("a") == 2
        await r.setbit("a", 5, False)
        assert await r.bitcount("a") == 1
        await r.setbit("a", 9, True)
        await r.setbit("a", 17, True)
        await r.setbit("a", 25, True)
        await r.setbit("a", 33, True)
        assert await r.bitcount("a") == 5
        assert await r.bitcount("a", 0, -1) == 5
        assert await r.bitcount("a", 2, 3) == 2
        assert await r.bitcount("a", 2, -1) == 3
        assert await r.bitcount("a", -2, -1) == 2
        assert await r.bitcount("a", 1, 1) == 1

    @skip_if_server_version_lt("2.6.0")
    @pytest.mark.onlynoncluster
    async def test_bitop_not_empty_string(self, r: redis.Redis):
        await r.set("a", "")
        await r.bitop("not", "r", "a")
        assert await r.get("r") is None

    @skip_if_server_version_lt("2.6.0")
    @pytest.mark.onlynoncluster
    async def test_bitop_not(self, r: redis.Redis):
        test_str = b"\xaa\x00\xff\x55"
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        await r.set("a", test_str)
        await r.bitop("not", "r", "a")
        assert int(binascii.hexlify(await r.get("r")), 16) == correct

    @skip_if_server_version_lt("2.6.0")
    @pytest.mark.onlynoncluster
    async def test_bitop_not_in_place(self, r: redis.Redis):
        test_str = b"\xaa\x00\xff\x55"
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        await r.set("a", test_str)
        await r.bitop("not", "a", "a")
        assert int(binascii.hexlify(await r.get("a")), 16) == correct

    @skip_if_server_version_lt("2.6.0")
    @pytest.mark.onlynoncluster
    async def test_bitop_single_string(self, r: redis.Redis):
        test_str = b"\x01\x02\xff"
        await r.set("a", test_str)
        await r.bitop("and", "res1", "a")
        await r.bitop("or", "res2", "a")
        await r.bitop("xor", "res3", "a")
        assert await r.get("res1") == test_str
        assert await r.get("res2") == test_str
        assert await r.get("res3") == test_str

    @skip_if_server_version_lt("2.6.0")
    @pytest.mark.onlynoncluster
    async def test_bitop_string_operands(self, r: redis.Redis):
        await r.set("a", b"\x01\x02\xff\xff")
        await r.set("b", b"\x01\x02\xff")
        await r.bitop("and", "res1", "a", "b")
        await r.bitop("or", "res2", "a", "b")
        await r.bitop("xor", "res3", "a", "b")
        assert int(binascii.hexlify(await r.get("res1")), 16) == 0x0102FF00
        assert int(binascii.hexlify(await r.get("res2")), 16) == 0x0102FFFF
        assert int(binascii.hexlify(await r.get("res3")), 16) == 0x000000FF

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.1.224")
    async def test_bitop_diff(self, r: redis.Redis):
        await r.set("a", b"\xf0")
        await r.set("b", b"\xc0")
        await r.set("c", b"\x80")

        result = await r.bitop("DIFF", "result", "a", "b", "c")
        assert result == 1
        assert await r.get("result") == b"\x30"

        await r.bitop("DIFF", "result2", "a", "nonexistent")
        assert await r.get("result2") == b"\xf0"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.1.224")
    async def test_bitop_diff1(self, r: redis.Redis):
        await r.set("a", b"\xf0")
        await r.set("b", b"\xc0")
        await r.set("c", b"\x80")

        result = await r.bitop("DIFF1", "result", "a", "b", "c")
        assert result == 1
        assert await r.get("result") == b"\x00"

        await r.set("d", b"\x0f")
        await r.set("e", b"\x03")
        await r.bitop("DIFF1", "result2", "d", "e")
        assert await r.get("result2") == b"\x00"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.1.224")
    async def test_bitop_andor(self, r: redis.Redis):
        await r.set("a", b"\xf0")
        await r.set("b", b"\xc0")
        await r.set("c", b"\x80")

        result = await r.bitop("ANDOR", "result", "a", "b", "c")
        assert result == 1
        assert await r.get("result") == b"\xc0"

        await r.set("x", b"\xf0")
        await r.set("y", b"\x0f")
        await r.bitop("ANDOR", "result2", "x", "y")
        assert await r.get("result2") == b"\x00"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.1.224")
    async def test_bitop_one(self, r: redis.Redis):
        await r.set("a", b"\xf0")
        await r.set("b", b"\xc0")
        await r.set("c", b"\x80")

        result = await r.bitop("ONE", "result", "a", "b", "c")
        assert result == 1
        assert await r.get("result") == b"\x30"

        await r.set("x", b"\xf0")
        await r.set("y", b"\x0f")
        await r.bitop("ONE", "result2", "x", "y")
        assert await r.get("result2") == b"\xff"

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.1.224")
    async def test_bitop_new_operations_with_empty_keys(self, r: redis.Redis):
        await r.set("a", b"\xff")

        await r.bitop("DIFF", "empty_result", "nonexistent", "a")
        assert await r.get("empty_result") == b"\x00"

        await r.bitop("DIFF1", "empty_result2", "a", "nonexistent")
        assert await r.get("empty_result2") == b"\x00"

        await r.bitop("ANDOR", "empty_result3", "a", "nonexistent")
        assert await r.get("empty_result3") == b"\x00"

        await r.bitop("ONE", "empty_result4", "nonexistent")
        assert await r.get("empty_result4") is None

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.1.224")
    async def test_bitop_new_operations_return_values(self, r: redis.Redis):
        await r.set("a", b"\xff\x00\xff")
        await r.set("b", b"\x00\xff")

        result1 = await r.bitop("DIFF", "result1", "a", "b")
        assert result1 == 3

        result2 = await r.bitop("DIFF1", "result2", "a", "b")
        assert result2 == 3

        result3 = await r.bitop("ANDOR", "result3", "a", "b")
        assert result3 == 3

        result4 = await r.bitop("ONE", "result4", "a", "b")
        assert result4 == 3

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.7")
    async def test_bitpos(self, r: redis.Redis):
        key = "key:bitpos"
        await r.set(key, b"\xff\xf0\x00")
        assert await r.bitpos(key, 0) == 12
        assert await r.bitpos(key, 0, 2, -1) == 16
        assert await r.bitpos(key, 0, -2, -1) == 12
        await r.set(key, b"\x00\xff\xf0")
        assert await r.bitpos(key, 1, 0) == 8
        assert await r.bitpos(key, 1, 1) == 8
        await r.set(key, b"\x00\x00\x00")
        assert await r.bitpos(key, 1) == -1

    @skip_if_server_version_lt("2.8.7")
    async def test_bitpos_wrong_arguments(self, r: redis.Redis):
        key = "key:bitpos:wrong:args"
        await r.set(key, b"\xff\xf0\x00")
        with pytest.raises(exceptions.RedisError):
            await r.bitpos(key, 0, end=1) == 12
        with pytest.raises(exceptions.RedisError):
            await r.bitpos(key, 7) == 12

    async def test_decr(self, r: redis.Redis):
        assert await r.decr("a") == -1
        assert await r.get("a") == b"-1"
        assert await r.decr("a") == -2
        assert await r.get("a") == b"-2"
        assert await r.decr("a", amount=5) == -7
        assert await r.get("a") == b"-7"

    async def test_decrby(self, r: redis.Redis):
        assert await r.decrby("a", amount=2) == -2
        assert await r.decrby("a", amount=3) == -5
        assert await r.get("a") == b"-5"

    async def test_delete(self, r: redis.Redis):
        assert await r.delete("a") == 0
        await r.set("a", "foo")
        assert await r.delete("a") == 1

    async def test_delete_with_multiple_keys(self, r: redis.Redis):
        await r.set("a", "foo")
        await r.set("b", "bar")
        assert await r.delete("a", "b") == 2
        assert await r.get("a") is None
        assert await r.get("b") is None

    async def test_delitem(self, r: redis.Redis):
        await r.set("a", "foo")
        await r.delete("a")
        assert await r.get("a") is None

    def _ensure_str(self, x):
        return x.decode("ascii") if isinstance(x, (bytes, bytearray)) else x

    async def _server_xxh3_digest(self, r, key):
        """
        Get the server-computed XXH3 hex digest for the key's value.
        Requires the DIGEST command implemented on the server.
        """
        d = await r.execute_command("DIGEST", key)
        return None if d is None else self._ensure_str(d).lower()

    @skip_if_server_version_lt("8.3.224")
    async def test_delex_nonexistent(self, r):
        await r.delete("nope")
        assert await r.delex("nope") == 0

    @skip_if_server_version_lt("8.3.224")
    async def test_delex_unconditional_delete_string(self, r):
        await r.set("k", b"v")
        assert await r.exists("k") == 1
        assert await r.delex("k") == 1
        assert await r.exists("k") == 0

    @skip_if_server_version_lt("8.3.224")
    async def test_delex_unconditional_delete_nonstring_allowed(self, r):
        # Spec: error happens only when a condition is specified on a non-string key.
        await r.lpush("lst", "a")
        assert await r.delex("lst") == 1
        assert await r.exists("lst") == 0

        await r.lpush("lst", "a")

        with pytest.raises(redis.ResponseError):
            await r.delex("lst", ifeq=b"a")
        assert await r.exists("lst") == 1

    @skip_if_server_version_lt("8.3.224")
    async def test_delex_ifeq(self, r):
        await r.set("k", b"abc")
        assert await r.delex("k", ifeq=b"abc") == 1  # matches → deleted
        assert await r.exists("k") == 0

        await r.set("k", b"abc")
        assert await r.delex("k", ifeq=b"zzz") == 0  # not match → not deleted
        assert await r.get("k") == b"abc"  # still there

    @skip_if_server_version_lt("8.3.224")
    async def test_delex_ifne(self, r):
        await r.set("k2", b"abc")
        assert await r.delex("k2", ifne=b"zzz") == 1  # different → deleted
        assert await r.exists("k2") == 0

        await r.set("k2", b"abc")
        assert await r.delex("k2", ifne=b"abc") == 0  # equal → not deleted
        assert await r.get("k2") == b"abc"

    @skip_if_server_version_lt("8.3.224")
    async def test_delex_with_conditionon_nonstring_values(self, r):
        await r.lpush("nk", "x")
        with pytest.raises(redis.ResponseError):
            await r.delex("nk", ifeq=b"x")
        with pytest.raises(redis.ResponseError):
            await r.delex("nk", ifne=b"x")
        with pytest.raises(redis.ResponseError):
            await r.delex("nk", ifdeq="deadbeef")

    @skip_if_server_version_lt("8.3.224")
    @pytest.mark.parametrize("val", [b"", b"abc", b"The quick brown fox"])
    async def test_delex_ifdeq_and_ifdne(self, r, val):
        await r.set("h", val)
        d = await self._server_xxh3_digest(r, "h")
        assert d is not None

        # IFDEQ should delete with exact digest
        await r.set("h", val)
        assert await r.delex("h", ifdeq=d) == 1
        assert await r.exists("h") == 0

        # IFDNE should NOT delete when digest matches
        await r.set("h", val)
        assert await r.delex("h", ifdne=d) == 0
        assert await r.get("h") == val

        # IFDNE should delete when digest doesn't match
        await r.set("h", val)
        wrong = "0" * len(d)
        if wrong == d:
            wrong = "f" * len(d)
        assert await r.delex("h", ifdne=wrong) == 1
        assert await r.exists("h") == 0

    @skip_if_server_version_lt("8.3.224")
    async def test_delex_pipeline(self, r):
        await r.mset({"p1{45}": b"A", "p2{45}": b"B"})
        p = r.pipeline()
        p.delex("p1{45}", ifeq=b"A")
        p.delex("p2{45}", ifne=b"B")  # false → 0
        p.delex("nope")  # nonexistent → 0
        out = await p.execute()
        assert out == [1, 0, 0]

    @skip_if_server_version_lt("8.3.224")
    async def test_delex_mutual_exclusion_client_side(self, r):
        with pytest.raises(ValueError):
            await r.delex("k", ifeq=b"A", ifne=b"B")
        with pytest.raises(ValueError):
            await r.delex("k", ifdeq="aa", ifdne="bb")

    @skip_if_server_version_lt("4.0.0")
    async def test_unlink(self, r: redis.Redis):
        assert await r.unlink("a") == 0
        await r.set("a", "foo")
        assert await r.unlink("a") == 1
        assert await r.get("a") is None

    @skip_if_server_version_lt("4.0.0")
    async def test_unlink_with_multiple_keys(self, r: redis.Redis):
        await r.set("a", "foo")
        await r.set("b", "bar")
        assert await r.unlink("a", "b") == 2
        assert await r.get("a") is None
        assert await r.get("b") is None

    @skip_if_server_version_lt("2.6.0")
    async def test_dump_and_restore(self, r: redis.Redis):
        await r.set("a", "foo")
        dumped = await r.dump("a")
        await r.delete("a")
        await r.restore("a", 0, dumped)
        assert await r.get("a") == b"foo"

    @skip_if_server_version_lt("3.0.0")
    async def test_dump_and_restore_and_replace(self, r: redis.Redis):
        await r.set("a", "bar")
        dumped = await r.dump("a")
        with pytest.raises(redis.ResponseError):
            await r.restore("a", 0, dumped)

        await r.restore("a", 0, dumped, replace=True)
        assert await r.get("a") == b"bar"

    @skip_if_server_version_lt("5.0.0")
    async def test_dump_and_restore_absttl(self, r: redis.Redis):
        await r.set("a", "foo")
        dumped = await r.dump("a")
        await r.delete("a")
        ttl = int(
            (await redis_server_time(r) + datetime.timedelta(minutes=1)).timestamp()
            * 1000
        )
        await r.restore("a", ttl, dumped, absttl=True)
        assert await r.get("a") == b"foo"
        assert 0 < await r.ttl("a") <= 61

    async def test_exists(self, r: redis.Redis):
        assert await r.exists("a") == 0
        await r.set("a", "foo")
        await r.set("b", "bar")
        assert await r.exists("a") == 1
        assert await r.exists("a", "b") == 2

    async def test_exists_contains(self, r: redis.Redis):
        assert not await r.exists("a")
        await r.set("a", "foo")
        assert await r.exists("a")

    async def test_expire(self, r: redis.Redis):
        assert not await r.expire("a", 10)
        await r.set("a", "foo")
        assert await r.expire("a", 10)
        assert 0 < await r.ttl("a") <= 10
        assert await r.persist("a")
        assert await r.ttl("a") == -1

    async def test_expireat_datetime(self, r: redis.Redis):
        expire_at = await redis_server_time(r) + datetime.timedelta(minutes=1)
        await r.set("a", "foo")
        assert await r.expireat("a", expire_at)
        assert 0 < await r.ttl("a") <= 61

    async def test_expireat_no_key(self, r: redis.Redis):
        expire_at = await redis_server_time(r) + datetime.timedelta(minutes=1)
        assert not await r.expireat("a", expire_at)

    async def test_expireat_unixtime(self, r: redis.Redis):
        expire_at = await redis_server_time(r) + datetime.timedelta(minutes=1)
        await r.set("a", "foo")
        expire_at_seconds = int(expire_at.timestamp())
        assert await r.expireat("a", expire_at_seconds)
        assert 0 < await r.ttl("a") <= 61

    @skip_if_server_version_lt("8.3.224")
    async def test_digest_nonexistent_returns_none(self, r):
        assert await r.digest("no:such:key") is None

    @skip_if_server_version_lt("8.3.224")
    async def test_digest_wrong_type_raises(self, r):
        await r.lpush("alist", "x")
        with pytest.raises(Exception):  # or redis.exceptions.ResponseError
            await r.digest("alist")

    @skip_if_server_version_lt("8.3.224")
    @pytest.mark.parametrize(
        "value", [b"", b"abc", b"The quick brown fox jumps over the lazy dog"]
    )
    async def test_digest_response_when_available(self, r, value):
        key = "k:digest"
        await r.delete(key)
        await r.set(key, value)

        res = await r.digest(key)
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
    async def test_local_digest_matches_server(self, r, value):
        key = "k:digest"
        await r.delete(key)
        await r.set(key, value)

        res_server = await r.digest(key)

        # Caution! This one is not executing execute_command and it is not async
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

    @skip_if_server_version_lt("8.3.224")
    async def test_pipeline_digest(self, r):
        k1, k2 = "k:d1{42}", "k:d2{42}"
        await r.mset({k1: b"A", k2: b"B"})
        p = r.pipeline()
        p.digest(k1)
        p.digest(k2)
        out = await p.execute()
        assert len(out) == 2
        for d in out:
            if isinstance(d, bytes):
                d = d.decode()
            assert d is None or len(d) == 16

    async def test_get_and_set(self, r: redis.Redis):
        # get and set can't be tested independently of each other
        assert await r.get("a") is None
        byte_string = b"value"
        integer = 5
        unicode_string = chr(3456) + "abcd" + chr(3421)
        assert await r.set("byte_string", byte_string)
        assert await r.set("integer", 5)
        assert await r.set("unicode_string", unicode_string)
        assert await r.get("byte_string") == byte_string
        assert await r.get("integer") == str(integer).encode()
        assert (await r.get("unicode_string")).decode("utf-8") == unicode_string

    async def test_getex_zero_expiry_options_are_mutually_exclusive(
        self, r: redis.Redis
    ):
        with pytest.raises(DataError):
            await r.getex("a", ex=0, px=1)

    async def test_get_set_bit(self, r: redis.Redis):
        # no value
        assert not await r.getbit("a", 5)
        # set bit 5
        assert not await r.setbit("a", 5, True)
        assert await r.getbit("a", 5)
        # unset bit 4
        assert not await r.setbit("a", 4, False)
        assert not await r.getbit("a", 4)
        # set bit 4
        assert not await r.setbit("a", 4, True)
        assert await r.getbit("a", 4)
        # set bit 5 again
        assert await r.setbit("a", 5, True)
        assert await r.getbit("a", 5)

    async def test_getrange(self, r: redis.Redis):
        await r.set("a", "foo")
        assert await r.getrange("a", 0, 0) == b"f"
        assert await r.getrange("a", 0, 2) == b"foo"
        assert await r.getrange("a", 3, 4) == b""

    async def test_getset(self, r: redis.Redis):
        assert await r.getset("a", "foo") is None
        assert await r.getset("a", "bar") == b"foo"
        assert await r.get("a") == b"bar"

    async def test_incr(self, r: redis.Redis):
        assert await r.incr("a") == 1
        assert await r.get("a") == b"1"
        assert await r.incr("a") == 2
        assert await r.get("a") == b"2"
        assert await r.incr("a", amount=5) == 7
        assert await r.get("a") == b"7"

    async def test_incrby(self, r: redis.Redis):
        assert await r.incrby("a") == 1
        assert await r.incrby("a", 4) == 5
        assert await r.get("a") == b"5"

    @skip_if_server_version_lt("2.6.0")
    async def test_incrbyfloat(self, r: redis.Redis):
        assert await r.incrbyfloat("a") == 1.0
        assert await r.get("a") == b"1"
        assert await r.incrbyfloat("a", 1.1) == 2.1
        assert float(await r.get("a")) == float(2.1)

    @skip_if_server_version_lt("8.7.0")
    async def test_increx_default(self, r: redis.Redis):
        key = "increx:default"
        assert await r.set(key, 10)
        assert await r.increx(key) == [11, 1]
        assert await r.get(key) == b"11"

    @skip_if_server_version_lt("8.7.0")
    async def test_increx_byint(self, r: redis.Redis):
        key = "increx:byint"
        assert await r.set(key, 20)
        assert await r.increx(key, byint=4) == [24, 4]
        assert await r.get(key) == b"24"

    @skip_if_server_version_lt("8.7.0")
    async def test_increx_byfloat(self, r: redis.Redis):
        key = "increx:float"
        assert await r.set(key, "1.5")
        result = await r.increx(key, byfloat=0.25, lbound=0, ubound=2)
        assert [float(value) for value in result] == pytest.approx([1.75, 0.25])
        assert float(await r.get(key)) == pytest.approx(1.75)

    @pytest.mark.fixed_client
    @skip_if_server_version_lt("8.7.0")
    @pytest.mark.parametrize("protocol", [2, 3])
    async def test_increx_unified_responses(self, create_redis, protocol):
        r = await create_redis(protocol=protocol, legacy_responses=False)
        key = "increx:unified"
        assert await r.set(key, "1.5")
        assert await r.increx(key, byfloat=0.25) == [1.75, 0.25]
        # A rejected increment returns the current value and a zero increment.
        result = await r.increx(key, byfloat=0.75, ubound=2)
        assert result == [1.75, 0.0]
        assert all(type(value) is float for value in result)
        assert await r.set(key, 20)
        result = await r.increx(key, byint=4)
        assert result == [24, 4]
        assert all(type(value) is int for value in result)

    @skip_if_server_version_lt("8.7.0")
    async def test_increx_saturating_bounds(self, r: redis.Redis):
        key = "increx:sat"
        assert await r.set(key, "1.8")
        result = await r.increx(key, byfloat=0.7, ubound=2, saturate=True)
        assert [float(value) for value in result] == pytest.approx([2.0, 0.2])
        assert float(await r.get(key)) == pytest.approx(2.0)

    @skip_if_server_version_lt("8.7.0")
    async def test_increx_overflow_rejected_keeps_value_and_ttl(self, r: redis.Redis):
        key = "increx:overflow"
        assert await r.set(key, 10)
        assert await r.expire(key, 60)
        ttl = await r.pttl(key)
        assert await r.increx(key, byint=5, ubound=12) == [10, 0]
        assert await r.get(key) == b"10"
        assert 0 < await r.pttl(key) <= ttl

    @skip_if_server_version_lt("8.7.0")
    async def test_increx_expiration_enx(self, r: redis.Redis):
        key = "increx:expiration"
        assert await r.set(key, 40)
        assert await r.ttl(key) == -1

        assert await r.increx(key, byint=2, ex=60, enx=True) == [42, 2]
        ttl = await r.ttl(key)
        assert 0 < ttl <= 60

        assert await r.increx(key, byint=3, ex=600, enx=True) == [45, 3]
        assert 0 < await r.ttl(key) <= ttl

    async def test_increx_invalid_options(self, r: redis.Redis):
        key = "increx:invalid"
        assert await r.set(key, 1)
        with pytest.raises(DataError):
            await r.increx(key, byfloat=1.0, byint=1)
        with pytest.raises(DataError):
            await r.increx(key, ex=10, px=10)
        with pytest.raises(DataError):
            await r.increx(key, enx=True)

    async def test_increx_zero_expiry_options_are_mutually_exclusive(
        self, r: redis.Redis
    ):
        with pytest.raises(DataError):
            await r.increx("a", ex=0, px=1)

    @pytest.mark.onlynoncluster
    async def test_keys(self, r: redis.Redis):
        assert await r.keys() == []
        keys_with_underscores = {b"test_a", b"test_b"}
        keys = keys_with_underscores.union({b"testc"})
        for key in keys:
            await r.set(key, 1)
        assert set(await r.keys(pattern="test_*")) == keys_with_underscores
        assert set(await r.keys(pattern="test*")) == keys

    @pytest.mark.onlynoncluster
    async def test_mget(self, r: redis.Redis):
        assert await r.mget([]) == []
        assert await r.mget(["a", "b"]) == [None, None]
        await r.set("a", "1")
        await r.set("b", "2")
        await r.set("c", "3")
        assert await r.mget("a", "other", "b", "c") == [b"1", None, b"2", b"3"]

    @pytest.mark.onlynoncluster
    async def test_mset(self, r: redis.Redis):
        d = {"a": b"1", "b": b"2", "c": b"3"}
        assert await r.mset(d)
        for k, v in d.items():
            assert await r.get(k) == v

    @pytest.mark.onlycluster
    async def test_mset_on_cluster(self, r):
        # validate that mset command works in cluster client
        # when the keys are in the same slot
        d = {"a:{test:1}": b"1", "b:{test:1}": b"2", "c:{test:1}": b"3"}
        assert await r.mset(d)
        for k, v in d.items():
            assert await r.get(k) == v

    @pytest.mark.onlycluster
    async def test_mset_on_cluster_multiple_slots(self, r):
        d = {"a": b"1", "b": b"2", "c": b"3"}
        with pytest.raises(RedisClusterException):
            assert await r.mset(d)

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("8.3.224")
    async def test_msetex_no_expiration_with_cluster_client(self, r):
        all_test_keys = ["1:{test:1}", "2:{test:1}"]
        for key in all_test_keys:
            await r.delete(key)

        # set items from mapping without expiration
        assert await r.msetex(mapping={"1:{test:1}": 1, "2:{test:1}": b"four"}) == 1
        assert await r.mget("1:{test:1}", "2:{test:1}") == [b"1", b"four"]
        assert await r.ttl("1:{test:1}") == -1
        assert await r.ttl("2:{test:1}") == -1

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("8.3.224")
    async def test_msetex_expiration_ex_and_keepttl_with_cluster_client(self, r):
        all_test_keys = ["1:{test:1}", "2:{test:1}"]
        for key in all_test_keys:
            await r.delete(key)

        # set items from mapping with expiration - testing ex field
        assert (
            await r.msetex(
                mapping={"1:{test:1}": 1, "2:{test:1}": "2"},
                ex=10,
            )
            == 1
        )
        ttls = await asyncio.gather(*[r.ttl(key) for key in all_test_keys])
        for ttl in ttls:
            assert pytest.approx(ttl) == 10

        assert await r.mget(*all_test_keys) == [b"1", b"2"]
        await asyncio.sleep(0.01)
        # validate keepttl: the TTL keeps counting down instead of being cleared
        assert await r.msetex(mapping={"1:{test:1}": 11}, keepttl=True) == 1
        assert 0 < await r.pttl("1:{test:1}") < 10000

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("8.3.224")
    async def test_msetex_expiration_px_with_cluster_client(self, r):
        all_test_keys = ["1:{test:1}", "2:{test:1}"]
        for key in all_test_keys:
            await r.delete(key)

        mapping = {"1:{test:1}": 1, "2:{test:1}": "2"}
        # set key/value pairs provided in mapping
        # with expiration - testing px field
        assert await r.msetex(mapping=mapping, px=60000) == 1

        ttls = await asyncio.gather(*[r.ttl(key) for key in mapping.keys()])
        for ttl in ttls:
            assert pytest.approx(ttl) == 60
        assert await r.mget(*mapping.keys()) == [b"1", b"2"]

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("8.3.224")
    async def test_msetex_expiration_pxat_and_nx_with_cluster_client(self, r):
        all_test_keys = [
            "1:{test:1}",
            "2:{test:1}",
            "3:{test:1}",
            "new:{test:1}",
            "new_2:{test:1}",
        ]
        for key in all_test_keys:
            await r.delete(key)

        mapping = {"1:{test:1}": 1, "2:{test:1}": "2", "3:{test:1}": "three"}
        assert await r.msetex(mapping=mapping, ex=30) == 1

        # NX is set with existing keys - nothing should be saved or updated
        expire_at = await redis_server_time(r) + datetime.timedelta(seconds=10)
        assert (
            await r.msetex(
                mapping={"1:{test:1}": "new_value", "new:{test:1}": "ok"},
                pxat=expire_at,
                data_persist_option=DataPersistOptions.NX,
            )
            == 0
        )
        ttls = await asyncio.gather(*[r.ttl(key) for key in mapping.keys()])
        for ttl in ttls:
            assert 10 < ttl <= 30
        assert await r.mget(*mapping.keys(), "new:{test:1}") == [
            b"1",
            b"2",
            b"three",
            None,
        ]

        # NX is set with non existing keys - values should be set
        assert (
            await r.msetex(
                mapping={"new:{test:1}": "ok", "new_2:{test:1}": "ok_2"},
                pxat=expire_at,
                data_persist_option=DataPersistOptions.NX,
            )
            == 1
        )
        old_ttls = await asyncio.gather(*[r.ttl(key) for key in mapping.keys()])
        new_ttls = await asyncio.gather(*(r.ttl(key) for key in ["new", "new_2"]))
        for ttl in old_ttls:
            assert 10 < ttl <= 30
        for ttl in new_ttls:
            assert ttl <= 11
        assert await r.mget(*mapping.keys(), "new:{test:1}", "new_2:{test:1}") == [
            b"1",
            b"2",
            b"three",
            b"ok",
            b"ok_2",
        ]

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("8.3.224")
    async def test_msetex_expiration_exat_and_xx_with_cluster_client(self, r):
        all_test_keys = ["1:{test:1}", "2:{test:1}", "3:{test:1}", "new:{test:1}"]
        for key in all_test_keys:
            await r.delete(key)

        mapping = {"1:{test:1}": 1, "2:{test:1}": "2", "3:{test:1}": "three"}
        assert await r.msetex(mapping, ex=30) == 1

        expire_at = await redis_server_time(r) + datetime.timedelta(seconds=10)
        ## XX is set with unexisting key - nothing should be saved or updated
        assert (
            await r.msetex(
                mapping={"1:{test:1}": "new_value", "new:{test:1}": "ok"},
                exat=expire_at,
                data_persist_option=DataPersistOptions.XX,
            )
            == 0
        )
        ttls = await asyncio.gather(*[r.ttl(key) for key in mapping.keys()])
        for ttl in ttls:
            assert 10 < ttl <= 30
        assert await r.mget(*mapping.keys(), "new:{test:1}") == [
            b"1",
            b"2",
            b"three",
            None,
        ]

        # XX is set with existing keys - values should be updated
        assert (
            await r.msetex(
                mapping={"1:{test:1}": "new_value", "2:{test:1}": "new_value_2"},
                exat=expire_at,
                data_persist_option=DataPersistOptions.XX,
            )
            == 1
        )
        ttls = await asyncio.gather(*[r.ttl(key) for key in mapping.keys()])
        assert ttls[0] <= 11
        assert ttls[1] <= 11
        assert 10 < ttls[2] <= 30
        assert await r.mget(
            "1:{test:1}", "2:{test:1}", "3:{test:1}", "new:{test:1}"
        ) == [
            b"new_value",
            b"new_value_2",
            b"three",
            None,
        ]

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("8.3.224")
    async def test_msetex_invalid_inputs_with_cluster_client(self, r):
        mapping = {"1": 1, "2": "2", "3": "three"}
        with pytest.raises(exceptions.RedisClusterException):
            await r.msetex(mapping)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.3.224")
    async def test_msetex_no_expiration(self, r):
        all_test_keys = ["1", "2"]
        for key in all_test_keys:
            await r.delete(key)

        # # set items from mapping without expiration
        assert await r.msetex(mapping={"1": 1, "2": b"four"}) == 1
        assert await r.mget("1", "2") == [b"1", b"four"]
        assert await r.ttl("1") == -1
        assert await r.ttl("2") == -1

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.3.224")
    async def test_msetex_expiration_ex_and_keepttl(self, r):
        all_test_keys = ["1", "2"]
        for key in all_test_keys:
            await r.delete(key)

        # set items from mapping with expiration - testing ex field
        assert (
            await r.msetex(
                mapping={"1": 1, "2": "2"},
                ex=10,
            )
            == 1
        )
        ttls = await asyncio.gather(*[r.ttl(key) for key in all_test_keys])
        for ttl in ttls:
            assert pytest.approx(ttl) == 10

        assert await r.mget(*all_test_keys) == [b"1", b"2"]
        await asyncio.sleep(0.01)
        # validate keepttl: the TTL keeps counting down instead of being cleared
        assert await r.msetex(mapping={"1": 11}, keepttl=True) == 1
        assert 0 < await r.pttl("1") < 10000

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.3.224")
    async def test_msetex_expiration_px(self, r):
        all_test_keys = ["1", "2"]
        for key in all_test_keys:
            await r.delete(key)

        mapping = {"1": 1, "2": "2"}
        # set key/value pairs provided in mapping
        # with expiration - testing px field
        assert await r.msetex(mapping=mapping, px=60000) == 1

        ttls = await asyncio.gather(*[r.ttl(key) for key in mapping.keys()])
        for ttl in ttls:
            assert pytest.approx(ttl) == 60
        assert await r.mget(*mapping.keys()) == [b"1", b"2"]

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.3.224")
    async def test_msetex_expiration_pxat_and_nx(self, r):
        all_test_keys = ["1", "2", "3", "new", "new_2"]
        for key in all_test_keys:
            await r.delete(key)

        mapping = {"1": 1, "2": "2", "3": "three"}
        assert await r.msetex(mapping=mapping, ex=30) == 1

        # NX is set with existing keys - nothing should be saved or updated
        expire_at = await redis_server_time(r) + datetime.timedelta(seconds=10)
        assert (
            await r.msetex(
                mapping={"1": "new_value", "new": "ok"},
                pxat=expire_at,
                data_persist_option=DataPersistOptions.NX,
            )
            == 0
        )
        ttls = await asyncio.gather(*[r.ttl(key) for key in mapping.keys()])
        for ttl in ttls:
            assert 10 < ttl <= 30
        assert await r.mget(*mapping.keys(), "new") == [b"1", b"2", b"three", None]

        # NX is set with non existing keys - values should be set
        assert (
            await r.msetex(
                mapping={"new": "ok", "new_2": "ok_2"},
                pxat=expire_at,
                data_persist_option=DataPersistOptions.NX,
            )
            == 1
        )
        old_ttls = await asyncio.gather(*[r.ttl(key) for key in mapping.keys()])
        new_ttls = await asyncio.gather(*(r.ttl(key) for key in ["new", "new_2"]))
        for ttl in old_ttls:
            assert 10 < ttl <= 30
        for ttl in new_ttls:
            assert ttl <= 11
        assert await r.mget(*mapping.keys(), "new", "new_2") == [
            b"1",
            b"2",
            b"three",
            b"ok",
            b"ok_2",
        ]

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.3.224")
    async def test_msetex_expiration_exat_and_xx(self, r):
        all_test_keys = ["1", "2", "3", "new"]
        for key in all_test_keys:
            await r.delete(key)

        mapping = {"1": 1, "2": "2", "3": "three"}
        assert await r.msetex(mapping, ex=30) == 1

        expire_at = await redis_server_time(r) + datetime.timedelta(seconds=10)
        ## XX is set with unexisting key - nothing should be saved or updated
        assert (
            await r.msetex(
                mapping={"1": "new_value", "new": "ok"},
                exat=expire_at,
                data_persist_option=DataPersistOptions.XX,
            )
            == 0
        )
        ttls = await asyncio.gather(*[r.ttl(key) for key in mapping.keys()])
        for ttl in ttls:
            assert 10 < ttl <= 30
        assert await r.mget(*mapping.keys(), "new") == [b"1", b"2", b"three", None]

        # XX is set with existing keys - values should be updated
        assert (
            await r.msetex(
                mapping={"1": "new_value", "2": "new_value_2"},
                exat=expire_at,
                data_persist_option=DataPersistOptions.XX,
            )
            == 1
        )
        ttls = await asyncio.gather(*[r.ttl(key) for key in mapping.keys()])
        assert ttls[0] <= 11
        assert ttls[1] <= 11
        assert 10 < ttls[2] <= 30
        assert await r.mget("1", "2", "3", "new") == [
            b"new_value",
            b"new_value_2",
            b"three",
            None,
        ]

    @skip_if_server_version_lt("8.3.224")
    async def test_msetex_invalid_inputs(self, r):
        mapping = {"1": 1, "2": "2"}
        with pytest.raises(exceptions.DataError):
            await r.msetex(mapping, ex=10, keepttl=True)

    async def test_msetex_zero_expiry_options_are_mutually_exclusive(
        self, r: redis.Redis
    ):
        with pytest.raises(DataError):
            await r.msetex({"a": 1}, ex=0, px=1)

    @pytest.mark.onlynoncluster
    async def test_msetnx(self, r: redis.Redis):
        d = {"a": b"1", "b": b"2", "c": b"3"}
        assert await r.msetnx(d)
        d2 = {"a": b"x", "d": b"4"}
        assert not await r.msetnx(d2)
        for k, v in d.items():
            assert await r.get(k) == v
        assert await r.get("d") is None

    @skip_if_server_version_lt("2.6.0")
    async def test_pexpire(self, r: redis.Redis):
        assert not await r.pexpire("a", 60000)
        await r.set("a", "foo")
        assert await r.pexpire("a", 60000)
        assert 0 < await r.pttl("a") <= 60000
        assert await r.persist("a")
        assert await r.pttl("a") == -1

    @skip_if_server_version_lt("2.6.0")
    async def test_pexpireat_datetime(self, r: redis.Redis):
        expire_at = await redis_server_time(r) + datetime.timedelta(minutes=1)
        await r.set("a", "foo")
        assert await r.pexpireat("a", expire_at)
        assert 0 < await r.pttl("a") <= 61000

    @skip_if_server_version_lt("2.6.0")
    async def test_pexpireat_no_key(self, r: redis.Redis):
        expire_at = await redis_server_time(r) + datetime.timedelta(minutes=1)
        assert not await r.pexpireat("a", expire_at)

    @skip_if_server_version_lt("2.6.0")
    async def test_pexpireat_unixtime(self, r: redis.Redis):
        expire_at = await redis_server_time(r) + datetime.timedelta(minutes=1)
        await r.set("a", "foo")
        expire_at_milliseconds = int(expire_at.timestamp() * 1000)
        assert await r.pexpireat("a", expire_at_milliseconds)
        assert 0 < await r.pttl("a") <= 61000

    @skip_if_server_version_lt("2.6.0")
    async def test_psetex(self, r: redis.Redis):
        assert await r.psetex("a", 1000, "value")
        assert await r.get("a") == b"value"
        assert 0 < await r.pttl("a") <= 1000

    @skip_if_server_version_lt("2.6.0")
    async def test_psetex_timedelta(self, r: redis.Redis):
        expire_at = datetime.timedelta(milliseconds=1000)
        assert await r.psetex("a", expire_at, "value")
        assert await r.get("a") == b"value"
        assert 0 < await r.pttl("a") <= 1000

    @skip_if_server_version_lt("2.6.0")
    async def test_pttl(self, r: redis.Redis):
        assert not await r.pexpire("a", 10000)
        await r.set("a", "1")
        assert await r.pexpire("a", 10000)
        assert 0 < await r.pttl("a") <= 10000
        assert await r.persist("a")
        assert await r.pttl("a") == -1

    @skip_if_server_version_lt("2.8.0")
    async def test_pttl_no_key(self, r: redis.Redis):
        """PTTL on servers 2.8 and after return -2 when the key doesn't exist"""
        assert await r.pttl("a") == -2

    @skip_if_server_version_lt("6.2.0")
    async def test_hrandfield(self, r):
        assert await r.hrandfield("key") is None
        await r.hset("key", mapping={"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})
        assert await r.hrandfield("key") is not None
        assert len(await r.hrandfield("key", 2)) == 2
        # with values
        assert_resp_response(r, len(await r.hrandfield("key", 2, True)), 4, 2)
        # without duplications
        assert len(await r.hrandfield("key", 10)) == 5
        # with duplications
        assert len(await r.hrandfield("key", -10)) == 10

    @pytest.mark.onlynoncluster
    async def test_randomkey(self, r: redis.Redis):
        assert await r.randomkey() is None
        for key in ("a", "b", "c"):
            await r.set(key, 1)
        assert await r.randomkey() in (b"a", b"b", b"c")

    @pytest.mark.onlynoncluster
    async def test_rename(self, r: redis.Redis):
        await r.set("a", "1")
        assert await r.rename("a", "b")
        assert await r.get("a") is None
        assert await r.get("b") == b"1"

    @pytest.mark.onlynoncluster
    async def test_renamenx(self, r: redis.Redis):
        await r.set("a", "1")
        await r.set("b", "2")
        assert not await r.renamenx("a", "b")
        assert await r.get("a") == b"1"
        assert await r.get("b") == b"2"

    @skip_if_server_version_lt("2.6.0")
    async def test_set_nx(self, r: redis.Redis):
        assert await r.set("a", "1", nx=True)
        assert not await r.set("a", "2", nx=True)
        assert await r.get("a") == b"1"

    @skip_if_server_version_lt("2.6.0")
    async def test_set_xx(self, r: redis.Redis):
        assert not await r.set("a", "1", xx=True)
        assert await r.get("a") is None
        await r.set("a", "bar")
        assert await r.set("a", "2", xx=True)
        assert await r.get("a") == b"2"

    @skip_if_server_version_lt("2.6.0")
    async def test_set_px(self, r: redis.Redis):
        assert await r.set("a", "1", px=10000)
        assert await r.get("a") == b"1"
        assert 0 < await r.pttl("a") <= 10000
        assert 0 < await r.ttl("a") <= 10

    @skip_if_server_version_lt("2.6.0")
    async def test_set_px_timedelta(self, r: redis.Redis):
        expire_at = datetime.timedelta(milliseconds=1000)
        assert await r.set("a", "1", px=expire_at)
        assert 0 < await r.pttl("a") <= 1000
        assert 0 < await r.ttl("a") <= 1

    @skip_if_server_version_lt("2.6.0")
    async def test_set_ex(self, r: redis.Redis):
        assert await r.set("a", "1", ex=10)
        assert 0 < await r.ttl("a") <= 10

    async def test_set_zero_expiry_options_are_mutually_exclusive(self, r: redis.Redis):
        with pytest.raises(DataError):
            await r.set("a", "1", ex=0, px=1)

    @skip_if_server_version_lt("2.6.0")
    async def test_set_ex_timedelta(self, r: redis.Redis):
        expire_at = datetime.timedelta(seconds=60)
        assert await r.set("a", "1", ex=expire_at)
        assert 0 < await r.ttl("a") <= 60

    @skip_if_server_version_lt("2.6.0")
    async def test_set_multipleoptions(self, r: redis.Redis):
        await r.set("a", "val")
        assert await r.set("a", "1", xx=True, px=10000)
        assert 0 < await r.ttl("a") <= 10

    @skip_if_server_version_lt(REDIS_6_VERSION)
    async def test_set_keepttl(self, r: redis.Redis):
        await r.set("a", "val")
        assert await r.set("a", "1", xx=True, px=10000)
        assert 0 < await r.ttl("a") <= 10
        await r.set("a", "2", keepttl=True)
        assert await r.get("a") == b"2"
        assert 0 < await r.ttl("a") <= 10

    async def test_set_empty_condition_is_mutually_exclusive(self, r: redis.Redis):
        with pytest.raises(DataError):
            await r.set("a", "1", nx=True, ifeq=b"")

    @skip_if_server_version_lt("8.3.224")
    async def test_set_ifeq_true_sets_and_returns_true(self, r):
        await r.delete("k")
        await r.set("k", b"foo")
        assert await r.set("k", b"bar", ifeq=b"foo") is True
        assert await r.get("k") == b"bar"

    @skip_if_server_version_lt("8.3.224")
    async def test_set_ifeq_false_does_not_set_returns_none(self, r):
        await r.delete("k")
        await r.set("k", b"foo")
        assert await r.set("k", b"bar", ifeq=b"nope") is None
        assert await r.get("k") == b"foo"

    @skip_if_server_version_lt("8.3.224")
    async def test_set_ifne_true_sets(self, r):
        await r.delete("k")
        await r.set("k", b"foo")
        assert await r.set("k", b"bar", ifne=b"zzz") is True
        assert await r.get("k") == b"bar"

    @skip_if_server_version_lt("8.3.224")
    async def test_set_ifne_false_does_not_set(self, r):
        await r.delete("k")
        await r.set("k", b"foo")
        assert await r.set("k", b"bar", ifne=b"foo") is None
        assert await r.get("k") == b"foo"

    @skip_if_server_version_lt("8.3.224")
    async def test_set_ifeq_when_key_missing_does_not_create(self, r):
        await r.delete("k")
        assert await r.set("k", b"bar", ifeq=b"foo") is None
        assert await r.exists("k") == 0

    @skip_if_server_version_lt("8.3.224")
    async def test_set_ifne_when_key_missing_creates(self, r):
        await r.delete("k")
        assert await r.set("k", b"bar", ifne=b"foo") is True
        assert await r.get("k") == b"bar"

    @skip_if_server_version_lt("8.3.224")
    @pytest.mark.parametrize("val", [b"", b"abc", b"The quick brown fox"])
    async def test_set_ifdeq_and_ifdne(self, r, val):
        await r.delete("k")
        await r.set("k", val)
        d = await self._server_xxh3_digest(r, "k")
        assert d is not None

        # IFDEQ must match to set; if key missing => won't create
        assert await r.set("k", b"X", ifdeq=d) is True
        assert await r.get("k") == b"X"

        await r.delete("k")
        # key missing + IFDEQ => not created
        assert await r.set("k", b"Y", ifdeq=d) is None
        assert await r.exists("k") == 0

        # IFDNE: create when missing, and set when digest differs
        assert await r.set("k", b"bar", ifdne=d) is True
        prev_d = await self._server_xxh3_digest(r, "k")
        assert prev_d is not None
        # If digest equal → do not set
        assert await r.set("k", b"zzz", ifdne=prev_d) is None
        assert await r.get("k") == b"bar"

    @skip_if_server_version_lt("8.3.224")
    async def test_set_with_get_returns_previous_value(self, r):
        await r.delete("k")
        # when key didn’t exist → returns None, and key is created if condition allows it
        prev = await r.set("k", b"v1", get=True, ifne=b"any")  # IFNE on missing creates
        assert prev is None
        # subsequent GET returns previous value, regardless of whether set occurs
        prev2 = await r.set(
            "k", b"v2", get=True, ifeq=b"v1"
        )  # matches → set; returns "v1"
        assert prev2 == b"v1"
        prev3 = await r.set(
            "k", b"v3", get=True, ifeq=b"no"
        )  # no set; returns previous "v2"
        assert prev3 == b"v2"
        assert await r.get("k") == b"v2"

    @skip_if_server_version_lt("8.3.224")
    async def test_set_mutual_exclusion_client_side(self, r):
        await r.delete("k")
        with pytest.raises(DataError):
            await r.set("k", b"v", nx=True, ifeq=b"x")
        with pytest.raises(DataError):
            await r.set("k", b"v", ifdeq="aa", ifdne="bb")
        with pytest.raises(DataError):
            await r.set("k", b"v", ex=1, px=1)
        with pytest.raises(DataError):
            await r.set("k", b"v", exat=1, pxat=1)
        with pytest.raises(DataError):
            await r.set("k", b"v", ex=1, exat=1)

    async def test_setex(self, r: redis.Redis):
        assert await r.setex("a", 60, "1")
        assert await r.get("a") == b"1"
        assert 0 < await r.ttl("a") <= 60

    async def test_setnx(self, r: redis.Redis):
        assert await r.setnx("a", "1")
        assert await r.get("a") == b"1"
        assert not await r.setnx("a", "2")
        assert await r.get("a") == b"1"

    async def test_setrange(self, r: redis.Redis):
        assert await r.setrange("a", 5, "foo") == 8
        assert await r.get("a") == b"\0\0\0\0\0foo"
        await r.set("a", "abcdefghijh")
        assert await r.setrange("a", 6, "12345") == 11
        assert await r.get("a") == b"abcdef12345"

    async def test_strlen(self, r: redis.Redis):
        await r.set("a", "foo")
        assert await r.strlen("a") == 3

    async def test_substr(self, r: redis.Redis):
        await r.set("a", "0123456789")
        assert await r.substr("a", 0) == b"0123456789"
        assert await r.substr("a", 2) == b"23456789"
        assert await r.substr("a", 3, 5) == b"345"
        assert await r.substr("a", 3, -2) == b"345678"

    async def test_ttl(self, r: redis.Redis):
        await r.set("a", "1")
        assert await r.expire("a", 10)
        assert 0 < await r.ttl("a") <= 10
        assert await r.persist("a")
        assert await r.ttl("a") == -1

    @skip_if_server_version_lt("2.8.0")
    async def test_ttl_nokey(self, r: redis.Redis):
        """TTL on servers 2.8 and after return -2 when the key doesn't exist"""
        assert await r.ttl("a") == -2

    async def test_type(self, r: redis.Redis):
        assert await r.type("a") == b"none"
        await r.set("a", "1")
        assert await r.type("a") == b"string"
        await r.delete("a")
        await r.lpush("a", "1")
        assert await r.type("a") == b"list"
        await r.delete("a")
        await r.sadd("a", "1")
        assert await r.type("a") == b"set"
        await r.delete("a")
        await r.zadd("a", {"1": 1})
        assert await r.type("a") == b"zset"

    # LIST COMMANDS
    @pytest.mark.onlynoncluster
    async def test_blpop(self, r: redis.Redis):
        await r.rpush("a", "1", "2")
        await r.rpush("b", "3", "4")
        assert_resp_response(
            r, await r.blpop(["b", "a"], timeout=1), (b"b", b"3"), [b"b", b"3"]
        )
        assert_resp_response(
            r, await r.blpop(["b", "a"], timeout=1), (b"b", b"4"), [b"b", b"4"]
        )
        assert_resp_response(
            r, await r.blpop(["b", "a"], timeout=1), (b"a", b"1"), [b"a", b"1"]
        )
        assert_resp_response(
            r, await r.blpop(["b", "a"], timeout=1), (b"a", b"2"), [b"a", b"2"]
        )
        assert await r.blpop(["b", "a"], timeout=0.1) is None
        await r.rpush("c", "1")
        assert_resp_response(
            r, await r.blpop("c", timeout=1), (b"c", b"1"), [b"c", b"1"]
        )

    @pytest.mark.onlynoncluster
    async def test_brpop(self, r: redis.Redis):
        await r.rpush("a", "1", "2")
        await r.rpush("b", "3", "4")
        assert_resp_response(
            r, await r.brpop(["b", "a"], timeout=1), (b"b", b"4"), [b"b", b"4"]
        )
        assert_resp_response(
            r, await r.brpop(["b", "a"], timeout=1), (b"b", b"3"), [b"b", b"3"]
        )
        assert_resp_response(
            r, await r.brpop(["b", "a"], timeout=1), (b"a", b"2"), [b"a", b"2"]
        )
        assert_resp_response(
            r, await r.brpop(["b", "a"], timeout=1), (b"a", b"1"), [b"a", b"1"]
        )
        assert await r.brpop(["b", "a"], timeout=0.1) is None
        await r.rpush("c", "1")
        assert_resp_response(
            r, await r.brpop("c", timeout=1), (b"c", b"1"), [b"c", b"1"]
        )

    @pytest.mark.onlynoncluster
    async def test_brpoplpush(self, r: redis.Redis):
        await r.rpush("a", "1", "2")
        await r.rpush("b", "3", "4")
        assert await r.brpoplpush("a", "b") == b"2"
        assert await r.brpoplpush("a", "b") == b"1"
        assert await r.brpoplpush("a", "b", timeout=0.1) is None
        assert await r.lrange("a", 0, -1) == []
        assert await r.lrange("b", 0, -1) == [b"1", b"2", b"3", b"4"]

    @pytest.mark.onlynoncluster
    async def test_brpoplpush_empty_string(self, r: redis.Redis):
        await r.rpush("a", "")
        assert await r.brpoplpush("a", "b") == b""

    async def test_lindex(self, r: redis.Redis):
        await r.rpush("a", "1", "2", "3")
        assert await r.lindex("a", "0") == b"1"
        assert await r.lindex("a", "1") == b"2"
        assert await r.lindex("a", "2") == b"3"

    async def test_linsert(self, r: redis.Redis):
        await r.rpush("a", "1", "2", "3")
        assert await r.linsert("a", "after", "2", "2.5") == 4
        assert await r.lrange("a", 0, -1) == [b"1", b"2", b"2.5", b"3"]
        assert await r.linsert("a", "before", "2", "1.5") == 5
        assert await r.lrange("a", 0, -1) == [b"1", b"1.5", b"2", b"2.5", b"3"]

    async def test_llen(self, r: redis.Redis):
        await r.rpush("a", "1", "2", "3")
        assert await r.llen("a") == 3

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.9.0")
    async def test_lmovem(self, r: redis.Redis):
        await r.rpush("a", "1", "2", "3", "4", "5")
        # single element (no count block), still an array reply
        assert await r.lmovem("a", "b") == [b"1"]
        # COUNT with OBO ordering: pushed one-by-one -> reversed block order
        assert await r.lmovem("a", "b", "LEFT", "LEFT", count=3, ordering="OBO") == [
            b"4",
            b"3",
            b"2",
        ]
        # up to count, fewer available; BULK preserves relative order
        assert await r.lmovem("a", "b", "LEFT", "LEFT", count=5, ordering="BULK") == [
            b"5"
        ]
        # empty source moves nothing
        assert await r.lmovem("a", "b", count=2, ordering="BULK") is None
        # EXACTLY with too few elements moves nothing (nil reply, source untouched)
        await r.rpush("names", "john")
        assert (
            await r.lmovem(
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
        assert await r.lrange("names", 0, -1) == [b"john"]
        # EXACTLY with enough elements, BULK preserves order
        await r.rpush("names", "doe")
        assert await r.lmovem(
            "names",
            "processed",
            "LEFT",
            "RIGHT",
            count=2,
            mode="EXACTLY",
            ordering="BULK",
        ) == [b"john", b"doe"]
        # ordering is mandatory whenever count is given (and vice versa)
        with pytest.raises(DataError):
            await r.lmovem("a", "b", count=2)
        with pytest.raises(DataError):
            await r.lmovem("a", "b", ordering="BULK")

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.9.0")
    async def test_blmovem(self, r: redis.Redis):
        await r.rpush("a", "1", "2", "3", "4", "5")
        assert await r.blmovem("a", "b", 1) == [b"1"]
        assert await r.blmovem(
            "a", "b", 1, "LEFT", "LEFT", count=3, ordering="BULK"
        ) == [b"2", b"3", b"4"]
        # up to count, fewer available
        assert await r.blmovem("a", "b", 1, count=5, ordering="BULK") == [b"5"]
        # timeout with empty source returns None
        assert await r.blmovem("foo", "bar", 1, count=2, ordering="BULK") is None
        # ordering is mandatory whenever count is given
        with pytest.raises(DataError):
            await r.blmovem("a", "b", 1, count=2)

    async def test_lpop(self, r: redis.Redis):
        await r.rpush("a", "1", "2", "3")
        assert await r.lpop("a") == b"1"
        assert await r.lpop("a") == b"2"
        assert await r.lpop("a") == b"3"
        assert await r.lpop("a") is None

    async def test_lpush(self, r: redis.Redis):
        assert await r.lpush("a", "1") == 1
        assert await r.lpush("a", "2") == 2
        assert await r.lpush("a", "3", "4") == 4
        assert await r.lrange("a", 0, -1) == [b"4", b"3", b"2", b"1"]

    async def test_lpushx(self, r: redis.Redis):
        assert await r.lpushx("a", "1") == 0
        assert await r.lrange("a", 0, -1) == []
        await r.rpush("a", "1", "2", "3")
        assert await r.lpushx("a", "4") == 4
        assert await r.lrange("a", 0, -1) == [b"4", b"1", b"2", b"3"]

    async def test_lrange(self, r: redis.Redis):
        await r.rpush("a", "1", "2", "3", "4", "5")
        assert await r.lrange("a", 0, 2) == [b"1", b"2", b"3"]
        assert await r.lrange("a", 2, 10) == [b"3", b"4", b"5"]
        assert await r.lrange("a", 0, -1) == [b"1", b"2", b"3", b"4", b"5"]

    async def test_lrem(self, r: redis.Redis):
        await r.rpush("a", "Z", "b", "Z", "Z", "c", "Z", "Z")
        # remove the first 'Z'  item
        assert await r.lrem("a", 1, "Z") == 1
        assert await r.lrange("a", 0, -1) == [b"b", b"Z", b"Z", b"c", b"Z", b"Z"]
        # remove the last 2 'Z' items
        assert await r.lrem("a", -2, "Z") == 2
        assert await r.lrange("a", 0, -1) == [b"b", b"Z", b"Z", b"c"]
        # remove all 'Z' items
        assert await r.lrem("a", 0, "Z") == 2
        assert await r.lrange("a", 0, -1) == [b"b", b"c"]

    async def test_lset(self, r: redis.Redis):
        await r.rpush("a", "1", "2", "3")
        assert await r.lrange("a", 0, -1) == [b"1", b"2", b"3"]
        assert await r.lset("a", 1, "4")
        assert await r.lrange("a", 0, 2) == [b"1", b"4", b"3"]

    @skip_if_server_version_lt("8.7.2")
    async def test_arset_at_index_zero(self, r: redis.Redis):
        assert await r.arset("a", 0, "v0") == 1

    @skip_if_server_version_lt("8.7.2")
    async def test_arset_at_positive_index(self, r: redis.Redis):
        assert await r.arset("a", 1, "v1") == 1

    @skip_if_server_version_lt("8.7.2")
    async def test_arset_multiple_values(self, r: redis.Redis):
        assert await r.arset("a", 0, "v0", "v1", "v2") == 3

    @skip_if_server_version_lt("8.7.2")
    async def test_arset_negative_index(self, r: redis.Redis):
        await r.arset("a", 0, "v0", "v1", "v2")
        with pytest.raises(redis.ResponseError):
            await r.arset("a", -1, "vlast")

    @skip_if_server_version_lt("8.7.2")
    async def test_armset_sets_values_at_correct_indices(self, r: redis.Redis):
        assert await r.armset("a", {0: "v0", 2: "v2", 4: "v4"}) == 3
        assert await r.arget("a", 0) == b"v0"
        assert await r.arget("a", 1) is None
        assert await r.arget("a", 2) == b"v2"
        assert await r.arget("a", 3) is None
        assert await r.arget("a", 4) == b"v4"

    @skip_if_server_version_lt("8.7.2")
    async def test_armset_single_pair(self, r: redis.Redis):
        assert await r.armset("a", {3: "v3"}) == 1
        assert await r.arget("a", 3) == b"v3"

    @skip_if_server_version_lt("8.7.2")
    async def test_armset_overwrites_existing_indices(self, r: redis.Redis):
        await r.arset("a", 0, "v0", "v1", "v2")
        assert await r.armset("a", {0: "new0", 1: "new1", 2: "new2"}) == 0
        assert await r.arget("a", 0) == b"new0"
        assert await r.arget("a", 1) == b"new1"
        assert await r.arget("a", 2) == b"new2"

    @skip_if_server_version_lt("8.7.2")
    async def test_armset_mixed_new_and_existing_indices(self, r: redis.Redis):
        await r.arset("a", 0, "v0", "v1")
        assert await r.armset("a", {0: "new0", 2: "v2", 3: "v3"}) == 2
        assert await r.arget("a", 0) == b"new0"
        assert await r.arget("a", 1) == b"v1"
        assert await r.arget("a", 2) == b"v2"
        assert await r.arget("a", 3) == b"v3"

    @skip_if_server_version_lt("8.7.2")
    async def test_arget_returns_value_at_index(self, r: redis.Redis):
        await r.arset("a", 0, "v0", "v1", "v2")
        assert await r.arget("a", 0) == b"v0"
        assert await r.arget("a", 1) == b"v1"
        assert await r.arget("a", 2) == b"v2"

    @skip_if_server_version_lt("8.7.2")
    async def test_arget_missing_key_returns_none(self, r: redis.Redis):
        assert await r.arget("a", 0) is None

    @skip_if_server_version_lt("8.7.2")
    async def test_arget_missing_index_returns_none(self, r: redis.Redis):
        await r.arset("a", 0, "v0")
        assert await r.arget("a", 5) is None

    @skip_if_server_version_lt("8.7.2")
    async def test_armget_returns_values_at_indices(self, r: redis.Redis):
        await r.arset("a", 0, "v0", "v1", "v2")
        assert await r.armget("a", 0, 1, 2) == [b"v0", b"v1", b"v2"]

    @skip_if_server_version_lt("8.7.2")
    async def test_armget_preserves_requested_order(self, r: redis.Redis):
        await r.arset("a", 0, "v0", "v1", "v2")
        assert await r.armget("a", 2, 0, 1) == [b"v2", b"v0", b"v1"]

    @skip_if_server_version_lt("8.7.2")
    async def test_armget_missing_key_returns_nones(self, r: redis.Redis):
        assert await r.armget("a", 0, 2, 5) == [None, None, None]

    @skip_if_server_version_lt("8.7.2")
    async def test_armget_missing_indices_return_nones(self, r: redis.Redis):
        await r.arset("a", 0, "v0", "v1")
        assert await r.armget("a", 0, 5, 1, 10) == [b"v0", None, b"v1", None]

    @skip_if_server_version_lt("8.7.2")
    async def test_armget_single_index(self, r: redis.Redis):
        await r.arset("a", 0, "v0", "v1")
        assert await r.armget("a", 1) == [b"v1"]

    @skip_if_server_version_lt("8.7.2")
    async def test_argetrange_returns_values_in_range(self, r: redis.Redis):
        await r.arset("a", 0, "v0", "v1", "v2")
        assert await r.argetrange("a", 0, 2) == [b"v0", b"v1", b"v2"]

    @skip_if_server_version_lt("8.7.2")
    async def test_arinsert_single_value(self, r: redis.Redis):
        assert await r.arinsert("a", "v0") == 0
        assert await r.armget("a", 0) == [b"v0"]

    @skip_if_server_version_lt("8.7.2")
    async def test_arinsert_multiple_values(self, r: redis.Redis):
        assert await r.arinsert("a", "v0", "v1", "v2") == 2
        assert await r.armget("a", 0, 1, 2) == [b"v0", b"v1", b"v2"]

    @skip_if_server_version_lt("8.7.2")
    async def test_arinsert_cursor_advancement(self, r: redis.Redis):
        assert await r.arinsert("a", "v0", "v1") == 1
        assert await r.arnext("a") == 2
        assert await r.arinsert("a", "v2") == 2
        assert await r.armget("a", 0, 1, 2) == [b"v0", b"v1", b"v2"]

    @skip_if_server_version_lt("8.7.2")
    async def test_arscan_returns_index_value_pairs(self, r: redis.Redis):
        await r.arset("a", 0, "v0", "v1", "v2")
        assert await r.arscan("a", 0, 2) == [[0, b"v0"], [1, b"v1"], [2, b"v2"]]

    @skip_if_server_version_lt("8.7.2")
    async def test_arscan_skips_empty_slots(self, r: redis.Redis):
        await r.armset("a", {0: "a", 5: "b", 9: "c"})
        assert await r.arscan("a", 0, 10) == [[0, b"a"], [5, b"b"], [9, b"c"]]

    @skip_if_server_version_lt("8.7.2")
    async def test_arscan_reverse(self, r: redis.Redis):
        await r.armset("a", {0: "a", 5: "b", 9: "c"})
        assert await r.arscan("a", 10, 0) == [[9, b"c"], [5, b"b"], [0, b"a"]]

    @skip_if_server_version_lt("8.7.2")
    async def test_arscan_with_limit(self, r: redis.Redis):
        await r.armset("a", {0: "a", 5: "b", 9: "c"})
        assert await r.arscan("a", 0, 10, limit=2) == [[0, b"a"], [5, b"b"]]

    @skip_if_server_version_lt("8.7.2")
    async def test_arscan_missing_key_returns_empty(self, r: redis.Redis):
        assert await r.arscan("a", 0, 10) == []

    @skip_if_server_version_lt("8.7.2")
    async def test_argrep_exact_match(self, r: redis.Redis):
        await r.armset("a", {0: "boot", 1: "warn", 2: "error", 3: "boot"})
        assert await r.argrep("a", 0, 3, [(ArrayPredicateType.EXACT, "boot")]) == [0, 3]

    @skip_if_server_version_lt("8.7.2")
    async def test_argrep_substring_match(self, r: redis.Redis):
        await r.armset("a", {0: "boot: ok", 1: "warn: disk", 2: "ERROR: cpu"})
        assert await r.argrep("a", 0, 2, [(ArrayPredicateType.MATCH, "warn")]) == [1]

    @skip_if_server_version_lt("8.7.2")
    async def test_argrep_glob_match(self, r: redis.Redis):
        await r.armset("a", {0: "warn:disk", 1: "info:ok", 2: "warn:net"})
        result = await r.argrep("a", 0, 2, [(ArrayPredicateType.GLOB, "warn:*")])
        assert result == [0, 2]

    @skip_if_server_version_lt("8.7.2")
    async def test_argrep_regex_match(self, r: redis.Redis):
        await r.armset("a", {0: "boot: ok", 1: "ERROR: cpu", 2: "error: net"})
        result = await r.argrep(
            "a",
            0,
            2,
            [(ArrayPredicateType.RE, r"^[A-Za-z]+: (cpu|net)$")],
            nocase=True,
        )
        assert result == [1, 2]

    @skip_if_server_version_lt("8.7.2")
    async def test_argrep_nocase(self, r: redis.Redis):
        await r.armset("a", {0: "ERROR", 1: "info", 2: "Error"})
        assert await r.argrep(
            "a", 0, 2, [(ArrayPredicateType.MATCH, "error")], nocase=True
        ) == [0, 2]

    @skip_if_server_version_lt("8.7.2")
    async def test_argrep_withvalues(self, r: redis.Redis):
        await r.armset("a", {0: "ERROR: cpu", 1: "info", 2: "error: net"})
        result = await r.argrep(
            "a",
            0,
            2,
            [(ArrayPredicateType.MATCH, "error")],
            withvalues=True,
            nocase=True,
        )
        assert result == [[0, b"ERROR: cpu"], [2, b"error: net"]]

    @skip_if_server_version_lt("8.7.2")
    async def test_argrep_limit(self, r: redis.Redis):
        await r.armset("a", {0: "ERROR", 1: "info", 2: "Error"})
        result = await r.argrep(
            "a",
            0,
            2,
            [(ArrayPredicateType.MATCH, "error")],
            limit=1,
            nocase=True,
        )
        assert result == [0]

    @skip_if_server_version_lt("8.7.2")
    async def test_argrep_combinator_and(self, r: redis.Redis):
        await r.armset("a", {0: "warn: ok", 1: "error: ok", 2: "warn: bad"})
        result = await r.argrep(
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
    async def test_argrep_combinator_or(self, r: redis.Redis):
        await r.armset("a", {0: "warn", 1: "info", 2: "error"})
        result = await r.argrep(
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
    async def test_argrep_missing_key(self, r: redis.Redis):
        assert await r.argrep("a", 0, 10, [(ArrayPredicateType.MATCH, "x")]) == []

    @skip_if_server_version_lt("8.7.2")
    async def test_argrep_reverse(self, r: redis.Redis):
        await r.armset("a", {0: "warn", 1: "info", 2: "warn"})
        assert await r.argrep("a", 2, 0, [(ArrayPredicateType.EXACT, "warn")]) == [2, 0]

    @skip_if_server_version_lt("8.7.2")
    async def test_ardel_single_index(self, r: redis.Redis):
        await r.arset("a", 0, "v0", "v1", "v2")
        assert await r.ardel("a", 1) == 1
        assert await r.arget("a", 1) is None
        assert await r.arget("a", 0) == b"v0"
        assert await r.arget("a", 2) == b"v2"

    @skip_if_server_version_lt("8.7.2")
    async def test_ardel_multiple_indices(self, r: redis.Redis):
        await r.arset("a", 0, "v0", "v1", "v2", "v3")
        assert await r.ardel("a", 0, 2, 3) == 3
        assert await r.arget("a", 0) is None
        assert await r.arget("a", 1) == b"v1"
        assert await r.arget("a", 2) is None
        assert await r.arget("a", 3) is None

    @skip_if_server_version_lt("8.7.2")
    async def test_ardel_non_existing_index(self, r: redis.Redis):
        await r.arset("a", 0, "v0", "v1")
        assert await r.ardel("a", 10) == 0
        assert await r.arget("a", 0) == b"v0"
        assert await r.arget("a", 1) == b"v1"

    @skip_if_server_version_lt("8.7.2")
    async def test_ardel_mixed_existing_and_missing(self, r: redis.Redis):
        await r.arset("a", 0, "v0", "v1", "v2")
        assert await r.ardel("a", 0, 10, 2) == 2
        assert await r.arget("a", 0) is None
        assert await r.arget("a", 1) == b"v1"
        assert await r.arget("a", 2) is None

    @skip_if_server_version_lt("8.7.2")
    async def test_ardelrange_single_range(self, r: redis.Redis):
        await r.arset("a", 0, "v0", "v1", "v2", "v3", "v4")
        assert await r.ardelrange("a", (1, 3)) == 3
        assert await r.arget("a", 0) == b"v0"
        assert await r.arget("a", 1) is None
        assert await r.arget("a", 2) is None
        assert await r.arget("a", 3) is None
        assert await r.arget("a", 4) == b"v4"

    @skip_if_server_version_lt("8.7.2")
    async def test_ardelrange_multiple_ranges(self, r: redis.Redis):
        await r.arset("a", 0, "v0", "v1", "v2", "v3", "v4", "v5")
        assert await r.ardelrange("a", (0, 1), (4, 5)) == 4
        assert await r.arget("a", 0) is None
        assert await r.arget("a", 1) is None
        assert await r.arget("a", 2) == b"v2"
        assert await r.arget("a", 3) == b"v3"
        assert await r.arget("a", 4) is None
        assert await r.arget("a", 5) is None

    @skip_if_server_version_lt("8.7.2")
    async def test_ardelrange_reversed_range(self, r: redis.Redis):
        await r.arset("a", 0, "v0", "v1", "v2", "v3", "v4")
        assert await r.ardelrange("a", (3, 1)) == 3
        assert await r.arget("a", 0) == b"v0"
        assert await r.arget("a", 1) is None
        assert await r.arget("a", 2) is None
        assert await r.arget("a", 3) is None
        assert await r.arget("a", 4) == b"v4"

    @skip_if_server_version_lt("8.7.2")
    async def test_ardelrange_overlapping_ranges(self, r: redis.Redis):
        await r.arset("a", 0, "v0", "v1", "v2", "v3", "v4")
        assert await r.ardelrange("a", (0, 2), (1, 3)) == 4
        assert await r.arget("a", 0) is None
        assert await r.arget("a", 1) is None
        assert await r.arget("a", 2) is None
        assert await r.arget("a", 3) is None
        assert await r.arget("a", 4) == b"v4"

    @skip_if_server_version_lt("8.7.2")
    async def test_ardelrange_missing_key(self, r: redis.Redis):
        assert await r.ardelrange("a", (0, 4)) == 0

    @skip_if_server_version_lt("8.7.2")
    async def test_ardelrange_empty_indices(self, r: redis.Redis):
        await r.arset("a", 0, "v0")
        assert await r.ardelrange("a", (5, 10)) == 0
        assert await r.arget("a", 0) == b"v0"

    @skip_if_server_version_lt("8.7.2")
    async def test_arcount_missing_key(self, r: redis.Redis):
        assert await r.arcount("a") == 0

    @skip_if_server_version_lt("8.7.2")
    async def test_arcount_returns_number_of_elements(self, r: redis.Redis):
        await r.arset("a", 0, "v0", "v1", "v2")
        assert await r.arcount("a") == 3

    @skip_if_server_version_lt("8.7.2")
    async def test_arcount_after_delete(self, r: redis.Redis):
        await r.arset("a", 0, "v0", "v1", "v2", "v3")
        await r.ardel("a", 1, 3)
        assert await r.arcount("a") == 2

    @skip_if_server_version_lt("8.7.2")
    async def test_arlen_missing_key(self, r: redis.Redis):
        assert await r.arlen("a") == 0

    @skip_if_server_version_lt("8.7.2")
    async def test_arlen_returns_length(self, r: redis.Redis):
        await r.arset("a", 0, "v0", "v1", "v2")
        assert await r.arlen("a") == 3

    @skip_if_server_version_lt("8.7.2")
    async def test_arlen_uses_max_index_plus_one(self, r: redis.Redis):
        await r.arset("a", 7, "v7")
        assert await r.arlen("a") == 8

    @skip_if_server_version_lt("8.7.2")
    async def test_arlen_unchanged_after_delete(self, r: redis.Redis):
        await r.arset("a", 0, "v0", "v1", "v2")
        await r.ardel("a", 1)
        assert await r.arlen("a") == 3

    @skip_if_server_version_lt("8.7.2")
    async def test_arinfo_returns_basic_fields(self, r: redis.Redis):
        await r.armset("a", {0: "x", 1: "y", 100: "z"})
        info = await r.arinfo("a")
        assert isinstance(info, dict)
        assert info["count"] == 3
        assert info["len"] == 101
        for field in ("next-insert-index", "slices", "slice-size"):
            assert field in info

    @skip_if_server_version_lt("8.7.2")
    async def test_arinfo_full_includes_slice_stats(self, r: redis.Redis):
        await r.armset("a", {0: "x", 1: "y", 100: "z"})
        info = await r.arinfo("a", full=True)
        for field in (
            "dense-slices",
            "sparse-slices",
            "avg-dense-size",
            "avg-dense-fill",
            "avg-sparse-size",
        ):
            assert field in info

    @skip_if_server_version_lt("8.7.2")
    async def test_arinfo_missing_key_raises(self, r: redis.Redis):
        with pytest.raises(redis.ResponseError):
            await r.arinfo("a")

    @skip_if_server_version_lt("8.7.2")
    async def test_arnext_missing_key(self, r: redis.Redis):
        assert await r.arnext("a") == 0

    @skip_if_server_version_lt("8.7.2")
    async def test_arnext_returns_next_index(self, r: redis.Redis):
        await r.arinsert("a", "v0", "v1", "v2")
        assert await r.arnext("a") == 3

    @skip_if_server_version_lt("8.7.2")
    async def test_arnext_after_delete(self, r: redis.Redis):
        await r.arinsert("a", "v0", "v1", "v2")
        await r.ardel("a", 1)
        assert await r.arnext("a") == 3

    @skip_if_server_version_lt("8.7.2")
    async def test_arseek_missing_key_returns_zero(self, r: redis.Redis):
        assert await r.arseek("a", 5) == 0

    @skip_if_server_version_lt("8.7.2")
    async def test_arseek_repositions_insert_cursor(self, r: redis.Redis):
        await r.arinsert("a", "v0")
        assert await r.arseek("a", 10) == 1
        assert await r.arnext("a") == 10
        await r.arinsert("a", "v10")
        assert await r.arget("a", 10) == b"v10"

    @skip_if_server_version_lt("8.7.2")
    async def test_arseek_followed_by_arnext(self, r: redis.Redis):
        await r.arinsert("a", "v0", "v1")
        assert await r.arseek("a", 7) == 1
        assert await r.arnext("a") == 7

    @skip_if_server_version_lt("8.7.2")
    async def test_arring_inserts_sequentially(self, r: redis.Redis):
        assert await r.arring("a", 3, "v0") == 0
        assert await r.arring("a", 3, "v1") == 1
        assert await r.arring("a", 3, "v2") == 2
        assert await r.arget("a", 0) == b"v0"
        assert await r.arget("a", 1) == b"v1"
        assert await r.arget("a", 2) == b"v2"

    @skip_if_server_version_lt("8.7.2")
    async def test_arring_wraps_when_full(self, r: redis.Redis):
        await r.arring("a", 3, "v0", "v1", "v2")
        assert await r.arring("a", 3, "v3") == 0
        assert await r.arget("a", 0) == b"v3"
        assert await r.arget("a", 1) == b"v1"
        assert await r.arget("a", 2) == b"v2"

    @skip_if_server_version_lt("8.7.2")
    async def test_arring_multiple_values_in_one_call(self, r: redis.Redis):
        assert await r.arring("a", 4, "v0", "v1", "v2") == 2
        assert await r.arget("a", 0) == b"v0"
        assert await r.arget("a", 2) == b"v2"

    @skip_if_server_version_lt("8.7.2")
    async def test_arring_returns_last_inserted_index(self, r: redis.Redis):
        assert await r.arring("a", 5, "v0", "v1", "v2", "v3", "v4", "v5") == 0

    @skip_if_server_version_lt("8.7.2")
    async def test_arring_truncates_on_smaller_size(self, r: redis.Redis):
        await r.arring("a", 5, "v0", "v1", "v2", "v3", "v4")
        assert await r.arring("a", 3, "v5") == 0
        assert await r.arlen("a") == 3

    @skip_if_server_version_lt("8.7.2")
    async def test_arlastitems_returns_last_elements(self, r: redis.Redis):
        await r.arinsert("a", "v0", "v1", "v2", "v3", "v4")
        assert await r.arlastitems("a", 3) == [b"v2", b"v3", b"v4"]

    @skip_if_server_version_lt("8.7.2")
    async def test_arlastitems_reverse_order(self, r: redis.Redis):
        await r.arinsert("a", "v0", "v1", "v2", "v3", "v4")
        assert await r.arlastitems("a", 3, rev=True) == [b"v4", b"v3", b"v2"]

    @skip_if_server_version_lt("8.7.2")
    async def test_arlastitems_count_greater_than_size(self, r: redis.Redis):
        await r.arinsert("a", "v0", "v1", "v2")
        assert await r.arlastitems("a", 10) == [b"v0", b"v1", b"v2"]

    @skip_if_server_version_lt("8.7.2")
    async def test_arlastitems_count_greater_than_size_reverse(self, r: redis.Redis):
        await r.arinsert("a", "v0", "v1", "v2")
        assert await r.arlastitems("a", 10, rev=True) == [b"v2", b"v1", b"v0"]

    @skip_if_server_version_lt("8.7.2")
    async def test_arlastitems_missing_key(self, r: redis.Redis):
        assert await r.arlastitems("a", 3) == []

    @skip_if_server_version_lt("8.7.2")
    async def test_arlastitems_zero_count(self, r: redis.Redis):
        await r.arinsert("a", "v0", "v1", "v2")
        assert await r.arlastitems("a", 0) == []

    @skip_if_server_version_lt("8.7.2")
    async def test_arop_sum(self, r: redis.Redis):
        await r.arset("a", 0, 1, 2, 3, 4, 5)
        assert await r.arop("a", 0, 4, ArrayAggregateOperations.SUM) == b"15"

    @skip_if_server_version_lt("8.7.2")
    async def test_arop_min_max(self, r: redis.Redis):
        await r.arset("a", 0, 5, 1, 4, 2, 3)
        assert await r.arop("a", 0, 4, ArrayAggregateOperations.MIN) == b"1"
        assert await r.arop("a", 0, 4, ArrayAggregateOperations.MAX) == b"5"

    @skip_if_server_version_lt("8.7.2")
    async def test_arop_bitwise(self, r: redis.Redis):
        await r.arset("a", 0, 1, 2, 3, 4, 5)
        assert await r.arop("a", 0, 4, ArrayAggregateOperations.AND) == 0
        assert await r.arop("a", 0, 4, ArrayAggregateOperations.OR) == 7
        assert await r.arop("a", 0, 4, ArrayAggregateOperations.XOR) == 1

    @skip_if_server_version_lt("8.7.2")
    async def test_arop_match(self, r: redis.Redis):
        await r.arset("a", 0, 1, 2, 3, 2, 1)
        assert await r.arop("a", 0, 4, ArrayAggregateOperations.MATCH, 2) == 2
        assert await r.arop("a", 0, 4, ArrayAggregateOperations.MATCH, 7) == 0

    @skip_if_server_version_lt("8.7.2")
    async def test_arop_used(self, r: redis.Redis):
        await r.arset("a", 0, "v0", "v1", "v2")
        assert await r.arop("a", 0, 2, ArrayAggregateOperations.USED) == 3
        await r.ardel("a", 1)
        assert await r.arop("a", 0, 2, ArrayAggregateOperations.USED) == 2

    @skip_if_server_version_lt("8.7.2")
    async def test_arop_reversed_range(self, r: redis.Redis):
        await r.arset("a", 0, 1, 2, 3, 4, 5)
        assert await r.arop("a", 4, 0, ArrayAggregateOperations.SUM) == b"15"

    @skip_if_server_version_lt("8.7.2")
    async def test_arop_non_numeric_values(self, r: redis.Redis):
        await r.arset("a", 0, "x", "y", "z")
        assert await r.arop("a", 0, 2, ArrayAggregateOperations.SUM) is None
        assert await r.arop("a", 0, 2, ArrayAggregateOperations.MIN) is None
        assert await r.arop("a", 0, 2, ArrayAggregateOperations.MAX) is None
        assert await r.arop("a", 0, 2, ArrayAggregateOperations.USED) == 3
        assert await r.arop("a", 0, 2, ArrayAggregateOperations.MATCH, "y") == 1

    @skip_if_server_version_lt("8.7.2")
    async def test_arop_mixed_numeric_non_numeric(self, r: redis.Redis):
        await r.arset("a", 0, 1, "x", 3)
        assert await r.arop("a", 0, 2, ArrayAggregateOperations.SUM) == b"4"
        assert await r.arop("a", 0, 2, ArrayAggregateOperations.MIN) == b"1"
        assert await r.arop("a", 0, 2, ArrayAggregateOperations.USED) == 3

    @skip_if_server_version_lt("8.7.2")
    async def test_arop_empty_range(self, r: redis.Redis):
        await r.arset("a", 0, 1, 2, 3)
        assert await r.arop("a", 100, 200, ArrayAggregateOperations.SUM) is None
        assert await r.arop("a", 100, 200, ArrayAggregateOperations.AND) is None
        assert await r.arop("a", 100, 200, ArrayAggregateOperations.OR) is None
        assert await r.arop("a", 100, 200, ArrayAggregateOperations.XOR) is None
        assert await r.arop("a", 100, 200, ArrayAggregateOperations.USED) == 0
        assert await r.arop("a", 100, 200, ArrayAggregateOperations.MATCH, 1) == 0

    @skip_if_server_version_lt("8.7.2")
    async def test_arop_missing_key(self, r: redis.Redis):
        assert await r.arop("a", 0, 4, ArrayAggregateOperations.SUM) is None
        assert await r.arop("a", 0, 4, ArrayAggregateOperations.AND) is None
        assert await r.arop("a", 0, 4, ArrayAggregateOperations.USED) == 0

    @skip_if_server_version_lt("8.7.2")
    async def test_arop_match_requires_value(self, r: redis.Redis):
        await r.arset("a", 0, 1, 2, 3)
        with pytest.raises(redis.ResponseError):
            await r.arop("a", 0, 2, ArrayAggregateOperations.MATCH)

    async def test_ltrim(self, r: redis.Redis):
        await r.rpush("a", "1", "2", "3")
        assert await r.ltrim("a", 0, 1)
        assert await r.lrange("a", 0, -1) == [b"1", b"2"]

    async def test_rpop(self, r: redis.Redis):
        await r.rpush("a", "1", "2", "3")
        assert await r.rpop("a") == b"3"
        assert await r.rpop("a") == b"2"
        assert await r.rpop("a") == b"1"
        assert await r.rpop("a") is None

    @pytest.mark.onlynoncluster
    async def test_rpoplpush(self, r: redis.Redis):
        await r.rpush("a", "a1", "a2", "a3")
        await r.rpush("b", "b1", "b2", "b3")
        assert await r.rpoplpush("a", "b") == b"a3"
        assert await r.lrange("a", 0, -1) == [b"a1", b"a2"]
        assert await r.lrange("b", 0, -1) == [b"a3", b"b1", b"b2", b"b3"]

    async def test_rpush(self, r: redis.Redis):
        assert await r.rpush("a", "1") == 1
        assert await r.rpush("a", "2") == 2
        assert await r.rpush("a", "3", "4") == 4
        assert await r.lrange("a", 0, -1) == [b"1", b"2", b"3", b"4"]

    @skip_if_server_version_lt("6.0.6")
    async def test_lpos(self, r: redis.Redis):
        assert await r.rpush("a", "a", "b", "c", "1", "2", "3", "c", "c") == 8
        assert await r.lpos("a", "a") == 0
        assert await r.lpos("a", "c") == 2

        assert await r.lpos("a", "c", rank=1) == 2
        assert await r.lpos("a", "c", rank=2) == 6
        assert await r.lpos("a", "c", rank=4) is None
        assert await r.lpos("a", "c", rank=-1) == 7
        assert await r.lpos("a", "c", rank=-2) == 6

        assert await r.lpos("a", "c", count=0) == [2, 6, 7]
        assert await r.lpos("a", "c", count=1) == [2]
        assert await r.lpos("a", "c", count=2) == [2, 6]
        assert await r.lpos("a", "c", count=100) == [2, 6, 7]

        assert await r.lpos("a", "c", count=0, rank=2) == [6, 7]
        assert await r.lpos("a", "c", count=2, rank=-1) == [7, 6]

        assert await r.lpos("axxx", "c", count=0, rank=2) == []
        assert await r.lpos("axxx", "c") is None

        assert await r.lpos("a", "x", count=2) == []
        assert await r.lpos("a", "x") is None

        assert await r.lpos("a", "a", count=0, maxlen=1) == [0]
        assert await r.lpos("a", "c", count=0, maxlen=1) == []
        assert await r.lpos("a", "c", count=0, maxlen=3) == [2]
        assert await r.lpos("a", "c", count=0, maxlen=3, rank=-1) == [7, 6]
        assert await r.lpos("a", "c", count=0, maxlen=7, rank=2) == [6]

    async def test_rpushx(self, r: redis.Redis):
        assert await r.rpushx("a", "b") == 0
        assert await r.lrange("a", 0, -1) == []
        await r.rpush("a", "1", "2", "3")
        assert await r.rpushx("a", "4") == 4
        assert await r.lrange("a", 0, -1) == [b"1", b"2", b"3", b"4"]

    # BLESS COMMANDS
    @skip_if_server_version_lt("8.11.0")
    async def test_bless_set_get_clear(self, r: redis.Redis):
        await r.set("a", 1)
        assert await r.bless_get("a") == []
        assert await r.bless_set("a", "NO-EVICT") == 1
        assert await r.bless_set("a", "NO-EVICT") == 0
        assert await r.bless_get("a") == [b"NO-EVICT"]
        assert await r.bless_clear("a", "NO-EVICT") == 1
        assert await r.bless_clear("a", "NO-EVICT") == 0
        assert await r.bless_get("a") == []

    @skip_if_server_version_lt("8.11.0")
    async def test_bless_missing_key(self, r: redis.Redis):
        with pytest.raises(exceptions.ResponseError):
            await r.bless_get("a")
        with pytest.raises(exceptions.ResponseError):
            await r.bless_set("a", "NO-EVICT")
        with pytest.raises(exceptions.ResponseError):
            await r.bless_clear("a", "NO-EVICT")

    @skip_if_server_version_lt("8.11.0")
    @pytest.mark.onlynoncluster
    async def test_bless_scan(self, r: redis.Redis):
        await r.set("a", 1)
        await r.set("b", 2)
        await r.set("c", 3)
        assert await r.bless_scan(0, "NO-EVICT") == (0, [])
        await r.bless_set("a", "NO-EVICT")
        await r.bless_set("b", "NO-EVICT")
        cursor, keys = await r.bless_scan(0, "NO-EVICT")
        assert cursor == 0
        assert set(keys) == {b"a", b"b"}

        cursor, keys = 0, []
        while True:
            cursor, page = await r.bless_scan(cursor, "NO-EVICT", count=1)
            keys.extend(page)
            if cursor == 0:
                break
        assert set(keys) == {b"a", b"b"}

    @skip_if_server_version_lt("8.11.0")
    @pytest.mark.onlynoncluster
    async def test_bless_scan_iter(self, r: redis.Redis):
        await r.set("a", 1)
        await r.set("b", 2)
        await r.set("c", 3)
        assert [k async for k in r.bless_scan_iter("NO-EVICT")] == []
        await r.bless_set("a", "NO-EVICT")
        await r.bless_set("b", "NO-EVICT")
        keys = [k async for k in r.bless_scan_iter("NO-EVICT")]
        assert set(keys) == {b"a", b"b"}
        keys = [k async for k in r.bless_scan_iter("NO-EVICT", count=1)]
        assert set(keys) == {b"a", b"b"}

    # SCAN COMMANDS
    @skip_if_server_version_lt("2.8.0")
    @pytest.mark.onlynoncluster
    async def test_scan(self, r: redis.Redis):
        await r.set("a", 1)
        await r.set("b", 2)
        await r.set("c", 3)
        cursor, keys = await r.scan()
        assert cursor == 0
        assert set(keys) == {b"a", b"b", b"c"}
        _, keys = await r.scan(match="a")
        assert set(keys) == {b"a"}

    @skip_if_server_version_lt(REDIS_6_VERSION)
    @pytest.mark.onlynoncluster
    async def test_scan_type(self, r: redis.Redis):
        await r.sadd("a-set", 1)
        await r.hset("a-hash", "foo", 2)
        await r.lpush("a-list", "aux", 3)
        _, keys = await r.scan(match="a*", _type="SET")
        assert set(keys) == {b"a-set"}

    @skip_if_server_version_lt("2.8.0")
    @pytest.mark.onlynoncluster
    async def test_scan_iter(self, r: redis.Redis):
        await r.set("a", 1)
        await r.set("b", 2)
        await r.set("c", 3)
        keys = [k async for k in r.scan_iter()]
        assert set(keys) == {b"a", b"b", b"c"}
        keys = [k async for k in r.scan_iter(match="a")]
        assert set(keys) == {b"a"}

    @skip_if_server_version_lt("2.8.0")
    async def test_sscan(self, r: redis.Redis):
        await r.sadd("a", 1, 2, 3)
        cursor, members = await r.sscan("a")
        assert cursor == 0
        assert set(members) == {b"1", b"2", b"3"}
        _, members = await r.sscan("a", match=b"1")
        assert set(members) == {b"1"}

    @skip_if_server_version_lt("2.8.0")
    async def test_sscan_iter(self, r: redis.Redis):
        await r.sadd("a", 1, 2, 3)
        members = [k async for k in r.sscan_iter("a")]
        assert set(members) == {b"1", b"2", b"3"}
        members = [k async for k in r.sscan_iter("a", match=b"1")]
        assert set(members) == {b"1"}

    @skip_if_server_version_lt("2.8.0")
    async def test_hscan(self, r: redis.Redis):
        await r.hset("a", mapping={"a": 1, "b": 2, "c": 3})
        cursor, dic = await r.hscan("a")
        assert cursor == 0
        assert dic == {b"a": b"1", b"b": b"2", b"c": b"3"}
        _, dic = await r.hscan("a", match="a")
        assert dic == {b"a": b"1"}
        _, dic = await r.hscan("a", no_values=False)
        assert dic == {b"a": b"1", b"b": b"2", b"c": b"3"}
        _, dic = await r.hscan("a_notset", match="a")
        assert dic == {}

    @skip_if_server_version_lt("7.3.240")
    async def test_hscan_novalues(self, r: redis.Redis):
        await r.hset("a", mapping={"a": 1, "b": 2, "c": 3})
        cursor, keys = await r.hscan("a", no_values=True)
        assert cursor == 0
        assert sorted(keys) == [b"a", b"b", b"c"]
        _, keys = await r.hscan("a", match="a", no_values=True)
        assert keys == [b"a"]
        _, keys = await r.hscan("a_notset", match="a", no_values=True)
        assert keys == []

    @skip_if_server_version_lt("2.8.0")
    async def test_hscan_iter(self, r: redis.Redis):
        await r.hset("a", mapping={"a": 1, "b": 2, "c": 3})
        dic = {k: v async for k, v in r.hscan_iter("a")}
        assert dic == {b"a": b"1", b"b": b"2", b"c": b"3"}
        dic = {k: v async for k, v in r.hscan_iter("a", match="a")}
        assert dic == {b"a": b"1"}
        dic = {k: v async for k, v in r.hscan_iter("a", no_values=False)}
        assert dic == {b"a": b"1", b"b": b"2", b"c": b"3"}
        dic = {k: v async for k, v in r.hscan_iter("a_notset", match="a")}
        assert dic == {}

    @skip_if_server_version_lt("7.3.240")
    async def test_hscan_iter_novalues(self, r: redis.Redis):
        await r.hset("a", mapping={"a": 1, "b": 2, "c": 3})
        keys = list([k async for k in r.hscan_iter("a", no_values=True)])
        assert sorted(keys) == [b"a", b"b", b"c"]
        keys = list([k async for k in r.hscan_iter("a", match="a", no_values=True)])
        assert keys == [b"a"]
        keys = list(
            [k async for k in r.hscan_iter("a", match="a_notset", no_values=True)]
        )
        assert keys == []

    @skip_if_server_version_lt("2.8.0")
    async def test_zscan(self, r: redis.Redis):
        await r.zadd("a", {"a": 1, "b": 2, "c": 3})
        cursor, pairs = await r.zscan("a")
        assert cursor == 0
        if expects_unified_shape(r):
            assert sorted(pairs) == [[b"a", 1.0], [b"b", 2.0], [b"c", 3.0]]
        else:
            assert set(pairs) == {(b"a", 1), (b"b", 2), (b"c", 3)}
        _, pairs = await r.zscan("a", match="a")
        if expects_unified_shape(r):
            assert pairs == [[b"a", 1.0]]
        else:
            assert set(pairs) == {(b"a", 1)}

    @skip_if_server_version_lt("2.8.0")
    async def test_zscan_iter(self, r: redis.Redis):
        await r.zadd("a", {"a": 1, "b": 2, "c": 3})
        pairs = [k async for k in r.zscan_iter("a")]
        if expects_unified_shape(r):
            assert sorted(pairs) == [[b"a", 1.0], [b"b", 2.0], [b"c", 3.0]]
        else:
            assert set(pairs) == {(b"a", 1), (b"b", 2), (b"c", 3)}
        pairs = [k async for k in r.zscan_iter("a", match="a")]
        if expects_unified_shape(r):
            assert pairs == [[b"a", 1.0]]
        else:
            assert set(pairs) == {(b"a", 1)}

    # SET COMMANDS
    async def test_sadd(self, r: redis.Redis):
        members = {b"1", b"2", b"3"}
        await r.sadd("a", *members)
        assert set(await r.smembers("a")) == members

    async def test_scard(self, r: redis.Redis):
        await r.sadd("a", "1", "2", "3")
        assert await r.scard("a") == 3

    @pytest.mark.onlynoncluster
    async def test_sdiff(self, r: redis.Redis):
        await r.sadd("a", "1", "2", "3")
        assert await r.sdiff("a", "b") == {b"1", b"2", b"3"}
        await r.sadd("b", "2", "3")
        assert await r.sdiff("a", "b") == {b"1"}

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.9.0")
    async def test_sdiffcard(self, r: redis.Redis):
        await r.sadd("s0", "a", "b", "c", "d", "e")
        await r.sadd("s1", "c", "d", "x")
        await r.sadd("s2", "e", "y")
        # exact difference cardinality: {a, b}
        assert await r.sdiffcard(3, ["s0", "s1", "s2"]) == 2
        # limited difference cardinality is capped at limit
        assert await r.sdiffcard(3, ["s0", "s1", "s2"], limit=1) == 1
        # a missing subtrahend key does not affect the result
        assert await r.sdiffcard(2, ["s0", "missing"]) == 5
        # a missing first key yields 0
        assert await r.sdiffcard(2, ["missing", "s0"]) == 0

    @pytest.mark.onlynoncluster
    async def test_sdiffstore(self, r: redis.Redis):
        await r.sadd("a", "1", "2", "3")
        assert await r.sdiffstore("c", "a", "b") == 3
        assert await r.smembers("c") == {b"1", b"2", b"3"}
        await r.sadd("b", "2", "3")
        assert await r.sdiffstore("c", "a", "b") == 1
        assert await r.smembers("c") == {b"1"}

    @pytest.mark.onlynoncluster
    async def test_sinter(self, r: redis.Redis):
        await r.sadd("a", "1", "2", "3")
        assert await r.sinter("a", "b") == set()
        await r.sadd("b", "2", "3")
        assert await r.sinter("a", "b") == {b"2", b"3"}

    @pytest.mark.onlynoncluster
    async def test_sinterstore(self, r: redis.Redis):
        await r.sadd("a", "1", "2", "3")
        assert await r.sinterstore("c", "a", "b") == 0
        assert await r.smembers("c") == set()
        await r.sadd("b", "2", "3")
        assert await r.sinterstore("c", "a", "b") == 2
        assert await r.smembers("c") == {b"2", b"3"}

    async def test_sismember(self, r: redis.Redis):
        await r.sadd("a", "1", "2", "3")
        assert await r.sismember("a", "1")
        assert await r.sismember("a", "2")
        assert await r.sismember("a", "3")
        assert not await r.sismember("a", "4")

    async def test_smembers(self, r: redis.Redis):
        await r.sadd("a", "1", "2", "3")
        assert set(await r.smembers("a")) == {b"1", b"2", b"3"}

    @pytest.mark.onlynoncluster
    async def test_smove(self, r: redis.Redis):
        await r.sadd("a", "a1", "a2")
        await r.sadd("b", "b1", "b2")
        assert await r.smove("a", "b", "a1")
        assert await r.smembers("a") == {b"a2"}
        assert await r.smembers("b") == {b"b1", b"b2", b"a1"}

    async def test_spop(self, r: redis.Redis):
        s = [b"1", b"2", b"3"]
        await r.sadd("a", *s)
        value = await r.spop("a")
        assert value in s
        assert set(await r.smembers("a")) == set(s) - {value}

    @skip_if_server_version_lt("3.2.0")
    async def test_spop_multi_value(self, r: redis.Redis):
        s = [b"1", b"2", b"3"]
        await r.sadd("a", *s)
        values = await r.spop("a", 2)
        assert len(values) == 2

        for value in values:
            assert value in s

        response = await r.spop("a", 1)
        assert set(response) == set(s) - set(values)

    async def test_srandmember(self, r: redis.Redis):
        s = [b"1", b"2", b"3"]
        await r.sadd("a", *s)
        assert await r.srandmember("a") in s

    @skip_if_server_version_lt("2.6.0")
    async def test_srandmember_multi_value(self, r: redis.Redis):
        s = [b"1", b"2", b"3"]
        await r.sadd("a", *s)
        randoms = await r.srandmember("a", number=2)
        assert len(randoms) == 2
        assert set(randoms).intersection(s) == set(randoms)

    async def test_srem(self, r: redis.Redis):
        await r.sadd("a", "1", "2", "3", "4")
        assert await r.srem("a", "5") == 0
        assert await r.srem("a", "2", "4") == 2
        assert set(await r.smembers("a")) == {b"1", b"3"}

    @pytest.mark.onlynoncluster
    async def test_sunion(self, r: redis.Redis):
        await r.sadd("a", "1", "2")
        await r.sadd("b", "2", "3")
        assert set(await r.sunion("a", "b")) == {b"1", b"2", b"3"}

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.9.0")
    async def test_sunioncard(self, r: redis.Redis):
        await r.sadd("s1", "a", "b", "c")
        await r.sadd("s2", "c", "d")
        # exact union cardinality: {a, b, c, d}
        assert await r.sunioncard(2, ["s1", "s2"]) == 4
        # approximate union cardinality (still an integer reply)
        assert await r.sunioncard(2, ["s1", "s2"], approx=True) == 4
        # a missing key is treated as an empty set
        assert await r.sunioncard(2, ["s1", "missing"]) == 3
        # limited union cardinality is capped at limit in exact mode
        assert await r.sunioncard(2, ["s1", "s2"], limit=3) == 3
        # APPROX combined with LIMIT does not exceed limit
        assert await r.sunioncard(2, ["s1", "s2"], limit=3, approx=True) <= 3

    @pytest.mark.onlynoncluster
    async def test_sunionstore(self, r: redis.Redis):
        await r.sadd("a", "1", "2")
        await r.sadd("b", "2", "3")
        assert await r.sunionstore("c", "a", "b") == 3
        assert set(await r.smembers("c")) == {b"1", b"2", b"3"}

    @skip_if_server_version_lt("8.11.240")
    async def test_sexpire_basic(self, r: redis.Redis):
        await r.delete("test:set")
        await r.sadd("test:set", "member1", "member2")
        assert await r.sexpire("test:set", 1, "member1") == [1]
        # test_spexpire_basic waits for a real expiry; here the stored TTL is enough
        assert await r.sttl("test:set", "member1", "member2") == [1, -1]
        assert 0 < (await r.spttl("test:set", "member1"))[0] <= 1000

    @skip_if_server_version_lt("8.11.240")
    async def test_sexpire_with_timedelta(self, r: redis.Redis):
        await r.delete("test:set")
        await r.sadd("test:set", "member1", "member2")
        assert await r.sexpire(
            "test:set", datetime.timedelta(seconds=10), "member1"
        ) == [1]
        # test_sexpire_basic waits for a real expiry; here the stored TTL is enough
        assert 0 < (await r.spttl("test:set", "member1"))[0] <= 10000
        assert await r.sttl("test:set", "member2") == [-1]

    @skip_if_server_version_lt("8.11.240")
    async def test_sexpire_conditions(self, r: redis.Redis):
        await r.delete("test:set")
        await r.sadd("test:set", "member1")
        assert await r.sexpire("test:set", 20, "member1", xx=True) == [0]
        assert await r.sexpire("test:set", 20, "member1", nx=True) == [1]
        assert await r.sexpire("test:set", 10, "member1", xx=True) == [1]
        assert await r.sexpire("test:set", 20, "member1", nx=True) == [0]
        # the xx call lowered the TTL and the last nx call left it alone
        assert 0 < (await r.spttl("test:set", "member1"))[0] <= 10000
        await r.sexpire("test:set", 20, "member1")
        assert await r.sexpire("test:set", 10, "member1", gt=True) == [0]
        assert await r.sexpire("test:set", 10, "member1", lt=True) == [1]
        assert 0 < (await r.spttl("test:set", "member1"))[0] <= 10000

    @skip_if_server_version_lt("8.11.240")
    async def test_sexpire_no_expiration_counts_as_infinite(self, r: redis.Redis):
        await r.delete("test:set")
        await r.sadd("test:set", "member1", "member2")
        # a member without an expiration never passes GT and always passes LT
        assert await r.sexpire("test:set", 100, "member1", gt=True) == [0]
        assert await r.sttl("test:set", "member1") == [-1]
        assert await r.sexpire("test:set", 100, "member2", lt=True) == [1]
        assert 0 < (await r.sttl("test:set", "member2"))[0] <= 100

    @skip_if_server_version_lt("8.11.240")
    async def test_sexpire_nonexistent_key_or_member(self, r: redis.Redis):
        await r.delete("test:set")
        assert await r.sexpire("test:set", 1, "member1", "member2") == [-2, -2]
        assert await r.exists("test:set") == 0
        await r.sadd("test:set", "member1")
        assert await r.sexpire("test:set", 1, "nonexistent_member") == [-2]

    @skip_if_server_version_lt("8.11.240")
    async def test_sexpire_multiple_members(self, r: redis.Redis):
        await r.delete("test:set")
        await r.sadd("test:set", "member1", "member2", "member3")
        await r.sexpire("test:set", 100, "member1")
        # the condition is evaluated per member, in the order of the request
        assert await r.sexpire(
            "test:set", 50, "member1", "member2", "nonexistent", nx=True
        ) == [0, 1, -2]
        ttls = await r.sttl("test:set", "member1", "member2", "member3")
        assert 50 < ttls[0] <= 100
        assert 0 < ttls[1] <= 50
        assert ttls[2] == -1

    @skip_if_server_version_lt("8.11.240")
    async def test_sexpire_multiple_condition_flags_error(self, r: redis.Redis):
        await r.delete("test:set")
        await r.sadd("test:set", "member1")
        with pytest.raises(ValueError) as e:
            await r.sexpire("test:set", 1, "member1", nx=True, xx=True)
        assert "Only one of" in str(e)

    @skip_if_server_version_lt("8.11.240")
    async def test_sexpire_past_time_deletes_members_and_key(self, r: redis.Redis):
        await r.delete("test:set")
        await r.sadd("test:set", "member1", "member2", "member3")
        assert await r.sexpire("test:set", 0, "member1") == [2]
        assert await r.smembers("test:set") == {b"member2", b"member3"}
        # when the last members go, the key goes with them
        assert await r.spexpireat("test:set", 1, "member2", "member3") == [2, 2]
        assert await r.exists("test:set") == 0

    @skip_if_server_version_lt("8.11.240")
    async def test_spexpire_basic(self, r: redis.Redis):
        await r.delete("test:set")
        await r.sadd("test:set", "member1", "member2")
        assert await r.spexpire("test:set", 100, "member1") == [1]
        # poll rather than sleep for the full TTL
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 3.0
        while await r.sismember("test:set", "member1"):
            assert loop.time() < deadline, "member1 did not expire"
            await asyncio.sleep(0.01)
        assert await r.smembers("test:set") == {b"member2"}

    @skip_if_server_version_lt("8.11.240")
    async def test_spexpire_with_timedelta(self, r: redis.Redis):
        await r.delete("test:set")
        await r.sadd("test:set", "member1", "member2")
        assert await r.spexpire(
            "test:set", datetime.timedelta(milliseconds=5000), "member1"
        ) == [1]
        assert 0 < (await r.spttl("test:set", "member1"))[0] <= 5000
        assert await r.spttl("test:set", "member2") == [-1]

    @skip_if_server_version_lt("8.11.240")
    async def test_spexpire_conditions(self, r: redis.Redis):
        await r.delete("test:set")
        await r.sadd("test:set", "member1")
        assert await r.spexpire("test:set", 20000, "member1", xx=True) == [0]
        assert await r.spexpire("test:set", 20000, "member1", nx=True) == [1]
        assert await r.spexpire("test:set", 10000, "member1", gt=True) == [0]
        assert await r.spexpire("test:set", 10000, "member1", lt=True) == [1]
        assert 0 < (await r.spttl("test:set", "member1"))[0] <= 10000

    @skip_if_server_version_lt("8.11.240")
    async def test_spexpire_multiple_condition_flags_error(self, r: redis.Redis):
        await r.delete("test:set")
        await r.sadd("test:set", "member1")
        with pytest.raises(ValueError) as e:
            await r.spexpire("test:set", 1000, "member1", gt=True, lt=True)
        assert "Only one of" in str(e)

    @skip_if_server_version_lt("8.11.240")
    async def test_sexpireat_basic(self, r: redis.Redis):
        await r.delete("test:set")
        await r.sadd("test:set", "member1", "member2")
        exp_time = int(
            (datetime.datetime.now() + datetime.timedelta(seconds=10)).timestamp()
        )
        assert await r.sexpireat("test:set", exp_time, "member1") == [1]
        assert await r.sexpiretime("test:set", "member1", "member2") == [exp_time, -1]

    @skip_if_server_version_lt("8.11.240")
    async def test_sexpireat_with_datetime(self, r: redis.Redis):
        await r.delete("test:set")
        await r.sadd("test:set", "member1", "member2")
        exp_time = (datetime.datetime.now() + datetime.timedelta(seconds=10)).replace(
            microsecond=0
        )
        assert await r.sexpireat("test:set", exp_time, "member1") == [1]
        assert await r.sexpiretime("test:set", "member1", "member2") == [
            int(exp_time.timestamp()),
            -1,
        ]

    @skip_if_server_version_lt("8.11.240")
    async def test_sexpireat_conditions(self, r: redis.Redis):
        await r.delete("test:set")
        await r.sadd("test:set", "member1")
        future_exp_time = int(
            (datetime.datetime.now() + datetime.timedelta(seconds=10)).timestamp()
        )
        past_exp_time = int(
            (datetime.datetime.now() - datetime.timedelta(seconds=10)).timestamp()
        )
        assert await r.sexpireat("test:set", future_exp_time, "member1", xx=True) == [0]
        assert await r.sexpireat("test:set", future_exp_time, "member1", nx=True) == [1]
        assert await r.sexpireat("test:set", past_exp_time, "member1", gt=True) == [0]
        assert await r.sexpireat("test:set", past_exp_time, "member1", lt=True) == [2]
        assert await r.exists("test:set") == 0

    @skip_if_server_version_lt("8.11.240")
    async def test_spexpireat_basic(self, r: redis.Redis):
        await r.delete("test:set")
        await r.sadd("test:set", "member1", "member2")
        exp_time = int(
            (datetime.datetime.now() + datetime.timedelta(seconds=10)).timestamp()
            * 1000
        )
        assert await r.spexpireat("test:set", exp_time, "member1") == [1]
        # the stored expire time is exact
        assert await r.spexpiretime("test:set", "member1", "member2") == [exp_time, -1]

    @skip_if_server_version_lt("8.11.240")
    async def test_spexpireat_with_datetime(self, r: redis.Redis):
        await r.delete("test:set")
        await r.sadd("test:set", "member1", "member2")
        exp_time = datetime.datetime.now() + datetime.timedelta(seconds=10)
        assert await r.spexpireat("test:set", exp_time, "member1") == [1]
        assert await r.spexpiretime("test:set", "member1", "member2") == [
            int(exp_time.timestamp() * 1000),
            -1,
        ]

    @skip_if_server_version_lt("8.11.240")
    async def test_sexpireat_multiple_condition_flags_error(self, r: redis.Redis):
        await r.delete("test:set")
        await r.sadd("test:set", "member1")
        exp_time = int(
            (datetime.datetime.now() + datetime.timedelta(seconds=10)).timestamp()
        )
        with pytest.raises(ValueError) as e:
            await r.sexpireat("test:set", exp_time, "member1", xx=True, gt=True)
        assert "Only one of" in str(e)

    @skip_if_server_version_lt("8.11.240")
    async def test_spexpireat_conditions(self, r: redis.Redis):
        await r.delete("test:set")
        await r.sadd("test:set", "member1")
        future_exp_time = int(
            (datetime.datetime.now() + datetime.timedelta(seconds=10)).timestamp()
            * 1000
        )
        past_exp_time = int(
            (datetime.datetime.now() - datetime.timedelta(seconds=10)).timestamp()
            * 1000
        )
        assert await r.spexpireat("test:set", future_exp_time, "member1", xx=True) == [
            0
        ]
        assert await r.spexpireat("test:set", future_exp_time, "member1", nx=True) == [
            1
        ]
        assert await r.spexpireat("test:set", past_exp_time, "member1", gt=True) == [0]
        assert await r.spexpireat("test:set", past_exp_time, "member1", lt=True) == [2]
        assert await r.exists("test:set") == 0

    @skip_if_server_version_lt("8.11.240")
    async def test_spexpireat_multiple_condition_flags_error(self, r: redis.Redis):
        await r.delete("test:set")
        await r.sadd("test:set", "member1")
        exp_time = int(
            (datetime.datetime.now() + datetime.timedelta(seconds=10)).timestamp()
            * 1000
        )
        with pytest.raises(ValueError) as e:
            await r.spexpireat("test:set", exp_time, "member1", nx=True, lt=True)
        assert "Only one of" in str(e)

    @skip_if_server_version_lt("8.11.240")
    async def test_spersist_multiple_members(self, r: redis.Redis):
        await r.delete("test:set")
        await r.sadd("test:set", "member1", "member2")
        await r.sexpire("test:set", 5000, "member1")
        assert await r.spersist("test:set", "member1", "member2", "member3") == [
            1,
            -1,
            -2,
        ]
        assert await r.sttl("test:set", "member1") == [-1]

    @skip_if_server_version_lt("8.11.240")
    async def test_spersist_nonexistent_key(self, r: redis.Redis):
        await r.delete("test:set")
        assert await r.spersist("test:set", "member1", "member2") == [-2, -2]

    @skip_if_server_version_lt("8.11.240")
    async def test_sttl_and_spttl_multiple_members(self, r: redis.Redis):
        await r.delete("test:set")
        await r.sadd("test:set", "member1", "member2")
        await r.spexpire("test:set", 1800, "member1")
        ttl = await r.sttl("test:set", "member1", "member2", "member3")
        # STTL rounds up, so a live member never reports 0; the exact rounding is
        # pinned by test_sexpiretime_and_spexpiretime_multiple_members
        assert ttl[0] in (1, 2)
        assert ttl[1:] == [-1, -2]
        pttl = await r.spttl("test:set", "member1", "member2", "member3")
        assert 0 < pttl[0] <= 1800
        assert pttl[1:] == [-1, -2]

    @skip_if_server_version_lt("8.11.240")
    async def test_sttl_and_spttl_nonexistent_key(self, r: redis.Redis):
        await r.delete("test:set")
        assert await r.sttl("test:set", "member1", "member2") == [-2, -2]
        assert await r.spttl("test:set", "member1", "member2") == [-2, -2]

    @skip_if_server_version_lt("8.11.240")
    async def test_sexpiretime_and_spexpiretime_multiple_members(self, r: redis.Redis):
        await r.delete("test:set")
        await r.sadd("test:set", "member1", "member2")
        exp_time_ms = int(
            (datetime.datetime.now() + datetime.timedelta(minutes=30)).timestamp()
            * 1000
        )
        await r.spexpireat("test:set", exp_time_ms, "member1")
        assert await r.spexpiretime("test:set", "member1", "member2", "member3") == [
            exp_time_ms,
            -1,
            -2,
        ]
        # SEXPIRETIME rounds up to the next second
        assert await r.sexpiretime("test:set", "member1", "member2", "member3") == [
            (exp_time_ms + 999) // 1000,
            -1,
            -2,
        ]

    @skip_if_server_version_lt("8.11.240")
    async def test_sexpiretime_and_spexpiretime_nonexistent_key(self, r: redis.Redis):
        await r.delete("test:set")
        assert await r.sexpiretime("test:set", "member1", "member2") == [-2, -2]
        assert await r.spexpiretime("test:set", "member1", "member2") == [-2, -2]

    @skip_if_server_version_lt("8.11.240")
    async def test_saddex_no_expiration(self, r: redis.Redis):
        await r.delete("test:set")
        assert await r.saddex("test:set", "member1", "member2") == 1
        assert await r.smembers("test:set") == {b"member1", b"member2"}
        assert await r.sttl("test:set", "member1", "member2") == [-1, -1]

    @skip_if_server_version_lt("8.11.240")
    async def test_saddex_nothing_changed(self, r: redis.Redis):
        await r.delete("test:set")
        await r.sadd("test:set", "member1")
        # 0 when no member was added, updated or deleted
        assert await r.saddex("test:set", "member1") == 0
        assert await r.saddex("test:set", "member1", keepttl=True) == 0
        assert await r.smembers("test:set") == {b"member1"}

    @skip_if_server_version_lt("8.11.240")
    async def test_saddex_without_expiration_discards_existing_one(
        self, r: redis.Redis
    ):
        await r.delete("test:set")
        assert await r.saddex("test:set", "member1", ex=100) == 1
        assert await r.saddex("test:set", "member1", "member2") == 1
        assert await r.sttl("test:set", "member1", "member2") == [-1, -1]

    @skip_if_server_version_lt("8.11.240")
    async def test_saddex_expiration_ex_and_keepttl(self, r: redis.Redis):
        await r.delete("test:set")
        assert await r.saddex("test:set", "member1", "member2", ex=10) == 1
        ttls = await r.sttl("test:set", "member1", "member2")
        for ttl in ttls:
            assert 0 < ttl <= 10
        # keepttl keeps the existing expiration; a new member gets none
        assert await r.saddex("test:set", "member1", "member3", keepttl=True) == 1
        assert 0 < (await r.spttl("test:set", "member1"))[0] <= 10000
        assert await r.sttl("test:set", "member3") == [-1]

    @skip_if_server_version_lt("8.11.240")
    async def test_saddex_expiration_px(self, r: redis.Redis):
        await r.delete("test:set")
        assert await r.saddex("test:set", "member1", "member2", px=60000) == 1
        pttls = await r.spttl("test:set", "member1", "member2")
        for pttl in pttls:
            assert 0 < pttl <= 60000

    @skip_if_server_version_lt("8.11.240")
    async def test_saddex_expiration_digit_strings(self, r: redis.Redis):
        await r.delete("test:set")
        assert await r.saddex("test:set", "member1", ex="60") == 1
        assert 0 < (await r.sttl("test:set", "member1"))[0] <= 60
        assert await r.saddex("test:set", "member2", px="500") == 1
        assert 0 < (await r.spttl("test:set", "member2"))[0] <= 500

    @skip_if_server_version_lt("8.11.240")
    async def test_saddex_expiration_exat_and_pxat(self, r: redis.Redis):
        await r.delete("test:set")
        exat = datetime.datetime.now() + datetime.timedelta(seconds=30)
        assert await r.saddex("test:set", "member1", exat=exat) == 1
        assert await r.sexpiretime("test:set", "member1") == [int(exat.timestamp())]
        pxat = int(
            (datetime.datetime.now() + datetime.timedelta(seconds=30)).timestamp()
            * 1000
        )
        assert await r.saddex("test:set", "member2", pxat=pxat) == 1
        assert await r.spexpiretime("test:set", "member2") == [pxat]

    @skip_if_server_version_lt("8.11.240")
    async def test_saddex_mnx(self, r: redis.Redis):
        await r.delete("test:set")
        await r.sadd("test:set", "member1")
        # MNX fails as a whole when any member exists
        assert (
            await r.saddex(
                "test:set",
                "member1",
                "member2",
                ex=10,
                data_persist_option=SetDataPersistOptions.MNX,
            )
            == 0
        )
        assert await r.smembers("test:set") == {b"member1"}
        assert await r.sttl("test:set", "member1") == [-1]
        assert (
            await r.saddex(
                "test:set",
                "member2",
                "member3",
                ex=10,
                data_persist_option=SetDataPersistOptions.MNX,
            )
            == 1
        )
        assert await r.smembers("test:set") == {b"member1", b"member2", b"member3"}

    @skip_if_server_version_lt("8.11.240")
    async def test_saddex_mxx(self, r: redis.Redis):
        await r.delete("test:set")
        # MXX on a key that does not exist creates nothing
        assert (
            await r.saddex(
                "test:set", "member1", data_persist_option=SetDataPersistOptions.MXX
            )
            == 0
        )
        assert await r.exists("test:set") == 0
        await r.sadd("test:set", "member1", "member2")
        assert (
            await r.saddex(
                "test:set",
                "member1",
                "member3",
                ex=10,
                data_persist_option=SetDataPersistOptions.MXX,
            )
            == 0
        )
        assert await r.smembers("test:set") == {b"member1", b"member2"}
        assert (
            await r.saddex(
                "test:set",
                "member1",
                "member2",
                ex=10,
                data_persist_option=SetDataPersistOptions.MXX,
            )
            == 1
        )
        ttls = await r.sttl("test:set", "member1", "member2")
        for ttl in ttls:
            assert 0 < ttl <= 10

    @skip_if_server_version_lt("8.11.240")
    async def test_saddex_past_expiration_removes_members(self, r: redis.Redis):
        await r.delete("test:set")
        await r.sadd("test:set", "member1", "member2")
        # 1 because member1 was deleted, even though nothing was added
        assert await r.saddex("test:set", "member1", "member3", pxat=1) == 1
        assert await r.smembers("test:set") == {b"member2"}
        # 0 when there is nothing to delete; the key is not created
        await r.delete("test:set")
        assert await r.saddex("test:set", "member1", exat=1) == 0
        assert await r.exists("test:set") == 0

    @skip_if_server_version_lt("8.11.240")
    async def test_saddex_invalid_inputs(self, r: redis.Redis):
        with pytest.raises(exceptions.DataError):
            await r.saddex("test:set", "member1", ex=10.0)

        with pytest.raises(exceptions.DataError):
            await r.saddex("test:set", "member1", ex=10, px=1000)

        with pytest.raises(exceptions.DataError):
            await r.saddex("test:set", "member1", ex=10, keepttl=True)

        # the server rejects an empty member list
        with pytest.raises(exceptions.ResponseError):
            await r.saddex("test:set")

    @skip_if_server_version_lt("8.11.240")
    async def test_set_member_expiration_wrong_type(self, r: redis.Redis):
        await r.delete("test:set")
        await r.set("test:set", "value")
        with pytest.raises(exceptions.ResponseError, match="WRONGTYPE"):
            await r.sexpire("test:set", 10, "member1")
        with pytest.raises(exceptions.ResponseError, match="WRONGTYPE"):
            await r.sttl("test:set", "member1")
        with pytest.raises(exceptions.ResponseError, match="WRONGTYPE"):
            await r.saddex("test:set", "member1")

    @skip_if_server_version_lt("8.11.240")
    async def test_set_member_expiration_pipeline(self, r: redis.Redis):
        await r.delete("test:set")
        async with r.pipeline() as pipe:
            pipe.saddex("test:set", "member1", "member2", ex=100)
            pipe.sexpire("test:set", 50, "member1", "member3", lt=True)
            pipe.spersist("test:set", "member2")
            pipe.sttl("test:set", "member1", "member2", "member3")
            saddex, sexpire, spersist, sttl = await pipe.execute()
        assert saddex == 1
        assert sexpire == [1, -2]
        assert spersist == [1]
        assert 0 < sttl[0] <= 50
        assert sttl[1:] == [-1, -2]

    # SORTED SET COMMANDS
    async def test_zadd(self, r: redis.Redis):
        mapping = {"a1": 1.0, "a2": 2.0, "a3": 3.0}
        await r.zadd("a", mapping)
        response = await r.zrange("a", 0, -1, withscores=True)
        assert_resp_response(
            r,
            response,
            [(b"a1", 1.0), (b"a2", 2.0), (b"a3", 3.0)],
            [[b"a1", 1.0], [b"a2", 2.0], [b"a3", 3.0]],
        )

        # error cases
        with pytest.raises(exceptions.DataError):
            await r.zadd("a", {})

        # cannot use both nx and xx options
        with pytest.raises(exceptions.DataError):
            await r.zadd("a", mapping, nx=True, xx=True)

        # cannot use the incr options with more than one value
        with pytest.raises(exceptions.DataError):
            await r.zadd("a", mapping, incr=True)

    async def test_zadd_nx(self, r: redis.Redis):
        assert await r.zadd("a", {"a1": 1}) == 1
        assert await r.zadd("a", {"a1": 99, "a2": 2}, nx=True) == 1
        response = await r.zrange("a", 0, -1, withscores=True)
        assert_resp_response(
            r, response, [(b"a1", 1.0), (b"a2", 2.0)], [[b"a1", 1.0], [b"a2", 2.0]]
        )

    async def test_zadd_xx(self, r: redis.Redis):
        assert await r.zadd("a", {"a1": 1}) == 1
        assert await r.zadd("a", {"a1": 99, "a2": 2}, xx=True) == 0
        response = await r.zrange("a", 0, -1, withscores=True)
        assert_resp_response(r, response, [(b"a1", 99.0)], [[b"a1", 99.0]])

    async def test_zadd_ch(self, r: redis.Redis):
        assert await r.zadd("a", {"a1": 1}) == 1
        assert await r.zadd("a", {"a1": 99, "a2": 2}, ch=True) == 2
        response = await r.zrange("a", 0, -1, withscores=True)
        assert_resp_response(
            r, response, [(b"a2", 2.0), (b"a1", 99.0)], [[b"a2", 2.0], [b"a1", 99.0]]
        )

    async def test_zadd_incr(self, r: redis.Redis):
        assert await r.zadd("a", {"a1": 1}) == 1
        assert await r.zadd("a", {"a1": 4.5}, incr=True) == 5.5

    async def test_zadd_incr_with_xx(self, r: redis.Redis):
        # this asks zadd to incr 'a1' only if it exists, but it clearly
        # doesn't. Redis returns a null value in this case and so should
        # redis-py
        assert await r.zadd("a", {"a1": 1}, xx=True, incr=True) is None

    async def test_zcard(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert await r.zcard("a") == 3

    async def test_zcount(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert await r.zcount("a", "-inf", "+inf") == 3
        assert await r.zcount("a", 1, 2) == 2
        assert await r.zcount("a", "(" + str(1), 2) == 1
        assert await r.zcount("a", 1, "(" + str(2)) == 1
        assert await r.zcount("a", 10, 20) == 0

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    async def test_zdiff(self, r):
        await r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        await r.zadd("b", {"a1": 1, "a2": 2})
        assert await r.zdiff(["a", "b"]) == [b"a3"]
        response = await r.zdiff(["a", "b"], withscores=True)
        assert_resp_response(r, response, [b"a3", b"3"], [[b"a3", 3.0]])

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    async def test_zdiffstore(self, r):
        await r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        await r.zadd("b", {"a1": 1, "a2": 2})
        assert await r.zdiffstore("out", ["a", "b"])
        assert await r.zrange("out", 0, -1) == [b"a3"]
        response = await r.zrange("out", 0, -1, withscores=True)
        assert_resp_response(r, response, [(b"a3", 3.0)], [[b"a3", 3.0]])

    async def test_zincrby(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert await r.zincrby("a", 1, "a2") == 3.0
        assert await r.zincrby("a", 5, "a3") == 8.0
        assert await r.zscore("a", "a2") == 3.0
        assert await r.zscore("a", "a3") == 8.0

    @skip_if_server_version_lt("2.8.9")
    async def test_zlexcount(self, r: redis.Redis):
        await r.zadd("a", {"a": 0, "b": 0, "c": 0, "d": 0, "e": 0, "f": 0, "g": 0})
        assert await r.zlexcount("a", "-", "+") == 7
        assert await r.zlexcount("a", "[b", "[f") == 5

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
    async def test_zaggregate_mapping_weights(
        self, r: redis.Redis, keys_factory, command, expected
    ):
        # Mapping wrappers must retain weights and the resulting rank order.
        await r.zadd("a", {"shared": 2, "left": 3})
        await r.zadd("b", {"shared": 5, "right": 7})
        keys = keys_factory({"a": 2, "b": -1})
        if command.endswith("store"):
            assert await getattr(r, command)("destination", keys) == len(expected)
            result = await r.zrange("destination", 0, -1, withscores=True)
        else:
            result = await getattr(r, command)(keys, withscores=True)
        assert_resp_response(r, result, expected, [list(row) for row in expected])

    @pytest.mark.onlynoncluster
    async def test_zinterstore_sum(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert await r.zinterstore("d", ["a", "b", "c"]) == 2
        response = await r.zrange("d", 0, -1, withscores=True)
        assert_resp_response(
            r, response, [(b"a3", 8), (b"a1", 9)], [[b"a3", 8.0], [b"a1", 9.0]]
        )

    @pytest.mark.onlynoncluster
    async def test_zinterstore_max(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert await r.zinterstore("d", ["a", "b", "c"], aggregate="MAX") == 2
        response = await r.zrange("d", 0, -1, withscores=True)
        assert_resp_response(
            r, response, [(b"a3", 5), (b"a1", 6)], [[b"a3", 5], [b"a1", 6]]
        )

    @pytest.mark.onlynoncluster
    async def test_zinterstore_min(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        await r.zadd("b", {"a1": 2, "a2": 3, "a3": 5})
        await r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert await r.zinterstore("d", ["a", "b", "c"], aggregate="MIN") == 2
        response = await r.zrange("d", 0, -1, withscores=True)
        assert_resp_response(
            r, response, [(b"a1", 1), (b"a3", 3)], [[b"a1", 1], [b"a3", 3]]
        )

    @pytest.mark.onlynoncluster
    async def test_zinterstore_with_weight(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert await r.zinterstore("d", {"a": 1, "b": 2, "c": 3}) == 2
        response = await r.zrange("d", 0, -1, withscores=True)
        assert_resp_response(
            r, response, [(b"a3", 20), (b"a1", 23)], [[b"a3", 20], [b"a1", 23]]
        )

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.7.0")
    async def test_zinterstore_count(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert await r.zinterstore("d", ["a", "b", "c"], aggregate="COUNT") == 2
        response = await r.zrange("d", 0, -1, withscores=True)
        assert_resp_response(
            r, response, [(b"a1", 3), (b"a3", 3)], [[b"a1", 3.0], [b"a3", 3.0]]
        )

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.7.0")
    async def test_zinterstore_count_with_weight(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            await r.zinterstore("d", {"a": 1, "b": 2, "c": 3}, aggregate="COUNT") == 2
        )
        response = await r.zrange("d", 0, -1, withscores=True)
        assert_resp_response(
            r, response, [(b"a1", 6), (b"a3", 6)], [[b"a1", 6.0], [b"a3", 6.0]]
        )

    @skip_if_server_version_lt("4.9.0")
    async def test_zpopmax(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        response = await r.zpopmax("a")
        assert_resp_response(
            r,
            response,
            [(b"a3", 3)],
            [b"a3", 3.0],
            unified_expected=[[b"a3", 3.0]],
        )

        # with count
        response = await r.zpopmax("a", count=2)
        assert_resp_response(
            r, response, [(b"a2", 2), (b"a1", 1)], [[b"a2", 2], [b"a1", 1]]
        )

    @skip_if_server_version_lt("4.9.0")
    async def test_zpopmin(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        response = await r.zpopmin("a")
        assert_resp_response(
            r,
            response,
            [(b"a1", 1)],
            [b"a1", 1.0],
            unified_expected=[[b"a1", 1.0]],
        )

        # with count
        response = await r.zpopmin("a", count=2)
        assert_resp_response(
            r, response, [(b"a2", 2), (b"a3", 3)], [[b"a2", 2], [b"a3", 3]]
        )

    @skip_if_server_version_lt("4.9.0")
    @pytest.mark.onlynoncluster
    async def test_bzpopmax(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 2})
        await r.zadd("b", {"b1": 10, "b2": 20})
        assert_resp_response(
            r,
            await r.bzpopmax(["b", "a"], timeout=1),
            (b"b", b"b2", 20),
            [b"b", b"b2", 20],
        )
        assert_resp_response(
            r,
            await r.bzpopmax(["b", "a"], timeout=1),
            (b"b", b"b1", 10),
            [b"b", b"b1", 10],
        )
        assert_resp_response(
            r,
            await r.bzpopmax(["b", "a"], timeout=1),
            (b"a", b"a2", 2),
            [b"a", b"a2", 2],
        )
        assert_resp_response(
            r,
            await r.bzpopmax(["b", "a"], timeout=1),
            (b"a", b"a1", 1),
            [b"a", b"a1", 1],
        )
        assert await r.bzpopmax(["b", "a"], timeout=0.1) is None
        await r.zadd("c", {"c1": 100})
        assert_resp_response(
            r, await r.bzpopmax("c", timeout=1), (b"c", b"c1", 100), [b"c", b"c1", 100]
        )

    @skip_if_server_version_lt("4.9.0")
    @pytest.mark.onlynoncluster
    async def test_bzpopmin(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 2})
        await r.zadd("b", {"b1": 10, "b2": 20})
        assert_resp_response(
            r,
            await r.bzpopmin(["b", "a"], timeout=1),
            (b"b", b"b1", 10),
            [b"b", b"b1", 10],
        )
        assert_resp_response(
            r,
            await r.bzpopmin(["b", "a"], timeout=1),
            (b"b", b"b2", 20),
            [b"b", b"b2", 20],
        )
        assert_resp_response(
            r,
            await r.bzpopmin(["b", "a"], timeout=1),
            (b"a", b"a1", 1),
            [b"a", b"a1", 1],
        )
        assert_resp_response(
            r,
            await r.bzpopmin(["b", "a"], timeout=1),
            (b"a", b"a2", 2),
            [b"a", b"a2", 2],
        )
        assert await r.bzpopmin(["b", "a"], timeout=0.1) is None
        await r.zadd("c", {"c1": 100})
        assert_resp_response(
            r, await r.bzpopmin("c", timeout=1), (b"c", b"c1", 100), [b"c", b"c1", 100]
        )

    async def test_zrange(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert await r.zrange("a", 0, 1) == [b"a1", b"a2"]
        assert await r.zrange("a", 1, 2) == [b"a2", b"a3"]

        # withscores
        response = await r.zrange("a", 0, 1, withscores=True)
        assert_resp_response(
            r, response, [(b"a1", 1.0), (b"a2", 2.0)], [[b"a1", 1.0], [b"a2", 2.0]]
        )
        response = await r.zrange("a", 1, 2, withscores=True)
        assert_resp_response(
            r, response, [(b"a2", 2.0), (b"a3", 3.0)], [[b"a2", 2.0], [b"a3", 3.0]]
        )

        # custom score cast function
        response = await r.zrange("a", 0, 1, withscores=True, score_cast_func=safe_str)
        assert_resp_response(
            r,
            response,
            [(b"a1", "1"), (b"a2", "2")],
            [[b"a1", "1.0"], [b"a2", "2.0"]],
        )

    async def test_zrange_desc_sends_zrevrange_by_default(self):
        """Rank-only desc=True keeps ZREVRANGE unless use_rev is set."""
        r = redis.Redis()
        with patch.object(
            r, "execute_command", new=AsyncMock(return_value=[])
        ) as execute:
            await r.zrange("a", 0, -1, desc=True)
        execute.assert_called_once_with(
            "ZREVRANGE", "a", 0, -1, withscores=False, score_cast_func=float, keys=["a"]
        )

    async def test_zrange_desc_use_rev_sends_zrange_rev(self):
        r = redis.Redis()
        with patch.object(
            r, "execute_command", new=AsyncMock(return_value=[])
        ) as execute:
            await r.zrange("a", 0, -1, desc=True, use_rev=True)
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

    async def test_zrange_desc_use_rev_withscores_and_score_cast(self):
        def cast_score(value):
            return value

        r = redis.Redis()
        with patch.object(
            r, "execute_command", new=AsyncMock(return_value=[])
        ) as execute:
            await r.zrange(
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

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("6.2.0")
    @pytest.mark.parametrize("dest", ["", b"", memoryview(b"")])
    async def test_zrangestore_empty_destination(self, r: redis.Redis, dest):
        await r.zadd("source", {"a": 1, "b": 2, "c": 3})
        assert await r.zrangestore(dest, "source", 0, 1) == 2
        assert await r.zrange(dest, 0, -1) == [b"a", b"b"]
        assert await r.zrange("source", 0, -1) == [b"a", b"b", b"c"]

    @skip_if_server_version_lt("2.8.9")
    async def test_zrangebylex(self, r: redis.Redis):
        await r.zadd("a", {"a": 0, "b": 0, "c": 0, "d": 0, "e": 0, "f": 0, "g": 0})
        assert await r.zrangebylex("a", "-", "[c") == [b"a", b"b", b"c"]
        assert await r.zrangebylex("a", "-", "(c") == [b"a", b"b"]
        assert await r.zrangebylex("a", "[aaa", "(g") == [b"b", b"c", b"d", b"e", b"f"]
        assert await r.zrangebylex("a", "[f", "+") == [b"f", b"g"]
        assert await r.zrangebylex("a", "-", "+", start=3, num=2) == [b"d", b"e"]

    @skip_if_server_version_lt("2.9.9")
    async def test_zrevrangebylex(self, r: redis.Redis):
        await r.zadd("a", {"a": 0, "b": 0, "c": 0, "d": 0, "e": 0, "f": 0, "g": 0})
        assert await r.zrevrangebylex("a", "[c", "-") == [b"c", b"b", b"a"]
        assert await r.zrevrangebylex("a", "(c", "-") == [b"b", b"a"]
        assert await r.zrevrangebylex("a", "(g", "[aaa") == [
            b"f",
            b"e",
            b"d",
            b"c",
            b"b",
        ]
        assert await r.zrevrangebylex("a", "+", "[f") == [b"g", b"f"]
        assert await r.zrevrangebylex("a", "+", "-", start=3, num=2) == [b"d", b"c"]

    async def test_zrangebyscore(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert await r.zrangebyscore("a", 2, 4) == [b"a2", b"a3", b"a4"]

        # slicing with start/num
        assert await r.zrangebyscore("a", 2, 4, start=1, num=2) == [b"a3", b"a4"]

        # withscores
        response = await r.zrangebyscore("a", 2, 4, withscores=True)
        assert_resp_response(
            r,
            response,
            [(b"a2", 2.0), (b"a3", 3.0), (b"a4", 4.0)],
            [[b"a2", 2.0], [b"a3", 3.0], [b"a4", 4.0]],
        )

        # custom score function
        response = await r.zrangebyscore(
            "a", 2, 4, withscores=True, score_cast_func=int
        )
        assert_resp_response(
            r,
            response,
            [(b"a2", 2), (b"a3", 3), (b"a4", 4)],
            [[b"a2", 2], [b"a3", 3], [b"a4", 4]],
        )
        response = await r.zrangebyscore(
            "a", 2, 4, withscores=True, score_cast_func=safe_str
        )
        assert_resp_response(
            r,
            response,
            [(b"a2", "2"), (b"a3", "3"), (b"a4", "4")],
            [[b"a2", "2.0"], [b"a3", "3.0"], [b"a4", "4.0"]],
        )

    async def test_zrank(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert await r.zrank("a", "a1") == 0
        assert await r.zrank("a", "a2") == 1
        assert await r.zrank("a", "a6") is None

    @skip_if_server_version_lt("7.2.0")
    async def test_zrank_withscore(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert await r.zrank("a", "a1") == 0
        assert await r.zrank("a", "a2") == 1
        assert await r.zrank("a", "a6") is None
        assert_resp_response(
            r, await r.zrank("a", "a3", withscore=True), [2, 3.0], [2, 3.0]
        )
        assert await r.zrank("a", "a6", withscore=True) is None

        # custom score cast function
        response = await r.zrank("a", "a3", withscore=True, score_cast_func=safe_str)
        assert_resp_response(r, response, [2, "3"], [2, "3.0"])

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.2.0")
    async def test_zset_scores_decode_responses(self, decoded_r: redis.Redis):
        # Scores handed back as text must be str, as on a RESP2 connection.
        r = decoded_r
        identity = lambda x: x  # noqa: E731
        await r.zadd("a", {"a1": 1, "a2": 2.5})
        await r.zadd("b", {"a1": 1})
        assert_resp_response(
            r,
            await r.zdiff(["a", "b"], withscores=True),
            ["a2", "2.5"],
            [["a2", 2.5]],
        )
        assert_resp_response(
            r,
            await r.zrange("a", 0, -1, withscores=True, score_cast_func=identity),
            [("a1", "1"), ("a2", "2.5")],
            [["a1", 1.0], ["a2", 2.5]],
        )
        assert_resp_response(
            r,
            await r.zrank("a", "a2", withscore=True, score_cast_func=identity),
            [1, "2.5"],
            [1, 2.5],
        )
        assert_resp_response(
            r,
            await r.zmpop(1, ["b"], min=True),
            ["b", [["a1", "1"]]],
            ["b", [["a1", 1.0]]],
        )

    async def test_zrem(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert await r.zrem("a", "a2") == 1
        assert await r.zrange("a", 0, -1) == [b"a1", b"a3"]
        assert await r.zrem("a", "b") == 0
        assert await r.zrange("a", 0, -1) == [b"a1", b"a3"]

    async def test_zrem_multiple_keys(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert await r.zrem("a", "a1", "a2") == 2
        assert await r.zrange("a", 0, 5) == [b"a3"]

    @skip_if_server_version_lt("2.8.9")
    async def test_zremrangebylex(self, r: redis.Redis):
        await r.zadd("a", {"a": 0, "b": 0, "c": 0, "d": 0, "e": 0, "f": 0, "g": 0})
        assert await r.zremrangebylex("a", "-", "[c") == 3
        assert await r.zrange("a", 0, -1) == [b"d", b"e", b"f", b"g"]
        assert await r.zremrangebylex("a", "[f", "+") == 2
        assert await r.zrange("a", 0, -1) == [b"d", b"e"]
        assert await r.zremrangebylex("a", "[h", "+") == 0
        assert await r.zrange("a", 0, -1) == [b"d", b"e"]

    async def test_zremrangebyrank(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert await r.zremrangebyrank("a", 1, 3) == 3
        assert await r.zrange("a", 0, 5) == [b"a1", b"a5"]

    async def test_zremrangebyscore(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert await r.zremrangebyscore("a", 2, 4) == 3
        assert await r.zrange("a", 0, -1) == [b"a1", b"a5"]
        assert await r.zremrangebyscore("a", 2, 4) == 0
        assert await r.zrange("a", 0, -1) == [b"a1", b"a5"]

    async def test_zrevrange(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert await r.zrevrange("a", 0, 1) == [b"a3", b"a2"]
        assert await r.zrevrange("a", 1, 2) == [b"a2", b"a1"]

        # withscores
        response = await r.zrevrange("a", 0, 1, withscores=True)
        assert_resp_response(
            r, response, [(b"a3", 3.0), (b"a2", 2.0)], [[b"a3", 3.0], [b"a2", 2.0]]
        )
        response = await r.zrevrange("a", 1, 2, withscores=True)
        assert_resp_response(
            r, response, [(b"a2", 2.0), (b"a1", 1.0)], [[b"a2", 2.0], [b"a1", 1.0]]
        )

        # custom score function
        response = await r.zrevrange("a", 0, 1, withscores=True, score_cast_func=int)
        assert_resp_response(
            r, response, [(b"a3", 3), (b"a2", 2)], [[b"a3", 3], [b"a2", 2]]
        )

        # custom score cast function
        # should be applied to resp2 and resp3
        # responses
        response = await r.zrevrange(
            "a", 0, 1, withscores=True, score_cast_func=safe_str
        )
        assert_resp_response(
            r,
            response,
            [(b"a3", "3"), (b"a2", "2")],
            [[b"a3", "3.0"], [b"a2", "2.0"]],
        )

    async def test_zrevrangebyscore(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert await r.zrevrangebyscore("a", 4, 2) == [b"a4", b"a3", b"a2"]

        # slicing with start/num
        assert await r.zrevrangebyscore("a", 4, 2, start=1, num=2) == [b"a3", b"a2"]

        # withscores
        response = await r.zrevrangebyscore("a", 4, 2, withscores=True)
        assert_resp_response(
            r,
            response,
            [(b"a4", 4.0), (b"a3", 3.0), (b"a2", 2.0)],
            [[b"a4", 4.0], [b"a3", 3.0], [b"a2", 2.0]],
        )

        # custom score function
        response = await r.zrevrangebyscore(
            "a", 4, 2, withscores=True, score_cast_func=int
        )
        assert_resp_response(
            r,
            response,
            [(b"a4", 4), (b"a3", 3), (b"a2", 2)],
            [[b"a4", 4], [b"a3", 3], [b"a2", 2]],
        )

    async def test_zrevrank(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert await r.zrevrank("a", "a1") == 4
        assert await r.zrevrank("a", "a2") == 3
        assert await r.zrevrank("a", "a6") is None

    @skip_if_server_version_lt("7.2.0")
    async def test_zrevrank_withscore(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 2, "a3": 3, "a4": 4, "a5": 5})
        assert await r.zrevrank("a", "a1") == 4
        assert await r.zrevrank("a", "a2") == 3
        assert await r.zrevrank("a", "a6") is None
        assert_resp_response(
            r, await r.zrevrank("a", "a3", withscore=True), [2, 3.0], [2, 3.0]
        )
        assert await r.zrevrank("a", "a6", withscore=True) is None

    async def test_zscore(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        assert await r.zscore("a", "a1") == 1.0
        assert await r.zscore("a", "a2") == 2.0
        assert await r.zscore("a", "a4") is None

    @pytest.mark.onlynoncluster
    async def test_zunionstore_sum(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert await r.zunionstore("d", ["a", "b", "c"]) == 4
        response = await r.zrange("d", 0, -1, withscores=True)
        assert_resp_response(
            r,
            response,
            [(b"a2", 3.0), (b"a4", 4.0), (b"a3", 8.0), (b"a1", 9.0)],
            [[b"a2", 3.0], [b"a4", 4.0], [b"a3", 8.0], [b"a1", 9.0]],
        )

    @pytest.mark.onlynoncluster
    async def test_zunionstore_max(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert await r.zunionstore("d", ["a", "b", "c"], aggregate="MAX") == 4
        respponse = await r.zrange("d", 0, -1, withscores=True)
        assert_resp_response(
            r,
            respponse,
            [(b"a2", 2.0), (b"a4", 4.0), (b"a3", 5.0), (b"a1", 6.0)],
            [[b"a2", 2.0], [b"a4", 4.0], [b"a3", 5.0], [b"a1", 6.0]],
        )

    @pytest.mark.onlynoncluster
    async def test_zunionstore_min(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 2, "a3": 3})
        await r.zadd("b", {"a1": 2, "a2": 2, "a3": 4})
        await r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert await r.zunionstore("d", ["a", "b", "c"], aggregate="MIN") == 4
        response = await r.zrange("d", 0, -1, withscores=True)
        assert_resp_response(
            r,
            response,
            [(b"a1", 1.0), (b"a2", 2.0), (b"a3", 3.0), (b"a4", 4.0)],
            [[b"a1", 1.0], [b"a2", 2.0], [b"a3", 3.0], [b"a4", 4.0]],
        )

    @pytest.mark.onlynoncluster
    async def test_zunionstore_with_weight(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert await r.zunionstore("d", {"a": 1, "b": 2, "c": 3}) == 4
        response = await r.zrange("d", 0, -1, withscores=True)
        assert_resp_response(
            r,
            response,
            [(b"a2", 5.0), (b"a4", 12.0), (b"a3", 20.0), (b"a1", 23.0)],
            [[b"a2", 5.0], [b"a4", 12.0], [b"a3", 20.0], [b"a1", 23.0]],
        )

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.7.0")
    async def test_zunionstore_count(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert await r.zunionstore("d", ["a", "b", "c"], aggregate="COUNT") == 4
        response = await r.zrange("d", 0, -1, withscores=True)
        assert_resp_response(
            r,
            response,
            [(b"a4", 1), (b"a2", 2), (b"a1", 3), (b"a3", 3)],
            [[b"a4", 1.0], [b"a2", 2.0], [b"a1", 3.0], [b"a3", 3.0]],
        )

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("8.7.0")
    async def test_zunionstore_count_with_weight(self, r: redis.Redis):
        await r.zadd("a", {"a1": 1, "a2": 1, "a3": 1})
        await r.zadd("b", {"a1": 2, "a2": 2, "a3": 2})
        await r.zadd("c", {"a1": 6, "a3": 5, "a4": 4})
        assert (
            await r.zunionstore("d", {"a": 1, "b": 2, "c": 3}, aggregate="COUNT") == 4
        )
        response = await r.zrange("d", 0, -1, withscores=True)
        assert_resp_response(
            r,
            response,
            [(b"a2", 3), (b"a4", 3), (b"a1", 6), (b"a3", 6)],
            [[b"a2", 3.0], [b"a4", 3.0], [b"a1", 6.0], [b"a3", 6.0]],
        )

    # HYPERLOGLOG TESTS
    @skip_if_server_version_lt("2.8.9")
    async def test_pfadd(self, r: redis.Redis):
        members = {b"1", b"2", b"3"}
        assert await r.pfadd("a", *members) == 1
        assert await r.pfadd("a", *members) == 0
        assert await r.pfcount("a") == len(members)

    @skip_if_server_version_lt("2.8.9")
    @pytest.mark.onlynoncluster
    async def test_pfcount(self, r: redis.Redis):
        members = {b"1", b"2", b"3"}
        await r.pfadd("a", *members)
        assert await r.pfcount("a") == len(members)
        members_b = {b"2", b"3", b"4"}
        await r.pfadd("b", *members_b)
        assert await r.pfcount("b") == len(members_b)
        assert await r.pfcount("a", "b") == len(members_b.union(members))

    @skip_if_server_version_lt("2.8.9")
    @pytest.mark.onlynoncluster
    async def test_pfmerge(self, r: redis.Redis):
        mema = {b"1", b"2", b"3"}
        memb = {b"2", b"3", b"4"}
        memc = {b"5", b"6", b"7"}
        await r.pfadd("a", *mema)
        await r.pfadd("b", *memb)
        await r.pfadd("c", *memc)
        await r.pfmerge("d", "c", "a")
        assert await r.pfcount("d") == 6
        await r.pfmerge("d", "b")
        assert await r.pfcount("d") == 7

    # HASH COMMANDS
    async def test_hget_and_hset(self, r: redis.Redis):
        await r.hset("a", mapping={"1": 1, "2": 2, "3": 3})
        assert await r.hget("a", "1") == b"1"
        assert await r.hget("a", "2") == b"2"
        assert await r.hget("a", "3") == b"3"

        # field was updated, redis returns 0
        assert await r.hset("a", "2", 5) == 0
        assert await r.hget("a", "2") == b"5"

        # field is new, redis returns 1
        assert await r.hset("a", "4", 4) == 1
        assert await r.hget("a", "4") == b"4"

        # key inside of hash that doesn't exist returns null value
        assert await r.hget("a", "b") is None

        # keys with bool(key) == False
        assert await r.hset("a", 0, 10) == 1
        assert await r.hset("a", "", 10) == 1

    async def test_hgetex_zero_expiry_options_are_mutually_exclusive(
        self, r: redis.Redis
    ):
        with pytest.raises(DataError):
            await r.hgetex("h", "f", ex=0, px=1)

    async def test_hsetex_zero_expiry_options_are_mutually_exclusive(
        self, r: redis.Redis
    ):
        with pytest.raises(DataError):
            await r.hsetex("h", "f", "v", ex=0, px=1)

    async def test_hget_and_hset_with_encodable_fields_and_values(self, r: redis.Redis):
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
            assert await r.hset("encodable-hash", field, value) == 1
            assert await r.hget("encodable-hash", field) == expected
            assert await r.hmget("encodable-hash", field) == [expected]

        assert await r.hmget("encodable-hash", bytearray(b"field-bytes")) == [
            b"value-bytes"
        ]

    async def test_hset_with_multi_key_values(self, r: redis.Redis):
        await r.hset("a", mapping={"1": 1, "2": 2, "3": 3})
        assert await r.hget("a", "1") == b"1"
        assert await r.hget("a", "2") == b"2"
        assert await r.hget("a", "3") == b"3"

        await r.hset("b", "foo", "bar", mapping={"1": 1, "2": 2})
        assert await r.hget("b", "1") == b"1"
        assert await r.hget("b", "2") == b"2"
        assert await r.hget("b", "foo") == b"bar"

    async def test_hset_without_data(self, r: redis.Redis):
        with pytest.raises(exceptions.DataError):
            await r.hset("x")

    async def test_hdel(self, r: redis.Redis):
        await r.hset("a", mapping={"1": 1, "2": 2, "3": 3})
        assert await r.hdel("a", "2") == 1
        assert await r.hget("a", "2") is None
        assert await r.hdel("a", "1", "3") == 2
        assert await r.hlen("a") == 0

    async def test_hexists(self, r: redis.Redis):
        await r.hset("a", mapping={"1": 1, "2": 2, "3": 3})
        assert await r.hexists("a", "1")
        assert not await r.hexists("a", "4")

    async def test_hgetall(self, r: redis.Redis):
        h = {b"a1": b"1", b"a2": b"2", b"a3": b"3"}
        await r.hset("a", mapping=h)
        assert await r.hgetall("a") == h

    async def test_hincrby(self, r: redis.Redis):
        assert await r.hincrby("a", "1") == 1
        assert await r.hincrby("a", "1", amount=2) == 3
        assert await r.hincrby("a", "1", amount=-2) == 1

    @skip_if_server_version_lt("2.6.0")
    async def test_hincrbyfloat(self, r: redis.Redis):
        assert await r.hincrbyfloat("a", "1") == 1.0
        assert await r.hincrbyfloat("a", "1") == 2.0
        assert await r.hincrbyfloat("a", "1", 1.2) == 3.2

    async def test_hkeys(self, r: redis.Redis):
        h = {b"a1": b"1", b"a2": b"2", b"a3": b"3"}
        await r.hset("a", mapping=h)
        local_keys = list(h.keys())
        remote_keys = await r.hkeys("a")
        assert sorted(local_keys) == sorted(remote_keys)

    async def test_hlen(self, r: redis.Redis):
        await r.hset("a", mapping={"1": 1, "2": 2, "3": 3})
        assert await r.hlen("a") == 3

    async def test_hmget(self, r: redis.Redis):
        assert await r.hset("a", mapping={"a": 1, "b": 2, "c": 3})
        assert await r.hmget("a", "a", "b", "c") == [b"1", b"2", b"3"]

    async def test_hmset(self, r: redis.Redis):
        h = {b"a": b"1", b"b": b"2", b"c": b"3"}
        assert await r.hmset("a", h)
        assert await r.hgetall("a") == h

    async def test_hsetnx(self, r: redis.Redis):
        # Initially set the hash field
        assert await r.hsetnx("a", "1", 1)
        assert await r.hget("a", "1") == b"1"
        assert not await r.hsetnx("a", "1", 2)
        assert await r.hget("a", "1") == b"1"

    async def test_hvals(self, r: redis.Redis):
        h = {b"a1": b"1", b"a2": b"2", b"a3": b"3"}
        await r.hset("a", mapping=h)
        local_vals = list(h.values())
        remote_vals = await r.hvals("a")
        assert sorted(local_vals) == sorted(remote_vals)

    @skip_if_server_version_lt("3.2.0")
    async def test_hstrlen(self, r: redis.Redis):
        await r.hset("a", mapping={"1": "22", "2": "333"})
        assert await r.hstrlen("a", "1") == 2
        assert await r.hstrlen("a", "2") == 3

    # SORT
    async def test_sort_basic(self, r: redis.Redis):
        await r.rpush("a", "3", "2", "1", "4")
        assert await r.sort("a") == [b"1", b"2", b"3", b"4"]

    async def test_sort_limited(self, r: redis.Redis):
        await r.rpush("a", "3", "2", "1", "4")
        assert await r.sort("a", start=1, num=2) == [b"2", b"3"]

    @pytest.mark.onlynoncluster
    async def test_sort_by(self, r: redis.Redis):
        await r.set("score:1", 8)
        await r.set("score:2", 3)
        await r.set("score:3", 5)
        await r.rpush("a", "3", "2", "1")
        assert await r.sort("a", by="score:*") == [b"2", b"3", b"1"]

    @pytest.mark.onlynoncluster
    async def test_sort_get(self, r: redis.Redis):
        await r.set("user:1", "u1")
        await r.set("user:2", "u2")
        await r.set("user:3", "u3")
        await r.rpush("a", "2", "3", "1")
        assert await r.sort("a", get="user:*") == [b"u1", b"u2", b"u3"]

    @pytest.mark.onlynoncluster
    async def test_sort_get_multi(self, r: redis.Redis):
        await r.set("user:1", "u1")
        await r.set("user:2", "u2")
        await r.set("user:3", "u3")
        await r.rpush("a", "2", "3", "1")
        assert await r.sort("a", get=("user:*", "#")) == [
            b"u1",
            b"1",
            b"u2",
            b"2",
            b"u3",
            b"3",
        ]

    @pytest.mark.onlynoncluster
    async def test_sort_get_groups_two(self, r: redis.Redis):
        await r.set("user:1", "u1")
        await r.set("user:2", "u2")
        await r.set("user:3", "u3")
        await r.rpush("a", "2", "3", "1")
        assert await r.sort("a", get=("user:*", "#"), groups=True) == [
            (b"u1", b"1"),
            (b"u2", b"2"),
            (b"u3", b"3"),
        ]

    @pytest.mark.onlynoncluster
    async def test_sort_groups_string_get(self, r: redis.Redis):
        await r.set("user:1", "u1")
        await r.set("user:2", "u2")
        await r.set("user:3", "u3")
        await r.rpush("a", "2", "3", "1")
        with pytest.raises(exceptions.DataError):
            await r.sort("a", get="user:*", groups=True)

    @pytest.mark.onlynoncluster
    async def test_sort_groups_just_one_get(self, r: redis.Redis):
        await r.set("user:1", "u1")
        await r.set("user:2", "u2")
        await r.set("user:3", "u3")
        await r.rpush("a", "2", "3", "1")
        with pytest.raises(exceptions.DataError):
            await r.sort("a", get=["user:*"], groups=True)

    async def test_sort_groups_no_get(self, r: redis.Redis):
        await r.set("user:1", "u1")
        await r.set("user:2", "u2")
        await r.set("user:3", "u3")
        await r.rpush("a", "2", "3", "1")
        with pytest.raises(exceptions.DataError):
            await r.sort("a", groups=True)

    @pytest.mark.onlynoncluster
    async def test_sort_groups_three_gets(self, r: redis.Redis):
        await r.set("user:1", "u1")
        await r.set("user:2", "u2")
        await r.set("user:3", "u3")
        await r.set("door:1", "d1")
        await r.set("door:2", "d2")
        await r.set("door:3", "d3")
        await r.rpush("a", "2", "3", "1")
        assert await r.sort("a", get=("user:*", "door:*", "#"), groups=True) == [
            (b"u1", b"d1", b"1"),
            (b"u2", b"d2", b"2"),
            (b"u3", b"d3", b"3"),
        ]

    async def test_sort_desc(self, r: redis.Redis):
        await r.rpush("a", "2", "3", "1")
        assert await r.sort("a", desc=True) == [b"3", b"2", b"1"]

    async def test_sort_alpha(self, r: redis.Redis):
        await r.rpush("a", "e", "c", "b", "d", "a")
        assert await r.sort("a", alpha=True) == [b"a", b"b", b"c", b"d", b"e"]

    @pytest.mark.onlynoncluster
    async def test_sort_store(self, r: redis.Redis):
        await r.rpush("a", "2", "3", "1")
        assert await r.sort("a", store="sorted_values") == 3
        assert await r.lrange("sorted_values", 0, -1) == [b"1", b"2", b"3"]

    @pytest.mark.onlynoncluster
    async def test_sort_all_options(self, r: redis.Redis):
        await r.set("user:1:username", "zeus")
        await r.set("user:2:username", "titan")
        await r.set("user:3:username", "hermes")
        await r.set("user:4:username", "hercules")
        await r.set("user:5:username", "apollo")
        await r.set("user:6:username", "athena")
        await r.set("user:7:username", "hades")
        await r.set("user:8:username", "dionysus")

        await r.set("user:1:favorite_drink", "yuengling")
        await r.set("user:2:favorite_drink", "rum")
        await r.set("user:3:favorite_drink", "vodka")
        await r.set("user:4:favorite_drink", "milk")
        await r.set("user:5:favorite_drink", "pinot noir")
        await r.set("user:6:favorite_drink", "water")
        await r.set("user:7:favorite_drink", "gin")
        await r.set("user:8:favorite_drink", "apple juice")

        await r.rpush("gods", "5", "8", "3", "1", "2", "7", "6", "4")
        num = await r.sort(
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
        assert await r.lrange("sorted", 0, 10) == [
            b"vodka",
            b"milk",
            b"gin",
            b"apple juice",
        ]

    async def test_sort_issue_924(self, r: redis.Redis):
        # Tests for issue https://github.com/andymccurdy/redis-py/issues/924
        await r.execute_command("SADD", "issue#924", 1)
        await r.execute_command("SORT", "issue#924")

    @pytest.mark.onlynoncluster
    async def test_cluster_addslots(self, mock_cluster_resp_ok):
        assert await mock_cluster_resp_ok.cluster("ADDSLOTS", 1) is True

    @pytest.mark.onlynoncluster
    async def test_cluster_count_failure_reports(self, mock_cluster_resp_int):
        assert isinstance(
            await mock_cluster_resp_int.cluster("COUNT-FAILURE-REPORTS", "node"), int
        )

    @pytest.mark.onlynoncluster
    async def test_cluster_countkeysinslot(self, mock_cluster_resp_int):
        assert isinstance(
            await mock_cluster_resp_int.cluster("COUNTKEYSINSLOT", 2), int
        )

    @pytest.mark.onlynoncluster
    async def test_cluster_delslots(self, mock_cluster_resp_ok):
        assert await mock_cluster_resp_ok.cluster("DELSLOTS", 1) is True

    @pytest.mark.onlynoncluster
    async def test_cluster_failover(self, mock_cluster_resp_ok):
        assert await mock_cluster_resp_ok.cluster("FAILOVER", 1) is True

    @pytest.mark.onlynoncluster
    async def test_cluster_forget(self, mock_cluster_resp_ok):
        assert await mock_cluster_resp_ok.cluster("FORGET", 1) is True

    @pytest.mark.onlynoncluster
    async def test_cluster_info(self, mock_cluster_resp_info):
        assert isinstance(await mock_cluster_resp_info.cluster("info"), dict)

    @pytest.mark.onlynoncluster
    async def test_cluster_keyslot(self, mock_cluster_resp_int):
        assert isinstance(await mock_cluster_resp_int.cluster("keyslot", "asdf"), int)

    @pytest.mark.onlynoncluster
    async def test_cluster_meet(self, mock_cluster_resp_ok):
        assert await mock_cluster_resp_ok.cluster("meet", "ip", "port", 1) is True

    @pytest.mark.onlynoncluster
    async def test_cluster_nodes(self, mock_cluster_resp_nodes):
        assert isinstance(await mock_cluster_resp_nodes.cluster("nodes"), dict)

    @pytest.mark.onlynoncluster
    async def test_cluster_replicate(self, mock_cluster_resp_ok):
        assert await mock_cluster_resp_ok.cluster("replicate", "nodeid") is True

    @pytest.mark.onlynoncluster
    async def test_cluster_reset(self, mock_cluster_resp_ok):
        assert await mock_cluster_resp_ok.cluster("reset", "hard") is True

    @pytest.mark.onlynoncluster
    async def test_cluster_saveconfig(self, mock_cluster_resp_ok):
        assert await mock_cluster_resp_ok.cluster("saveconfig") is True

    @pytest.mark.onlynoncluster
    async def test_cluster_setslot(self, mock_cluster_resp_ok):
        assert (
            await mock_cluster_resp_ok.cluster("setslot", 1, "IMPORTING", "nodeid")
            is True
        )

    @pytest.mark.onlynoncluster
    async def test_cluster_slaves(self, mock_cluster_resp_slaves):
        assert isinstance(
            await mock_cluster_resp_slaves.cluster("slaves", "nodeid"), dict
        )

    @skip_if_server_version_lt("3.0.0")
    @skip_if_server_version_gte("7.0.0")
    @pytest.mark.onlynoncluster
    async def test_readwrite(self, r: redis.Redis):
        assert await r.readwrite()

    @skip_if_server_version_lt("3.0.0")
    @pytest.mark.onlynoncluster
    async def test_readonly_invalid_cluster_state(self, r: redis.Redis):
        with pytest.raises(exceptions.RedisError):
            await r.readonly()

    @skip_if_server_version_lt("3.0.0")
    @pytest.mark.onlynoncluster
    async def test_readonly(self, mock_cluster_resp_ok):
        assert await mock_cluster_resp_ok.readonly() is True

    # GEO COMMANDS
    @skip_if_server_version_lt("3.2.0")
    async def test_geoadd(self, r: redis.Redis):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        assert await r.geoadd("barcelona", values) == 2
        assert await r.zcard("barcelona") == 2

    @skip_if_server_version_lt("3.2.0")
    async def test_geoadd_invalid_params(self, r: redis.Redis):
        with pytest.raises(exceptions.RedisError):
            await r.geoadd("barcelona", (1, 2))

    @skip_if_server_version_lt("3.2.0")
    async def test_geodist(self, r: redis.Redis):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        assert await r.geoadd("barcelona", values) == 2
        assert await r.geodist("barcelona", "place1", "place2") == 3067.4157

    @skip_if_server_version_lt("3.2.0")
    async def test_geodist_units(self, r: redis.Redis):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        await r.geoadd("barcelona", values)
        assert await r.geodist("barcelona", "place1", "place2", "km") == 3.0674

    @skip_if_server_version_lt("3.2.0")
    async def test_geodist_missing_one_member(self, r: redis.Redis):
        values = (2.1909389952632, 41.433791470673, "place1")
        await r.geoadd("barcelona", values)
        assert await r.geodist("barcelona", "place1", "missing_member", "km") is None

    @skip_if_server_version_lt("3.2.0")
    async def test_geodist_invalid_units(self, r: redis.Redis):
        with pytest.raises(exceptions.RedisError):
            assert await r.geodist("x", "y", "z", "inches")

    @skip_if_server_version_lt("3.2.0")
    async def test_geohash(self, r: redis.Redis):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        await r.geoadd("barcelona", values)
        assert_resp_response(
            r,
            await r.geohash("barcelona", "place1", "place2", "place3"),
            ["sp3e9yg3kd0", "sp3e9cbc3t0", None],
            [b"sp3e9yg3kd0", b"sp3e9cbc3t0", None],
            ["sp3e9yg3kd0", "sp3e9cbc3t0", None],
        )

    @skip_if_server_version_lt("3.2.0")
    async def test_geopos(self, r: redis.Redis):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        await r.geoadd("barcelona", values)
        # redis uses 52 bits precision, hereby small errors may be introduced.
        assert_resp_response(
            r,
            await r.geopos("barcelona", "place1", "place2"),
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
    async def test_geopos_no_value(self, r: redis.Redis):
        assert await r.geopos("barcelona", "place1", "place2") == [None, None]

    @skip_if_server_version_lt("3.2.0")
    @skip_if_server_version_gte("4.0.0")
    async def test_old_geopos_no_value(self, r: redis.Redis):
        assert await r.geopos("barcelona", "place1", "place2") == []

    @skip_if_server_version_lt("3.2.0")
    async def test_georadius(self, r: redis.Redis):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            b"\x80place2",
        )

        await r.geoadd("barcelona", values)
        assert await r.georadius("barcelona", 2.191, 41.433, 1000) == [b"place1"]
        assert await r.georadius("barcelona", 2.187, 41.406, 1000) == [b"\x80place2"]

    @skip_if_server_version_lt("3.2.0")
    async def test_georadius_no_values(self, r: redis.Redis):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        await r.geoadd("barcelona", values)
        assert await r.georadius("barcelona", 1, 2, 1000) == []

    @skip_if_server_version_lt("3.2.0")
    async def test_georadius_units(self, r: redis.Redis):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        await r.geoadd("barcelona", values)
        assert await r.georadius("barcelona", 2.191, 41.433, 1, unit="km") == [
            b"place1"
        ]

    @skip_unless_arch_bits(64)
    @skip_if_server_version_lt("3.2.0")
    async def test_georadius_with(self, r: redis.Redis):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        await r.geoadd("barcelona", values)

        # test a bunch of combinations to test the parse response
        # function.
        assert await r.georadius(
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

        assert await r.georadius(
            "barcelona", 2.191, 41.433, 1, unit="km", withdist=True, withcoord=True
        ) == [[b"place1", 0.0881, (2.19093829393386841, 41.43379028184083523)]]

        assert await r.georadius(
            "barcelona", 2.191, 41.433, 1, unit="km", withhash=True, withcoord=True
        ) == [
            [b"place1", 3471609698139488, (2.19093829393386841, 41.43379028184083523)]
        ]

        # test no values.
        assert (
            await r.georadius(
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

    @skip_if_server_version_lt("3.2.0")
    async def test_georadius_count(self, r: redis.Redis):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        await r.geoadd("barcelona", values)
        assert await r.georadius("barcelona", 2.191, 41.433, 3000, count=1) == [
            b"place1"
        ]

    @skip_if_server_version_lt("3.2.0")
    async def test_georadius_sort(self, r: redis.Redis):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        await r.geoadd("barcelona", values)
        assert await r.georadius("barcelona", 2.191, 41.433, 3000, sort="ASC") == [
            b"place1",
            b"place2",
        ]
        assert await r.georadius("barcelona", 2.191, 41.433, 3000, sort="DESC") == [
            b"place2",
            b"place1",
        ]

    @skip_if_server_version_lt("3.2.0")
    @pytest.mark.onlynoncluster
    async def test_georadius_store(self, r: redis.Redis):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        await r.geoadd("barcelona", values)
        await r.georadius("barcelona", 2.191, 41.433, 1000, store="places_barcelona")
        assert await r.zrange("places_barcelona", 0, -1) == [b"place1"]

    @skip_unless_arch_bits(64)
    @skip_if_server_version_lt("3.2.0")
    @pytest.mark.onlynoncluster
    async def test_georadius_store_dist(self, r: redis.Redis):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            "place2",
        )

        await r.geoadd("barcelona", values)
        await r.georadius(
            "barcelona", 2.191, 41.433, 1000, store_dist="places_barcelona"
        )
        # instead of save the geo score, the distance is saved.
        assert await r.zscore("places_barcelona", "place1") == 88.05060698409301

    @skip_unless_arch_bits(64)
    @skip_if_server_version_lt("3.2.0")
    async def test_georadiusmember(self, r: redis.Redis):
        values = (2.1909389952632, 41.433791470673, "place1") + (
            2.1873744593677,
            41.406342043777,
            b"\x80place2",
        )

        await r.geoadd("barcelona", values)
        assert await r.georadiusbymember("barcelona", "place1", 4000) == [
            b"\x80place2",
            b"place1",
        ]
        assert await r.georadiusbymember("barcelona", "place1", 10) == [b"place1"]

        assert await r.georadiusbymember(
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

    @pytest.mark.fixed_client
    async def test_geosearch_forwards_zero_numeric_arguments(self):
        # Async mirror of TestRedisCommands.test_geosearch_forwards_zero_numeric_arguments.
        # GeoCommands is shared, but execute_command is awaited on this stack.
        client = redis.Redis()

        async def wire_args(**kwargs):
            with patch.object(
                client, "execute_command", new_callable=AsyncMock, return_value=[]
            ) as m:
                await client.geosearch("places", **kwargs)
                return list(m.call_args[0])

        origin_radius = await wire_args(longitude=0, latitude=0, radius=0, unit="m")
        assert origin_radius[2:5] == [b"FROMLONLAT", 0, 0]
        assert origin_radius[-3:] == [b"BYRADIUS", 0, "m"]

        member_zero = await wire_args(member=0, radius=10, unit="km")
        assert member_zero[2:4] == [b"FROMMEMBER", 0]
        assert member_zero[-3:] == [b"BYRADIUS", 10, "km"]

        box_zero_width = await wire_args(
            longitude=1, latitude=2, width=0, height=1, unit="m"
        )
        assert box_zero_width[-4:] == [b"BYBOX", 0, 1, "m"]

        count_zero = await wire_args(
            longitude=1, latitude=2, radius=10, count=0, unit="m"
        )
        assert count_zero[-2:] == [b"COUNT", 0]
        assert b"COUNT" not in await wire_args(
            longitude=1, latitude=2, radius=10, unit="m"
        )

        with pytest.raises(DataError):
            await client.geosearch(
                "places", member="Paris", longitude=0, latitude=0, radius=10
            )

    @skip_if_server_version_lt("5.0.0")
    async def test_xack(self, r: redis.Redis):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        # xack on a stream that doesn't exist
        assert await r.xack(stream, group, "0-0") == 0

        m1 = await r.xadd(stream, {"one": "one"})
        m2 = await r.xadd(stream, {"two": "two"})
        m3 = await r.xadd(stream, {"three": "three"})

        # xack on a group that doesn't exist
        assert await r.xack(stream, group, m1) == 0

        await r.xgroup_create(stream, group, 0)
        await r.xreadgroup(group, consumer, streams={stream: ">"})
        # xack returns the number of ack'd elements
        assert await r.xack(stream, group, m1) == 1
        assert await r.xack(stream, group, m2, m3) == 2

    @skip_if_server_version_lt("5.0.0")
    async def test_xadd(self, r: redis.Redis):
        stream = "stream"
        message_id = await r.xadd(stream, {"foo": "bar"})
        assert re.match(rb"[0-9]+\-[0-9]+", message_id)

        # explicit message id
        message_id = b"9999999999999999999-0"
        assert message_id == await r.xadd(stream, {"foo": "bar"}, id=message_id)

        # with maxlen, the list evicts the first message
        await r.xadd(stream, {"foo": "bar"}, maxlen=2, approximate=False)
        assert await r.xlen(stream) == 2

    @skip_if_server_version_lt("5.0.0")
    async def test_xclaim(self, r: redis.Redis):
        stream = "stream"
        group = "group"
        consumer1 = "consumer1"
        consumer2 = "consumer2"

        message_id = await r.xadd(stream, {"john": "wick"})
        message = await get_stream_message(r, stream, message_id)
        await r.xgroup_create(stream, group, 0)

        # trying to claim a message that isn't already pending doesn't
        # do anything
        response = await r.xclaim(
            stream, group, consumer2, min_idle_time=0, message_ids=(message_id,)
        )
        assert response == []

        # read the group as consumer1 to initially claim the messages
        await r.xreadgroup(group, consumer1, streams={stream: ">"})

        # claim the message as consumer2
        response = await r.xclaim(
            stream, group, consumer2, min_idle_time=0, message_ids=(message_id,)
        )
        assert response[0] == message

        # reclaim the message as consumer1, but use the justid argument
        # which only returns message ids
        assert await r.xclaim(
            stream,
            group,
            consumer1,
            min_idle_time=0,
            message_ids=(message_id,),
            justid=True,
        ) == [message_id]

    @skip_if_server_version_lt("7.0.0")
    async def test_xclaim_trimmed(self, r: redis.Redis):
        # xclaim should not raise an exception if the item is not there
        stream = "stream"
        group = "group"

        await r.xgroup_create(stream, group, id="$", mkstream=True)

        # add a couple of new items
        sid1 = await r.xadd(stream, {"item": 0})
        sid2 = await r.xadd(stream, {"item": 0})

        # read them from consumer1
        await r.xreadgroup(group, "consumer1", {stream: ">"})

        # add a 3rd and trim the stream down to 2 items
        await r.xadd(stream, {"item": 3}, maxlen=2, approximate=False)

        # xclaim them from consumer2
        # the item that is still in the stream should be returned
        item = await r.xclaim(stream, group, "consumer2", 0, [sid1, sid2])
        assert len(item) == 1
        assert item[0][0] == sid2

    @skip_if_server_version_lt("5.0.0")
    async def test_xdel(self, r: redis.Redis):
        stream = "stream"

        # deleting from an empty stream doesn't do anything
        assert await r.xdel(stream, 1) == 0

        m1 = await r.xadd(stream, {"foo": "bar"})
        m2 = await r.xadd(stream, {"foo": "bar"})
        m3 = await r.xadd(stream, {"foo": "bar"})

        # xdel returns the number of deleted elements
        assert await r.xdel(stream, m1) == 1
        assert await r.xdel(stream, m2, m3) == 2

    @skip_if_server_version_lt("7.0.0")
    async def test_xgroup_create(self, r: redis.Redis):
        # tests xgroup_create and xinfo_groups
        stream = "stream"
        group = "group"
        await r.xadd(stream, {"foo": "bar"})

        # no group is setup yet, no info to obtain
        assert await r.xinfo_groups(stream) == []

        assert await r.xgroup_create(stream, group, 0)
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
        assert await r.xinfo_groups(stream) == expected

    @skip_if_server_version_lt("7.0.0")
    async def test_xgroup_create_mkstream(self, r: redis.Redis):
        # tests xgroup_create and xinfo_groups
        stream = "stream"
        group = "group"

        # an error is raised if a group is created on a stream that
        # doesn't already exist
        with pytest.raises(exceptions.ResponseError):
            await r.xgroup_create(stream, group, 0)

        # however, with mkstream=True, the underlying stream is created
        # automatically
        assert await r.xgroup_create(stream, group, 0, mkstream=True)
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
        assert await r.xinfo_groups(stream) == expected

    @skip_if_server_version_lt("5.0.0")
    async def test_xgroup_delconsumer(self, r: redis.Redis):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        await r.xadd(stream, {"foo": "bar"})
        await r.xadd(stream, {"foo": "bar"})
        await r.xgroup_create(stream, group, 0)

        # a consumer that hasn't yet read any messages doesn't do anything
        assert await r.xgroup_delconsumer(stream, group, consumer) == 0

        # read all messages from the group
        await r.xreadgroup(group, consumer, streams={stream: ">"})

        # deleting the consumer should return 2 pending messages
        assert await r.xgroup_delconsumer(stream, group, consumer) == 2

    @skip_if_server_version_lt("5.0.0")
    async def test_xgroup_destroy(self, r: redis.Redis):
        stream = "stream"
        group = "group"
        await r.xadd(stream, {"foo": "bar"})

        # destroying a nonexistent group returns False
        assert not await r.xgroup_destroy(stream, group)

        await r.xgroup_create(stream, group, 0)
        assert await r.xgroup_destroy(stream, group)

    @skip_if_server_version_lt("7.0.0")
    @skip_if_server_version_gte("8.2.2")
    async def test_xgroup_setid(self, r: redis.Redis):
        stream = "stream"
        group = "group"
        message_id = await r.xadd(stream, {"foo": "bar"})

        await r.xgroup_create(stream, group, 0)
        # advance the last_delivered_id to the message_id
        await r.xgroup_setid(stream, group, message_id, entries_read=2)
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
        assert await r.xinfo_groups(stream) == expected

    @skip_if_server_version_lt("8.2.2")
    async def test_xgroup_setid_fixed_max_entries_read(self, r):
        stream = "stream"
        group = "group"
        message_id = await r.xadd(stream, {"foo": "bar"})
        await r.xadd(stream, {"foo1": "bar1"})

        await r.xgroup_create(stream, group, 0)
        # advance the last_delivered_id to the message_id
        await r.xgroup_setid(stream, group, message_id, entries_read=2)
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
        assert await r.xinfo_groups(stream) == expected

    @skip_if_server_version_lt("7.2.0")
    async def test_xinfo_consumers(self, r: redis.Redis):
        stream = "stream"
        group = "group"
        consumer1 = "consumer1"
        consumer2 = "consumer2"
        await r.xadd(stream, {"foo": "bar"})
        await r.xadd(stream, {"foo": "bar"})
        await r.xadd(stream, {"foo": "bar"})

        await r.xgroup_create(stream, group, 0)
        await r.xreadgroup(group, consumer1, streams={stream: ">"}, count=1)
        await r.xreadgroup(group, consumer2, streams={stream: ">"})
        info = await r.xinfo_consumers(stream, group)
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

    @skip_if_server_version_lt("5.0.0")
    async def test_xinfo_stream(self, r: redis.Redis):
        stream = "stream"
        m1 = await r.xadd(stream, {"foo": "bar"})
        m2 = await r.xadd(stream, {"foo": "bar"})
        info = await r.xinfo_stream(stream)

        assert info["length"] == 2
        assert info["first-entry"] == await get_stream_message(r, stream, m1)
        assert info["last-entry"] == await get_stream_message(r, stream, m2)

        await r.xtrim(stream, 0)
        info = await r.xinfo_stream(stream)
        assert info["length"] == 0
        assert info["first-entry"] is None
        assert info["last-entry"] is None

    @skip_if_server_version_lt("6.0.0")
    async def test_xinfo_stream_full(self, r: redis.Redis):
        stream = "stream"
        group = "group"

        await r.xadd(stream, {"foo": "bar"})
        info = await r.xinfo_stream(stream, full=True)
        assert info["length"] == 1
        assert len(info["groups"]) == 0

        await r.xgroup_create(stream, group, 0)
        info = await r.xinfo_stream(stream, full=True)
        assert info["length"] == 1

        await r.xreadgroup(group, "consumer", streams={stream: ">"})
        info = await r.xinfo_stream(stream, full=True)
        consumer = info["groups"][0]["consumers"][0]
        assert isinstance(consumer, dict)
        assert consumer["name"] == b"consumer"
        assert consumer["pel-count"] == 1

    @skip_if_server_version_lt("8.5.0")
    async def test_xinfo_stream_idempotent_fields(self, r: redis.Redis):
        stream = "stream"

        # Create stream with regular entry
        await r.xadd(stream, {"foo": "bar"})
        info = await r.xinfo_stream(stream)

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
        await r.xadd(stream, {"field1": "value1"}, idmpauto="producer1")
        info = await r.xinfo_stream(stream)

        # After adding idempotent entry
        assert info["pids-tracked"] == 1  # One producer tracked
        assert info["iids-tracked"] == 1  # One iid tracked
        assert info["iids-added"] == 1  # One idempotent entry added
        assert info["iids-duplicates"] == 0  # No duplicates yet

        # Add duplicate entry
        await r.xadd(stream, {"field1": "value1"}, idmpauto="producer1")
        info = await r.xinfo_stream(stream)

        # After duplicate
        assert info["pids-tracked"] == 1  # Still one producer
        assert info["iids-tracked"] == 1  # Still one iid (duplicate doesn't add new)
        assert info["iids-added"] == 1  # Still one unique entry
        assert info["iids-duplicates"] == 1  # One duplicate detected

        # Add entry from different producer
        await r.xadd(stream, {"field2": "value2"}, idmpauto="producer2")
        info = await r.xinfo_stream(stream)

        # After second producer
        assert info["pids-tracked"] == 2  # Two producers tracked
        assert info["iids-tracked"] == 2  # Two iids tracked
        assert info["iids-added"] == 2  # Two unique entries
        assert info["iids-duplicates"] == 1  # Still one duplicate

    @skip_if_server_version_lt("5.0.0")
    async def test_xlen(self, r: redis.Redis):
        stream = "stream"
        assert await r.xlen(stream) == 0
        await r.xadd(stream, {"foo": "bar"})
        await r.xadd(stream, {"foo": "bar"})
        assert await r.xlen(stream) == 2

    @skip_if_server_version_lt("8.7.2")
    async def test_xnack_silent(self, r: ClientT):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        m1 = await r.xadd(stream, {"foo": "bar"})
        m2 = await r.xadd(stream, {"foo": "bar"})
        await r.xgroup_create(stream, group, 0)
        await r.xreadgroup(group, consumer, streams={stream: ">"})
        result = await r.xnack(stream, group, "SILENT", m1, m2)
        assert result == 2

    @skip_if_server_version_lt("8.7.2")
    async def test_xnack_fail(self, r: ClientT):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        m1 = await r.xadd(stream, {"foo": "bar"})
        await r.xgroup_create(stream, group, 0)
        await r.xreadgroup(group, consumer, streams={stream: ">"})
        result = await r.xnack(stream, group, "FAIL", m1)
        assert result == 1

    @skip_if_server_version_lt("8.7.2")
    async def test_xnack_fatal(self, r: ClientT):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        m1 = await r.xadd(stream, {"foo": "bar"})
        await r.xgroup_create(stream, group, 0)
        await r.xreadgroup(group, consumer, streams={stream: ">"})
        result = await r.xnack(stream, group, "FATAL", m1)
        assert result == 1

    @skip_if_server_version_lt("8.7.2")
    async def test_xnack_multiple_ids(self, r: ClientT):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        m1 = await r.xadd(stream, {"foo": "bar"})
        m2 = await r.xadd(stream, {"foo": "bar"})
        m3 = await r.xadd(stream, {"foo": "bar"})
        await r.xgroup_create(stream, group, 0)
        await r.xreadgroup(group, consumer, streams={stream: ">"})
        result = await r.xnack(stream, group, "FAIL", m1, m2, m3)
        assert result == 3

    @skip_if_server_version_lt("8.7.2")
    async def test_xnack_some_ids_not_in_pel(self, r: ClientT):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        m1 = await r.xadd(stream, {"foo": "bar"})
        m2 = await r.xadd(stream, {"foo": "bar"})
        await r.xgroup_create(stream, group, 0)
        await r.xreadgroup(group, consumer, streams={stream: ">"})
        result = await r.xnack(stream, group, "FAIL", m1, m2, "999999-0")
        assert result == 2

    @skip_if_server_version_lt("8.7.2")
    async def test_xnack_retrycount(self, r: ClientT):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        m1 = await r.xadd(stream, {"foo": "bar"})
        await r.xgroup_create(stream, group, 0)
        await r.xreadgroup(group, consumer, streams={stream: ">"})
        result = await r.xnack(stream, group, "FAIL", m1, retrycount=5)
        assert result == 1

    @skip_if_server_version_lt("8.7.2")
    async def test_xnack_force(self, r: ClientT):
        stream = "stream"
        group = "group"
        m1 = await r.xadd(stream, {"foo": "bar"})
        await r.xgroup_create(stream, group, 0)
        result = await r.xnack(stream, group, "FAIL", m1, force=True)
        assert result == 1

    @skip_if_server_version_lt("8.7.2")
    async def test_xnack_invalid_mode(self, r: ClientT):
        stream = "stream"
        group = "group"
        m1 = await r.xadd(stream, {"foo": "bar"})
        await r.xgroup_create(stream, group, 0)
        with pytest.raises(redis.DataError):
            await r.xnack(stream, group, "INVALID", m1)

    @skip_if_server_version_lt("8.7.2")
    async def test_xnack_no_ids(self, r: ClientT):
        stream = "stream"
        group = "group"
        await r.xadd(stream, {"foo": "bar"})
        await r.xgroup_create(stream, group, 0)
        with pytest.raises(redis.DataError):
            await r.xnack(stream, group, "FAIL")

    @skip_if_server_version_lt("8.7.2")
    async def test_xnack_negative_retrycount(self, r: ClientT):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        m1 = await r.xadd(stream, {"foo": "bar"})
        await r.xgroup_create(stream, group, 0)
        await r.xreadgroup(group, consumer, streams={stream: ">"})
        with pytest.raises(redis.DataError):
            await r.xnack(stream, group, "FAIL", m1, retrycount=-1)

    @skip_if_server_version_lt("5.0.0")
    async def test_xpending(self, r: redis.Redis):
        stream = "stream"
        group = "group"
        consumer1 = "consumer1"
        consumer2 = "consumer2"
        m1 = await r.xadd(stream, {"foo": "bar"})
        m2 = await r.xadd(stream, {"foo": "bar"})
        await r.xgroup_create(stream, group, 0)

        # xpending on a group that has no consumers yet
        expected = {"pending": 0, "min": None, "max": None, "consumers": []}
        assert await r.xpending(stream, group) == expected

        # read 1 message from the group with each consumer
        await r.xreadgroup(group, consumer1, streams={stream: ">"}, count=1)
        await r.xreadgroup(group, consumer2, streams={stream: ">"}, count=1)

        expected = {
            "pending": 2,
            "min": m1,
            "max": m2,
            "consumers": [
                {"name": consumer1.encode(), "pending": 1},
                {"name": consumer2.encode(), "pending": 1},
            ],
        }
        assert await r.xpending(stream, group) == expected

    @skip_if_server_version_lt("5.0.0")
    async def test_xpending_range(self, r: redis.Redis):
        stream = "stream"
        group = "group"
        consumer1 = "consumer1"
        consumer2 = "consumer2"
        m1 = await r.xadd(stream, {"foo": "bar"})
        m2 = await r.xadd(stream, {"foo": "bar"})
        await r.xgroup_create(stream, group, 0)

        # xpending range on a group that has no consumers yet
        assert await r.xpending_range(stream, group, min="-", max="+", count=5) == []

        # read 1 message from the group with each consumer
        await r.xreadgroup(group, consumer1, streams={stream: ">"}, count=1)
        await r.xreadgroup(group, consumer2, streams={stream: ">"}, count=1)

        response = await r.xpending_range(stream, group, min="-", max="+", count=5)
        assert len(response) == 2
        assert response[0]["message_id"] == m1
        assert response[0]["consumer"] == consumer1.encode()
        assert response[1]["message_id"] == m2
        assert response[1]["consumer"] == consumer2.encode()

    @skip_if_server_version_lt("5.0.0")
    @pytest.mark.parametrize("consumer", ["", b"", "consumer1", b"consumer1"])
    async def test_xpending_range_consumer_filter(self, r, consumer):
        stream, group = "stream", "group"
        first = await r.xadd(stream, {"foo": "bar"})
        second = await r.xadd(stream, {"foo": "baz"})
        await r.xgroup_create(stream, group, 0)
        await r.xreadgroup(group, consumer, streams={stream: ">"}, count=1)
        await r.xreadgroup(group, "other", streams={stream: ">"}, count=1)

        pending = await r.xpending_range(stream, group, "-", "+", 5)
        assert [item["message_id"] for item in pending] == [first, second]

        filtered = await r.xpending_range(
            stream, group, "-", "+", 5, consumername=consumer
        )
        assert [item["message_id"] for item in filtered] == [first]
        expected_consumer = consumer.encode() if isinstance(consumer, str) else consumer
        assert filtered[0]["consumer"] == expected_consumer

    @skip_if_server_version_lt("5.0.0")
    async def test_xrange(self, r: redis.Redis):
        stream = "stream"
        m1 = await r.xadd(stream, {"foo": "bar"})
        m2 = await r.xadd(stream, {"foo": "bar"})
        m3 = await r.xadd(stream, {"foo": "bar"})
        m4 = await r.xadd(stream, {"foo": "bar"})

        def get_ids(results):
            return [result[0] for result in results]

        results = await r.xrange(stream, min=m1)
        assert get_ids(results) == [m1, m2, m3, m4]

        results = await r.xrange(stream, min=m2, max=m3)
        assert get_ids(results) == [m2, m3]

        results = await r.xrange(stream, max=m3)
        assert get_ids(results) == [m1, m2, m3]

        results = await r.xrange(stream, max=m2, count=1)
        assert get_ids(results) == [m1]

    @skip_if_server_version_lt("5.0.0")
    async def test_xread(self, r: redis.Redis):
        stream = "stream"
        m1 = await r.xadd(stream, {"foo": "bar"})
        m2 = await r.xadd(stream, {"bing": "baz"})

        strem_name = stream.encode()
        expected_entries = [
            await get_stream_message(r, stream, m1),
            await get_stream_message(r, stream, m2),
        ]
        # xread starting at 0 returns both messages
        res = await r.xread(streams={stream: 0})
        assert_resp_response(
            r,
            res,
            [[strem_name, expected_entries]],
            {strem_name: [expected_entries]},
            {strem_name: expected_entries},
        )

        expected_entries = [await get_stream_message(r, stream, m1)]
        # xread starting at 0 and count=1 returns only the first message
        res = await r.xread(streams={stream: 0}, count=1)
        assert_resp_response(
            r,
            res,
            [[strem_name, expected_entries]],
            {strem_name: [expected_entries]},
            {strem_name: expected_entries},
        )

        expected_entries = [await get_stream_message(r, stream, m2)]
        # xread starting at m1 returns only the second message
        res = await r.xread(streams={stream: m1})
        assert_resp_response(
            r,
            res,
            [[strem_name, expected_entries]],
            {strem_name: [expected_entries]},
            {strem_name: expected_entries},
        )

    def _total_stream_entries(self, r, response):
        """Count entries across all streams regardless of response shape."""
        shape = expected_response_shape(r)
        if shape == "legacy_resp2":
            return sum(len(item[1]) for item in response)
        elif shape == "legacy_resp3":
            return sum(len(entries[0]) for entries in response.values())
        else:
            return sum(len(entries) for entries in response.values())

    async def test_xread_max_count_max_size_validation(self, r: redis.Redis):
        with pytest.raises(DataError):
            await r.xread(streams={"stream": 0}, max_count=0)
        with pytest.raises(DataError):
            await r.xread(streams={"stream": 0}, max_count=-1)
        with pytest.raises(DataError):
            await r.xread(streams={"stream": 0}, max_size=0)
        # max_count must be >= count when both are provided
        with pytest.raises(DataError):
            await r.xread(streams={"stream": 0}, count=5, max_count=3)

    @skip_if_server_version_lt("8.9.0")
    async def test_xread_with_max_count(self, r: redis.Redis):
        stream = "stream"
        for i in range(5):
            await r.xadd(stream, {"f": i})

        # max_count caps the total number of returned entries
        res = await r.xread(streams={stream: 0}, max_count=2)
        assert self._total_stream_entries(r, res) == 2

    @skip_if_server_version_lt("8.9.0")
    async def test_xread_with_max_size(self, r: redis.Redis):
        stream = "stream"
        await r.xadd(stream, {"f": "v"})

        # a generous soft cap returns the available entry
        res = await r.xread(streams={stream: 0}, max_size=65536)
        assert self._total_stream_entries(r, res) == 1

        # max_size is a soft cap: a single available entry that exceeds the cap
        # is still returned rather than suppressed
        res = await r.xread(streams={stream: 0}, max_size=1)
        assert self._total_stream_entries(r, res) == 1

    @skip_if_server_version_lt("8.9.0")
    async def test_xread_max_count_cumulative_across_streams(self, r: redis.Redis):
        stream_1 = "stream1:{maxcount}"
        stream_2 = "stream2:{maxcount}"
        for i in range(3):
            await r.xadd(stream_1, {"f": i})
        for i in range(3):
            await r.xadd(stream_2, {"f": i})

        # count is per-stream (up to 3 each), max_count caps the cumulative
        # reply; streams are served in caller order, so stream_1 contributes 3
        # and stream_2 contributes 1
        res = await r.xread(streams={stream_1: 0, stream_2: 0}, count=3, max_count=4)
        assert self._total_stream_entries(r, res) == 4

    async def test_xreadgroup_max_count_max_size_validation(self, r: redis.Redis):
        with pytest.raises(DataError):
            await r.xreadgroup("g", "c", streams={"stream": ">"}, max_count=0)
        with pytest.raises(DataError):
            await r.xreadgroup("g", "c", streams={"stream": ">"}, max_size=-1)
        with pytest.raises(DataError):
            await r.xreadgroup("g", "c", streams={"stream": ">"}, count=5, max_count=3)

    @skip_if_server_version_lt("8.9.0")
    async def test_xreadgroup_with_max_count(self, r: redis.Redis):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        for i in range(5):
            await r.xadd(stream, {"f": i})
        await r.xgroup_create(stream, group, 0)

        # max_count caps the total number of returned entries
        res = await r.xreadgroup(group, consumer, streams={stream: ">"}, max_count=2)
        assert self._total_stream_entries(r, res) == 2
        await r.xgroup_destroy(stream, group)

    @skip_if_server_version_lt("8.9.0")
    async def test_xreadgroup_max_count_cumulative_across_streams(self, r: redis.Redis):
        stream_1 = "stream1:{grpmaxcount}"
        stream_2 = "stream2:{grpmaxcount}"
        group = "group"
        consumer = "consumer"
        for i in range(3):
            await r.xadd(stream_1, {"f": i})
        for i in range(3):
            await r.xadd(stream_2, {"f": i})
        await r.xgroup_create(stream_1, group, 0)
        await r.xgroup_create(stream_2, group, 0)

        res = await r.xreadgroup(
            group,
            consumer,
            streams={stream_1: ">", stream_2: ">"},
            count=3,
            max_count=4,
        )
        assert self._total_stream_entries(r, res) == 4
        await r.xgroup_destroy(stream_1, group)
        await r.xgroup_destroy(stream_2, group)

    @skip_if_server_version_lt("5.0.0")
    async def test_xreadgroup(self, r: redis.Redis):
        stream = "stream"
        group = "group"
        consumer = "consumer"
        m1 = await r.xadd(stream, {"foo": "bar"})
        m2 = await r.xadd(stream, {"bing": "baz"})
        await r.xgroup_create(stream, group, 0)

        strem_name = stream.encode()
        expected_entries = [
            await get_stream_message(r, stream, m1),
            await get_stream_message(r, stream, m2),
        ]

        # xread starting at 0 returns both messages
        res = await r.xreadgroup(group, consumer, streams={stream: ">"})
        assert_resp_response(
            r,
            res,
            [[strem_name, expected_entries]],
            {strem_name: [expected_entries]},
            {strem_name: expected_entries},
        )

        await r.xgroup_destroy(stream, group)
        await r.xgroup_create(stream, group, 0)

        expected_entries = [await get_stream_message(r, stream, m1)]

        # xread with count=1 returns only the first message
        res = await r.xreadgroup(group, consumer, streams={stream: ">"}, count=1)
        assert_resp_response(
            r,
            res,
            [[strem_name, expected_entries]],
            {strem_name: [expected_entries]},
            {strem_name: expected_entries},
        )

        await r.xgroup_destroy(stream, group)

        # create the group using $ as the last id meaning subsequent reads
        # will only find messages added after this
        await r.xgroup_create(stream, group, "$")

        # xread starting after the last message returns an empty message list
        res = await r.xreadgroup(group, consumer, streams={stream: ">"})
        assert_resp_response(r, res, [], {})

        # xreadgroup with noack does not have any items in the PEL
        await r.xgroup_destroy(stream, group)
        await r.xgroup_create(stream, group, "0")
        res = await r.xreadgroup(group, consumer, streams={stream: ">"}, noack=True)
        empty_res = await r.xreadgroup(group, consumer, streams={stream: "0"})
        shape = expected_response_shape(r)
        if shape == "legacy_resp2":
            assert len(res[0][1]) == 2
            # now there should be nothing pending
            assert len(empty_res[0][1]) == 0
        elif shape == "legacy_resp3":
            assert len(res[strem_name][0]) == 2
            # now there should be nothing pending
            assert len(empty_res[strem_name][0]) == 0
        else:
            assert len(res[strem_name]) == 2
            # now there should be nothing pending
            assert len(empty_res[strem_name]) == 0

        await r.xgroup_destroy(stream, group)
        await r.xgroup_create(stream, group, "0")
        # delete all the messages in the stream
        expected_entries = [(m1, {}), (m2, {})]
        await r.xreadgroup(group, consumer, streams={stream: ">"})
        await r.xtrim(stream, 0)
        res = await r.xreadgroup(group, consumer, streams={stream: "0"})
        assert_resp_response(
            r,
            res,
            [[strem_name, expected_entries]],
            {strem_name: [expected_entries]},
            {strem_name: expected_entries},
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
    async def test_xreadgroup_with_claim_min_idle_time(self, r):
        stream = "stream"
        group = "group"
        consumer_1 = "c1"
        stream_name = stream.encode()

        try:
            # before test cleanup
            await r.xgroup_destroy(stream, group)
        except ResponseError:
            pass

        m1 = await r.xadd(stream, {"key_m1": "val_m1"})
        m2 = await r.xadd(stream, {"key_m2": "val_m2"})

        await r.xgroup_create(stream, group, 0)

        # read all the messages - this will save the msgs in PEL
        await r.xreadgroup(group, consumer_1, streams={stream: ">"})

        # wait for 100ms - so that the messages would have been in the PEL for long enough
        await asyncio.sleep(0.1)

        m3 = await r.xadd(stream, {"key_m3": "val_m3"})
        m4 = await r.xadd(stream, {"key_m4": "val_m4"})

        expected_entries = {
            stream_name: [
                {"msg": await get_stream_message(r, stream, m1), "min_idle_time": 100},
                {"msg": await get_stream_message(r, stream, m2), "min_idle_time": 100},
                {"msg": await get_stream_message(r, stream, m3), "min_idle_time": 0},
                {"msg": await get_stream_message(r, stream, m4), "min_idle_time": 0},
            ]
        }

        res = await r.xreadgroup(
            group, consumer_1, streams={stream: ">"}, claim_min_idle_time=100
        )

        self._validate_xreadgroup_with_claim_min_idle_time_response(
            r, res, expected_entries
        )

        # validate PEL still holds the messages until they are acknowledged
        response = await r.xpending_range(stream, group, min="-", max="+", count=5)
        assert len(response) == 4

        # add 2 more messages
        m5 = await r.xadd(stream, {"key_m5": "val_m5"})
        m6 = await r.xadd(stream, {"key_m6": "val_m6"})

        expected_entries = [
            await get_stream_message(r, stream, m5),
            await get_stream_message(r, stream, m6),
        ]
        # read all the messages - this will save the msgs in PEL
        res = await r.xreadgroup(group, consumer_1, streams={stream: ">"})
        assert_resp_response(
            r,
            res,
            [[stream_name, expected_entries]],
            {stream_name: [expected_entries]},
            {stream_name: expected_entries},
        )

        # add 2 more messages
        m7 = await r.xadd(stream, {"key_m7": "val_m7"})
        m8 = await r.xadd(stream, {"key_m8": "val_m8"})
        # Use a threshold safely above this test's elapsed time so only
        # newly-delivered messages are returned.
        expected_entries = {
            stream_name: [
                {"msg": await get_stream_message(r, stream, m7), "min_idle_time": 0},
                {"msg": await get_stream_message(r, stream, m8), "min_idle_time": 0},
            ]
        }
        res = await r.xreadgroup(
            group, consumer_1, streams={stream: ">"}, claim_min_idle_time=60_000
        )
        self._validate_xreadgroup_with_claim_min_idle_time_response(
            r, res, expected_entries
        )

    @skip_if_server_version_lt("8.3.224")
    async def test_xreadgroup_with_claim_min_idle_time_multiple_streams_same_slots(
        self, r
    ):
        stream_1 = "stream1:{same_slot:42}"
        stream_2 = "stream2:{same_slot:42}"
        group = "group"
        consumer_1 = "c1"

        stream_1_name = stream_1.encode()
        stream_2_name = stream_2.encode()

        # before test cleanup
        try:
            await r.xgroup_destroy(stream_1, group)
        except ResponseError:
            pass
        try:
            await r.xgroup_destroy(stream_2, group)
        except ResponseError:
            pass

        m1_s1 = await r.xadd(stream_1, {"key_m1_s1": "val_m1"})
        m2_s1 = await r.xadd(stream_1, {"key_m2_s1": "val_m2"})
        await r.xgroup_create(stream_1, group, 0)

        m1_s2 = await r.xadd(stream_2, {"key_m1_s2": "val_m1"})
        m2_s2 = await r.xadd(stream_2, {"key_m2_s2": "val_m2"})
        await r.xgroup_create(stream_2, group, 0)

        # read all the messages - this will save the msgs in PEL
        await r.xreadgroup(group, consumer_1, streams={stream_1: ">", stream_2: ">"})

        # wait for 100ms - so that the messages would have been in the PEL for long enough
        await asyncio.sleep(0.1)

        m3_s1 = await r.xadd(stream_1, {"key_m3_s1": "val_m3"})
        m4_s2 = await r.xadd(stream_2, {"key_m4_s2": "val_m4"})

        expected_entries = {
            stream_1_name: [
                {
                    "msg": await get_stream_message(r, stream_1, m1_s1),
                    "min_idle_time": 100,
                },
                {
                    "msg": await get_stream_message(r, stream_1, m2_s1),
                    "min_idle_time": 100,
                },
                {
                    "msg": await get_stream_message(r, stream_1, m3_s1),
                    "min_idle_time": 0,
                },
            ],
            stream_2_name: [
                {
                    "msg": await get_stream_message(r, stream_2, m1_s2),
                    "min_idle_time": 100,
                },
                {
                    "msg": await get_stream_message(r, stream_2, m2_s2),
                    "min_idle_time": 100,
                },
                {
                    "msg": await get_stream_message(r, stream_2, m4_s2),
                    "min_idle_time": 0,
                },
            ],
        }

        res = await r.xreadgroup(
            group,
            consumer_1,
            streams={stream_1: ">", stream_2: ">"},
            claim_min_idle_time=100,
        )

        self._validate_xreadgroup_with_claim_min_idle_time_response(
            r, res, expected_entries
        )

        # validate PEL still holds the messages until they are acknowledged
        response = await r.xpending_range(stream_1, group, min="-", max="+", count=5)
        assert len(response) == 3
        response = await r.xpending_range(stream_2, group, min="-", max="+", count=5)
        assert len(response) == 3

        # add 2 more messages
        await r.xadd(stream_1, {"key_m5": "val_m5"})
        await r.xadd(stream_2, {"key_m6": "val_m6"})

        # read all the messages - this will save the msgs in PEL
        await r.xreadgroup(group, consumer_1, streams={stream_1: ">", stream_2: ">"})

        # add 2 more messages
        m7 = await r.xadd(stream_1, {"key_m7": "val_m7"})
        m8 = await r.xadd(stream_2, {"key_m8": "val_m8"})
        # Use a threshold safely above this test's elapsed time so only
        # newly-delivered messages are returned.
        expected_entries = {
            stream_1_name: [
                {"msg": await get_stream_message(r, stream_1, m7), "min_idle_time": 0}
            ],
            stream_2_name: [
                {"msg": await get_stream_message(r, stream_2, m8), "min_idle_time": 0}
            ],
        }
        res = await r.xreadgroup(
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
    async def test_xreadgroup_with_claim_min_idle_time_multiple_streams(self, r):
        stream_1 = "stream1"
        stream_2 = "stream2"
        group = "group"
        consumer_1 = "c1"

        stream_1_name = stream_1.encode()
        stream_2_name = stream_2.encode()

        # before test cleanup
        try:
            await r.xgroup_destroy(stream_1, group)
        except ResponseError:
            pass
        try:
            await r.xgroup_destroy(stream_2, group)
        except ResponseError:
            pass

        m1_s1 = await r.xadd(stream_1, {"key_m1_s1": "val_m1"})
        m2_s1 = await r.xadd(stream_1, {"key_m2_s1": "val_m2"})
        await r.xgroup_create(stream_1, group, 0)

        m1_s2 = await r.xadd(stream_2, {"key_m1_s2": "val_m1"})
        m2_s2 = await r.xadd(stream_2, {"key_m2_s2": "val_m2"})
        await r.xgroup_create(stream_2, group, 0)

        # read all the messages - this will save the msgs in PEL
        await r.xreadgroup(group, consumer_1, streams={stream_1: ">", stream_2: ">"})

        # wait for 100ms - so that the messages would have been in the PEL for long enough
        await asyncio.sleep(0.1)

        m3_s1 = await r.xadd(stream_1, {"key_m3_s1": "val_m3"})
        m4_s2 = await r.xadd(stream_2, {"key_m4_s2": "val_m4"})

        expected_entries = {
            stream_1_name: [
                {
                    "msg": await get_stream_message(r, stream_1, m1_s1),
                    "min_idle_time": 100,
                },
                {
                    "msg": await get_stream_message(r, stream_1, m2_s1),
                    "min_idle_time": 100,
                },
                {
                    "msg": await get_stream_message(r, stream_1, m3_s1),
                    "min_idle_time": 0,
                },
            ],
            stream_2_name: [
                {
                    "msg": await get_stream_message(r, stream_2, m1_s2),
                    "min_idle_time": 100,
                },
                {
                    "msg": await get_stream_message(r, stream_2, m2_s2),
                    "min_idle_time": 100,
                },
                {
                    "msg": await get_stream_message(r, stream_2, m4_s2),
                    "min_idle_time": 0,
                },
            ],
        }

        res = await r.xreadgroup(
            group,
            consumer_1,
            streams={stream_1: ">", stream_2: ">"},
            claim_min_idle_time=100,
        )

        self._validate_xreadgroup_with_claim_min_idle_time_response(
            r, res, expected_entries
        )

        # validate PEL still holds the messages until they are acknowledged
        response = await r.xpending_range(stream_1, group, min="-", max="+", count=5)
        assert len(response) == 3
        response = await r.xpending_range(stream_2, group, min="-", max="+", count=5)
        assert len(response) == 3

        # add 2 more messages
        await r.xadd(stream_1, {"key_m5": "val_m5"})
        await r.xadd(stream_2, {"key_m6": "val_m6"})

        # read all the messages - this will save the msgs in PEL
        await r.xreadgroup(group, consumer_1, streams={stream_1: ">", stream_2: ">"})

        # add 2 more messages
        m7 = await r.xadd(stream_1, {"key_m7": "val_m7"})
        m8 = await r.xadd(stream_2, {"key_m8": "val_m8"})
        # Use a threshold safely above this test's elapsed time so only
        # newly-delivered messages are returned.
        expected_entries = {
            stream_1_name: [
                {"msg": await get_stream_message(r, stream_1, m7), "min_idle_time": 0}
            ],
            stream_2_name: [
                {"msg": await get_stream_message(r, stream_2, m8), "min_idle_time": 0}
            ],
        }
        res = await r.xreadgroup(
            group,
            consumer_1,
            streams={stream_1: ">", stream_2: ">"},
            claim_min_idle_time=60_000,
        )
        self._validate_xreadgroup_with_claim_min_idle_time_response(
            r, res, expected_entries
        )

    @skip_if_server_version_lt("5.0.0")
    async def test_xrevrange(self, r: redis.Redis):
        stream = "stream"
        m1 = await r.xadd(stream, {"foo": "bar"})
        m2 = await r.xadd(stream, {"foo": "bar"})
        m3 = await r.xadd(stream, {"foo": "bar"})
        m4 = await r.xadd(stream, {"foo": "bar"})

        def get_ids(results):
            return [result[0] for result in results]

        results = await r.xrevrange(stream, max=m4)
        assert get_ids(results) == [m4, m3, m2, m1]

        results = await r.xrevrange(stream, max=m3, min=m2)
        assert get_ids(results) == [m3, m2]

        results = await r.xrevrange(stream, min=m3)
        assert get_ids(results) == [m4, m3]

        results = await r.xrevrange(stream, min=m2, count=1)
        assert get_ids(results) == [m4]

    @skip_if_server_version_lt("5.0.0")
    async def test_xtrim(self, r: redis.Redis):
        stream = "stream"

        # trimming an empty key doesn't do anything
        assert await r.xtrim(stream, 1000) == 0

        await r.xadd(stream, {"foo": "bar"})
        await r.xadd(stream, {"foo": "bar"})
        await r.xadd(stream, {"foo": "bar"})
        await r.xadd(stream, {"foo": "bar"})

        # trimming an amount large than the number of messages
        # doesn't do anything
        assert await r.xtrim(stream, 5, approximate=False) == 0

        # 1 message is trimmed
        assert await r.xtrim(stream, 3, approximate=False) == 1

    @skip_if_server_version_lt("8.1.224")
    async def test_xdelex(self, r: redis.Redis):
        stream = "stream"

        m1 = await r.xadd(stream, {"foo": "bar"})
        m2 = await r.xadd(stream, {"foo": "bar"})
        m3 = await r.xadd(stream, {"foo": "bar"})
        m4 = await r.xadd(stream, {"foo": "bar"})

        # Test XDELEX with default ref_policy (KEEPREF)
        result = await r.xdelex(stream, m1)
        assert result == [1]

        # Test XDELEX with explicit KEEPREF
        result = await r.xdelex(stream, m2, ref_policy="KEEPREF")
        assert result == [1]

        # Test XDELEX with DELREF
        result = await r.xdelex(stream, m3, ref_policy="DELREF")
        assert result == [1]

        # Test XDELEX with ACKED
        result = await r.xdelex(stream, m4, ref_policy="ACKED")
        assert result == [1]

        # Test with non-existent ID
        result = await r.xdelex(stream, "999999-0", ref_policy="KEEPREF")
        assert result == [-1]

        # Test with multiple IDs
        m5 = await r.xadd(stream, {"foo": "bar"})
        m6 = await r.xadd(stream, {"foo": "bar"})
        result = await r.xdelex(stream, m5, m6, ref_policy="KEEPREF")
        assert result == [1, 1]

        # Test error cases
        with pytest.raises(redis.DataError):
            await r.xdelex(stream, "123-0", ref_policy="INVALID")

        with pytest.raises(redis.DataError):
            await r.xdelex(stream)  # No IDs provided

    @skip_if_server_version_lt("8.1.224")
    async def test_xackdel(self, r: redis.Redis):
        stream = "stream"
        group = "group"
        consumer = "consumer"

        m1 = await r.xadd(stream, {"foo": "bar"})
        m2 = await r.xadd(stream, {"foo": "bar"})
        m3 = await r.xadd(stream, {"foo": "bar"})
        m4 = await r.xadd(stream, {"foo": "bar"})
        await r.xgroup_create(stream, group, 0)

        await r.xreadgroup(group, consumer, streams={stream: ">"})

        # Test XACKDEL with default ref_policy (KEEPREF)
        result = await r.xackdel(stream, group, m1)
        assert result == [1]

        # Test XACKDEL with explicit KEEPREF
        result = await r.xackdel(stream, group, m2, ref_policy="KEEPREF")
        assert result == [1]

        # Test XACKDEL with DELREF
        result = await r.xackdel(stream, group, m3, ref_policy="DELREF")
        assert result == [1]

        # Test XACKDEL with ACKED
        result = await r.xackdel(stream, group, m4, ref_policy="ACKED")
        assert result == [1]

        # Test with non-existent ID
        result = await r.xackdel(stream, group, "999999-0", ref_policy="KEEPREF")
        assert result == [-1]

        # Test error cases
        with pytest.raises(redis.DataError):
            await r.xackdel(stream, group, m1, ref_policy="INVALID")

        with pytest.raises(redis.DataError):
            await r.xackdel(stream, group)  # No IDs provided

    @skip_if_server_version_lt("8.1.224")
    async def test_xtrim_with_options(self, r: redis.Redis):
        stream = "stream"

        await r.xadd(stream, {"foo": "bar"})
        await r.xadd(stream, {"foo": "bar"})
        await r.xadd(stream, {"foo": "bar"})
        await r.xadd(stream, {"foo": "bar"})

        # Test XTRIM with KEEPREF ref_policy
        assert (
            await r.xtrim(stream, maxlen=2, approximate=False, ref_policy="KEEPREF")
            == 2
        )

        await r.xadd(stream, {"foo": "bar"})
        await r.xadd(stream, {"foo": "bar"})

        # Test XTRIM with DELREF ref_policy
        assert (
            await r.xtrim(stream, maxlen=2, approximate=False, ref_policy="DELREF") == 2
        )

        await r.xadd(stream, {"foo": "bar"})
        await r.xadd(stream, {"foo": "bar"})

        # Test XTRIM with ACKED ref_policy
        assert (
            await r.xtrim(stream, maxlen=2, approximate=False, ref_policy="ACKED") == 2
        )

        # Test error case
        with pytest.raises(redis.DataError):
            await r.xtrim(stream, maxlen=2, ref_policy="INVALID")

    @skip_if_server_version_lt("8.1.224")
    async def test_xadd_with_options(self, r: redis.Redis):
        stream = "stream"

        # Test XADD with KEEPREF ref_policy
        await r.xadd(
            stream, {"foo": "bar"}, maxlen=2, approximate=False, ref_policy="KEEPREF"
        )
        await r.xadd(
            stream, {"foo": "bar"}, maxlen=2, approximate=False, ref_policy="KEEPREF"
        )
        await r.xadd(
            stream, {"foo": "bar"}, maxlen=2, approximate=False, ref_policy="KEEPREF"
        )
        assert await r.xlen(stream) == 2

        # Test XADD with DELREF ref_policy
        await r.xadd(
            stream, {"foo": "bar"}, maxlen=2, approximate=False, ref_policy="DELREF"
        )
        assert await r.xlen(stream) == 2

        # Test XADD with ACKED ref_policy
        await r.xadd(
            stream, {"foo": "bar"}, maxlen=2, approximate=False, ref_policy="ACKED"
        )
        assert await r.xlen(stream) == 2

        # Test error case
        with pytest.raises(redis.DataError):
            await r.xadd(stream, {"foo": "bar"}, ref_policy="INVALID")

    @skip_if_server_version_lt("8.5.0")
    async def test_xadd_idmpauto(self, r: redis.Redis):
        stream = "stream"

        # XADD with IDMPAUTO - first write
        message_id1 = await r.xadd(stream, {"field1": "value1"}, idmpauto="producer1")

        # Test XADD with IDMPAUTO - duplicate write returns same ID
        message_id2 = await r.xadd(stream, {"field1": "value1"}, idmpauto="producer1")
        assert message_id1 == message_id2

        # Test XADD with IDMPAUTO - different content creates new entry
        message_id3 = await r.xadd(stream, {"field1": "value2"}, idmpauto="producer1")
        assert message_id3 != message_id1

        # Test XADD with IDMPAUTO - different producer creates new entry
        message_id4 = await r.xadd(stream, {"field1": "value1"}, idmpauto="producer2")
        assert message_id4 != message_id1

        # Verify stream has 3 entries (2 unique from producer1, 1 from producer2)
        assert await r.xlen(stream) == 3

    @skip_if_server_version_lt("8.5.0")
    async def test_xadd_idmp(self, r: redis.Redis):
        stream = "stream"

        # Test XADD with IDMP - first write
        message_id1 = await r.xadd(
            stream, {"field1": "value1"}, idmp=("producer1", b"msg1")
        )

        # Test XADD with IDMP - duplicate write returns same ID
        message_id2 = await r.xadd(
            stream, {"field1": "value1"}, idmp=("producer1", b"msg1")
        )
        assert message_id1 == message_id2

        # Test XADD with IDMP - different iid creates new entry
        message_id3 = await r.xadd(
            stream, {"field1": "value1"}, idmp=("producer1", b"msg2")
        )
        assert message_id3 != message_id1

        # Test XADD with IDMP - different producer creates new entry
        message_id4 = await r.xadd(
            stream, {"field1": "value1"}, idmp=("producer2", b"msg1")
        )
        assert message_id4 != message_id1

        # Test XADD with IDMP - shorter binary iid
        await r.xadd(stream, {"field1": "value1"}, idmp=("producer1", b"\x01"))

        # Verify stream has 4 entries
        assert await r.xlen(stream) == 4

    @skip_if_server_version_lt("8.5.0")
    async def test_xadd_idmp_validation(self, r: redis.Redis):
        stream = "stream"

        # Test error: both idmpauto and idmp specified
        with pytest.raises(redis.DataError):
            await r.xadd(
                stream,
                {"foo": "bar"},
                idmpauto="producer1",
                idmp=("producer1", b"msg1"),
            )

        # Test error: idmpauto with explicit id
        with pytest.raises(redis.DataError):
            await r.xadd(
                stream, {"foo": "bar"}, id="1234567890-0", idmpauto="producer1"
            )

        # Test error: idmp with explicit id
        with pytest.raises(redis.DataError):
            await r.xadd(
                stream, {"foo": "bar"}, id="1234567890-0", idmp=("producer1", b"msg1")
            )

        # Test error: idmp not a tuple
        with pytest.raises(redis.DataError):
            await r.xadd(stream, {"foo": "bar"}, idmp="invalid")

        # Test error: idmp tuple with wrong number of elements
        with pytest.raises(redis.DataError):
            await r.xadd(stream, {"foo": "bar"}, idmp=("producer1",))

        # Test error: idmp tuple with wrong number of elements
        with pytest.raises(redis.DataError):
            await r.xadd(stream, {"foo": "bar"}, idmp=("producer1", b"msg1", "extra"))

    @skip_if_server_version_lt("8.5.0")
    async def test_xcfgset_idmp_duration(self, r: redis.Redis):
        stream = "stream"

        # Create stream first
        await r.xadd(stream, {"foo": "bar"})

        # Test XCFGSET with IDMP-DURATION only
        assert await r.xcfgset(stream, idmp_duration=120) == b"OK"

        # Test with minimum value
        assert await r.xcfgset(stream, idmp_duration=1) == b"OK"

        # Test with maximum value
        assert await r.xcfgset(stream, idmp_duration=300) == b"OK"

    @skip_if_server_version_lt("8.5.0")
    async def test_xcfgset_idmp_maxsize(self, r: redis.Redis):
        stream = "stream"

        # Create stream first
        await r.xadd(stream, {"foo": "bar"})

        # Test XCFGSET with IDMP-MAXSIZE only
        assert await r.xcfgset(stream, idmp_maxsize=5000) == b"OK"

        # Test with minimum value
        assert await r.xcfgset(stream, idmp_maxsize=1) == b"OK"

        # Test with maximum value
        assert await r.xcfgset(stream, idmp_maxsize=10000) == b"OK"

    @skip_if_server_version_lt("8.5.0")
    async def test_xcfgset_both_parameters(self, r: redis.Redis):
        stream = "stream"

        # Create stream first
        await r.xadd(stream, {"foo": "bar"})

        # Test XCFGSET with both IDMP-DURATION and IDMP-MAXSIZE
        assert await r.xcfgset(stream, idmp_duration=120, idmp_maxsize=5000) == b"OK"

        # Test with different values
        assert await r.xcfgset(stream, idmp_duration=60, idmp_maxsize=10000) == b"OK"

    @skip_if_server_version_lt("8.5.0")
    async def test_xcfgset_validation(self, r: redis.Redis):
        stream = "stream"

        # Test error: no parameters provided
        with pytest.raises(redis.DataError):
            await r.xcfgset(stream)

        # Test error: idmp_duration too small
        with pytest.raises(redis.DataError):
            await r.xcfgset(stream, idmp_duration=0)

        # Test error: idmp_duration too large
        with pytest.raises(redis.DataError):
            await r.xcfgset(stream, idmp_duration=301)

        # Test error: idmp_duration not an integer
        with pytest.raises(redis.DataError):
            await r.xcfgset(stream, idmp_duration="invalid")

        # Test error: idmp_maxsize too small
        with pytest.raises(redis.DataError):
            await r.xcfgset(stream, idmp_maxsize=0)

        # Test error: idmp_maxsize too large
        with pytest.raises(redis.DataError):
            await r.xcfgset(stream, idmp_maxsize=1000001)

        # Test error: idmp_maxsize not an integer
        with pytest.raises(redis.DataError):
            await r.xcfgset(stream, idmp_maxsize="invalid")

    @pytest.mark.onlynoncluster
    async def test_bitfield_operations(self, r: redis.Redis):
        # comments show affected bits
        await r.execute_command("SELECT", 10)
        bf = r.bitfield("a")
        resp = await (
            bf.set("u8", 8, 255)  # 00000000 11111111
            .get("u8", 0)  # 00000000
            .get("u4", 8)  # 1111
            .get("u4", 12)  # 1111
            .get("u4", 13)  # 111 0
            .execute()
        )
        assert resp == [0, 0, 15, 15, 14]

        # .set() returns the previous value...
        resp = await (
            bf.set("u8", 4, 1)  # 0000 0001
            .get("u16", 0)  # 00000000 00011111
            .set("u16", 0, 0)  # 00000000 00000000
            .execute()
        )
        assert resp == [15, 31, 31]

        # incrby adds to the value
        resp = await (
            bf.incrby("u8", 8, 254)  # 00000000 11111110
            .incrby("u8", 8, 1)  # 00000000 11111111
            .get("u16", 0)  # 00000000 11111111
            .execute()
        )
        assert resp == [254, 255, 255]

        # Verify overflow protection works as a method:
        await r.delete("a")
        resp = await (
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
        await r.delete("a")
        resp = await (
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
        await r.delete("a")
        bf = r.bitfield("a", default_overflow="FAIL")
        resp = await (
            bf.set("u8", 8, 255)  # 00000000 11111111
            .incrby("u8", 8, 1)  # 00000000 11111111  fail default
            .get("u16", 0)  # 00000000 11111111
            .execute()
        )
        assert resp == [0, None, 255]

    @skip_if_server_version_lt("6.0.0")
    async def test_bitfield_ro(self, r: redis.Redis):
        bf = r.bitfield("a")
        resp = await bf.set("u8", 8, 255).execute()
        assert resp == [0]

        resp = await r.bitfield_ro("a", "u8", 0)
        assert resp == [0]

        items = [("u4", 8), ("u4", 12), ("u4", 13)]
        resp = await r.bitfield_ro("a", "u8", 0, items)
        assert resp == [0, 15, 15, 14]

    @skip_if_server_version_lt("4.0.0")
    async def test_memory_stats(self, r: redis.Redis):
        # put a key into the current db to make sure that "db.<current-db>"
        # has data
        await r.set("foo", "bar")
        stats = await r.memory_stats()
        assert isinstance(stats, dict)
        for key, value in stats.items():
            if key.startswith("db."):
                assert not isinstance(value, list)

    @pytest.mark.fixed_client
    @skip_if_server_version_lt("4.0.0")
    @skip_if_redis_enterprise()
    @pytest.mark.parametrize("protocol", [2, 3])
    async def test_memory_stats_unified_responses(self, create_redis, protocol):
        r = await create_redis(protocol=protocol, legacy_responses=False)
        await r.set("foo", "bar")
        stats = await r.memory_stats()
        # Redis replies with doubles for these, which RESP2 sends as bulk strings.
        for key in ("dataset.percentage", "peak.percentage", "fragmentation"):
            assert type(stats[key]) is float
        assert type(stats["peak.allocated"]) is int
        for key, value in stats.items():
            if key.startswith("db.") and isinstance(value, dict):
                assert all(type(v) is int for v in value.values())

    @skip_if_server_version_lt("4.0.0")
    async def test_memory_usage(self, r: redis.Redis):
        await r.set("foo", "bar")
        assert isinstance(await r.memory_usage("foo"), int)

    @skip_if_server_version_lt("4.0.0")
    async def test_module_list(self, r: redis.Redis):
        assert isinstance(await r.module_list(), list)
        for x in await r.module_list():
            assert isinstance(x, dict)

    @pytest.mark.onlynoncluster
    async def test_interrupted_command(self, r: redis.Redis):
        """
        Regression test for issue #1128:  An Un-handled BaseException
        will leave the socket with un-read response to a previous
        command.
        """
        ready = asyncio.Event()

        async def helper():
            with pytest.raises(asyncio.CancelledError):
                # blocking pop
                ready.set()
                await r.brpop(["nonexist"])
            # If the following is not done, further Timout operations will fail,
            # because the timeout won't catch its Cancelled Error if the task
            # has a pending cancel.  Python documentation probably should reflect this.
            asyncio.current_task().uncancel()
            # if all is well, we can continue.  The following should not hang.
            await r.set("status", "down")

        task = asyncio.create_task(helper())
        await ready.wait()
        await asyncio.sleep(0.01)
        # the task is now sleeping, lets send it an exception
        task.cancel()
        # If all is well, the task should finish right away, otherwise fail with Timeout
        async with async_timeout(1.0):
            await task


@pytest.mark.onlynoncluster
class TestBinarySave:
    async def test_binary_get_set(self, r: redis.Redis):
        assert await r.set(" foo bar ", "123")
        assert await r.get(" foo bar ") == b"123"

        assert await r.set(" foo\r\nbar\r\n ", "456")
        assert await r.get(" foo\r\nbar\r\n ") == b"456"

        assert await r.set(" \r\n\t\x07\x13 ", "789")
        assert await r.get(" \r\n\t\x07\x13 ") == b"789"

        assert sorted(await r.keys("*")) == [
            b" \r\n\t\x07\x13 ",
            b" foo\r\nbar\r\n ",
            b" foo bar ",
        ]

        assert await r.delete(" foo bar ")
        assert await r.delete(" foo\r\nbar\r\n ")
        assert await r.delete(" \r\n\t\x07\x13 ")

    async def test_binary_lists(self, r: redis.Redis):
        mapping = {
            b"foo bar": [b"1", b"2", b"3"],
            b"foo\r\nbar\r\n": [b"4", b"5", b"6"],
            b"foo\tbar\x07": [b"7", b"8", b"9"],
        }
        # fill in lists
        for key, value in mapping.items():
            await r.rpush(key, *value)

        # check that KEYS returns all the keys as they are
        assert sorted(await r.keys("*")) == sorted(mapping.keys())

        # check that it is possible to get list content by key name
        for key, value in mapping.items():
            assert await r.lrange(key, 0, -1) == value

    async def test_large_responses(self, r: redis.Redis):
        """The PythonParser has some special cases for return values > 1MB"""
        # load up 5MB of data into a key
        data = "".join([ascii_letters] * (5000000 // len(ascii_letters)))
        await r.set("a", data)
        assert await r.get("a") == data.encode()

    async def test_floating_point_encoding(self, r: redis.Redis):
        """
        High precision floating point values sent to the server should keep
        precision.
        """
        timestamp = 1349673917.939762
        await r.zadd("a", {"a1": timestamp})
        assert await r.zscore("a", "a1") == timestamp


@pytest.mark.fixed_client
class TestInfoParsing:
    """INFO reply parsing on canned replies; no server needed."""

    async def test_22_info(self):
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


@pytest.mark.asyncio
class TestAsyncXreadXreadgroupMetricsExport:
    """Tests for xread/xreadgroup async commands to verify streaming lag metrics are exported."""

    @skip_if_server_version_lt("5.0.0")
    async def test_async_xread_exports_streaming_lag_metric(self, r: redis.Redis):
        """Test that async xread exports streaming lag metric."""
        stream = "test-stream-metrics"

        # Add a message to the stream
        await r.xadd(stream, {"foo": "bar"})

        with patch(
            "redis.commands.core.async_record_streaming_lag",
            new_callable=AsyncMock,
        ) as mock_recorder:
            # Read from the stream
            result = await r.xread(streams={stream: "0"})

            # Verify the async recorder was called
            mock_recorder.assert_awaited_once()
            call_args = mock_recorder.call_args
            # Verify response was passed to the recorder
            assert call_args[1]["response"] is not None
            # Verify result is returned correctly
            assert result is not None

        # Cleanup
        await r.delete(stream)

    @skip_if_server_version_lt("5.0.0")
    async def test_async_xreadgroup_exports_streaming_lag_metric_with_consumer_group(
        self, r: redis.Redis
    ):
        """Test that async xreadgroup exports streaming lag metric with consumer group."""
        stream = "test-stream-metrics-group"
        group = "test-group"
        consumer = "test-consumer"

        # Add a message and create consumer group
        await r.xadd(stream, {"foo": "bar"})
        try:
            await r.xgroup_create(stream, group, 0)
        except ResponseError:
            # Group may already exist
            pass

        with patch(
            "redis.commands.core.async_record_streaming_lag",
            new_callable=AsyncMock,
        ) as mock_recorder:
            # Read from the stream via consumer group
            result = await r.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream: ">"},
            )

            # Verify the async recorder was called with consumer group
            mock_recorder.assert_awaited_once()
            call_args = mock_recorder.call_args
            assert call_args[1]["response"] is not None
            assert call_args[1]["consumer_group"] == group
            # Verify result is returned correctly
            assert result is not None

        # Cleanup
        await r.xgroup_destroy(stream, group)
        await r.delete(stream)

    @skip_if_server_version_lt("5.0.0")
    async def test_async_xread_handles_empty_response(self, r: redis.Redis):
        """Test that async xread handles empty response gracefully."""
        stream = "test-stream-empty"

        # Create an empty stream by adding and deleting a message
        msg_id = await r.xadd(stream, {"foo": "bar"})
        await r.xdel(stream, msg_id)

        with patch(
            "redis.commands.core.async_record_streaming_lag",
            new_callable=AsyncMock,
        ) as mock_recorder:
            # Read from the stream starting after the deleted message
            result = await r.xread(streams={stream: msg_id})

            # Verify the async recorder was called (even with empty response)
            mock_recorder.assert_awaited_once()
            # Result should be None or empty ([] for RESP2, {} for RESP3)
            assert result is None or result == [] or result == {}

        # Cleanup
        await r.delete(stream)
