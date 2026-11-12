import asyncio
import functools
import random
import string
import time
from asyncio import Lock as AsyncLock
from asyncio import sleep as async_sleep
from typing import Optional, Tuple, Union
from unittest.mock import AsyncMock, call

import pytest
import pytest_asyncio
import redis
from redis import AuthenticationError, DataError, RedisError, ResponseError
from redis.asyncio import Connection, ConnectionPool, Redis
from redis.asyncio.retry import Retry
from redis.auth.err import RequestTokenErr
from redis.backoff import NoBackoff
from redis.credentials import CredentialProvider, UsernamePasswordCredentialProvider
from redis.exceptions import ConnectionError
from redis.utils import str_if_bytes
from tests.conftest import get_endpoint, skip_if_redis_enterprise
from tests.entraid_utils import AuthType
from tests.test_asyncio.conftest import get_credential_provider

try:
    from redis_entraid.cred_provider import EntraIdCredentialsProvider
except ImportError:
    EntraIdCredentialsProvider = None


async def _wait_until_passes(check, timeout=5.0):
    """Re-run ``check`` until its assertions pass, or re-raise at the deadline."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            check()
            return
        except AssertionError:
            if time.monotonic() >= deadline:
                raise
            await async_sleep(0.01)


@pytest.fixture()
def endpoint(request):
    endpoint_name = request.config.getoption("--endpoint-name")

    try:
        return get_endpoint(endpoint_name)
    except FileNotFoundError as e:
        pytest.skip(
            f"Skipping scenario test because endpoints file is missing: {str(e)}"
        )


@pytest_asyncio.fixture()
async def r_credential(request, create_redis, endpoint):
    credential_provider = request.param.get("cred_provider_class", None)

    if credential_provider is not None:
        credential_provider = get_credential_provider(request)

    kwargs = {
        "credential_provider": credential_provider,
    }

    return await create_redis(url=endpoint, **kwargs)


@pytest_asyncio.fixture()
async def r_acl_teardown(r: redis.Redis):
    """
    A special fixture which removes the provided names from the database after use
    """
    usernames = []

    def factory(username):
        usernames.append(username)
        return r

    yield factory
    for username in usernames:
        await r.acl_deluser(username)


@pytest_asyncio.fixture()
async def r_required_pass_teardown(r: redis.Redis):
    """
    A special fixture which removes the provided password from the database after use
    """
    passwords = []

    def factory(username):
        passwords.append(username)
        return r

    yield factory
    for password in passwords:
        try:
            await r.auth(password)
        except (ResponseError, AuthenticationError):
            await r.auth("default", "")
        await r.config_set("requirepass", "")


class NoPassCredProvider(CredentialProvider):
    def get_credentials(self) -> Union[Tuple[str], Tuple[str, str]]:
        return "username", ""


class FlakyCredProvider(CredentialProvider):
    """Fails, or stalls until cancelled, on the first fetch only."""

    def __init__(self, stall: bool = False):
        self.stall = stall
        self.calls = 0
        self.fetching = asyncio.Event()

    async def get_credentials_async(self) -> Union[Tuple[str], Tuple[str, str]]:
        self.calls += 1
        if self.calls == 1:
            self.fetching.set()
            if self.stall:
                await asyncio.Event().wait()
            raise RequestTokenErr("token endpoint unavailable")
        return "username", "password"


class AsyncRandomAuthCredProvider(CredentialProvider):
    def __init__(self, user: Optional[str], endpoint: str):
        self.user = user
        self.endpoint = endpoint

    @functools.lru_cache(maxsize=10)
    def get_credentials(self) -> Union[Tuple[str, str], Tuple[str]]:
        def get_random_string(length):
            letters = string.ascii_lowercase
            result_str = "".join(random.choice(letters) for i in range(length))
            return result_str

        if self.user:
            auth_token: str = get_random_string(5) + self.user + "_" + self.endpoint
            return self.user, auth_token
        else:
            auth_token: str = get_random_string(5) + self.endpoint
            return (auth_token,)


async def init_acl_user(r, username, password):
    # reset the user
    await r.acl_deluser(username)
    if password:
        assert (
            await r.acl_setuser(
                username,
                enabled=True,
                passwords=["+" + password],
                keys="~*",
                commands=[
                    "+ping",
                    "+command",
                    "+info",
                    "+select",
                    "+flushdb",
                    "+cluster",
                ],
            )
            is True
        )
    else:
        assert (
            await r.acl_setuser(
                username,
                enabled=True,
                keys="~*",
                commands=[
                    "+ping",
                    "+command",
                    "+info",
                    "+select",
                    "+flushdb",
                    "+cluster",
                ],
                nopass=True,
            )
            is True
        )


async def init_required_pass(r, password):
    await r.config_set("requirepass", password)


@pytest.mark.asyncio
class TestCredentialsProvider:
    @skip_if_redis_enterprise()
    async def test_only_pass_without_creds_provider(
        self, r_required_pass_teardown, create_redis
    ):
        # test for default user (`username` is supposed to be optional)
        password = "password"
        r = r_required_pass_teardown(password)
        await init_required_pass(r, password)
        assert await r.auth(password) is True

        r2 = await create_redis(flushdb=False, password=password)

        assert await r2.ping() is True

    @skip_if_redis_enterprise()
    async def test_user_and_pass_without_creds_provider(
        self, r_acl_teardown, create_redis
    ):
        """
        Test backward compatibility with username and password
        """
        # test for other users
        username = "username"
        password = "password"
        r = r_acl_teardown(username)
        await init_acl_user(r, username, password)
        r2 = await create_redis(flushdb=False, username=username, password=password)

        assert await r2.ping() is True

    @pytest.mark.parametrize("username", ["username", None])
    @skip_if_redis_enterprise()
    @pytest.mark.onlynoncluster
    async def test_credential_provider_with_supplier(
        self, r_acl_teardown, r_required_pass_teardown, create_redis, username
    ):
        creds_provider = AsyncRandomAuthCredProvider(
            user=username,
            endpoint="localhost",
        )

        auth_args = creds_provider.get_credentials()
        password = auth_args[-1]

        if username:
            r = r_acl_teardown(username)
            await init_acl_user(r, username, password)
        else:
            r = r_required_pass_teardown(password)
            await init_required_pass(r, password)

        r2 = await create_redis(flushdb=False, credential_provider=creds_provider)

        assert await r2.ping() is True

    async def test_async_credential_provider_no_password_success(
        self, r_acl_teardown, create_redis
    ):
        username = "username"
        r = r_acl_teardown(username)
        await init_acl_user(r, username, "")
        r2 = await create_redis(
            flushdb=False,
            credential_provider=NoPassCredProvider(),
        )
        assert await r2.ping() is True

    @pytest.mark.onlynoncluster
    async def test_credential_provider_no_password_error(
        self, r_acl_teardown, create_redis
    ):
        username = "username"
        r = r_acl_teardown(username)
        await init_acl_user(r, username, "password")
        with pytest.raises(AuthenticationError) as e:
            await create_redis(
                flushdb=False,
                credential_provider=NoPassCredProvider(),
                single_connection_client=True,
            )
        assert e.match("invalid username-password")
        assert await r.acl_deluser(username)

    @pytest.mark.onlynoncluster
    @pytest.mark.parametrize("create_redis", [(False,)], ids=["pool"], indirect=True)
    async def test_credential_provider_error_does_not_skip_authentication(
        self, r_acl_teardown, create_redis
    ):
        username = "username"
        r = r_acl_teardown(username)
        await init_acl_user(r, username, "password")
        provider = FlakyCredProvider()
        r2 = await create_redis(flushdb=False, credential_provider=provider)

        with pytest.raises(RequestTokenErr):
            await r2.ping()

        # The failed connection must not be reused as if the handshake had run
        assert await r2.ping() is True
        assert provider.calls == 2
        assert username in [client["user"] for client in await r.client_list()]

    @pytest.mark.onlynoncluster
    @pytest.mark.parametrize("create_redis", [(False,)], ids=["pool"], indirect=True)
    async def test_cancelled_credential_fetch_does_not_skip_authentication(
        self, r_acl_teardown, create_redis
    ):
        username = "username"
        r = r_acl_teardown(username)
        await init_acl_user(r, username, "password")
        provider = FlakyCredProvider(stall=True)
        r2 = await create_redis(flushdb=False, credential_provider=provider)

        ping = asyncio.create_task(r2.ping())
        await provider.fetching.wait()
        ping.cancel()
        with pytest.raises(asyncio.CancelledError):
            await ping

        # The failed connection must not be reused as if the handshake had run
        assert await r2.ping() is True
        assert provider.calls == 2
        assert username in [client["user"] for client in await r.client_list()]

    @pytest.mark.onlynoncluster
    async def test_password_and_username_together_with_cred_provider_raise_error(
        self, r_acl_teardown, create_redis
    ):
        username = "username"
        r = r_acl_teardown(username)
        await init_acl_user(r, username, "password")
        cred_provider = UsernamePasswordCredentialProvider(
            username="username", password="password"
        )
        with pytest.raises(DataError) as e:
            await create_redis(
                flushdb=False,
                username="username",
                password="password",
                credential_provider=cred_provider,
                single_connection_client=True,
            )
        assert e.match(
            "'username' and 'password' cannot be passed along with "
            "'credential_provider'."
        )

    @pytest.mark.onlynoncluster
    async def test_change_username_password_on_existing_connection(
        self, r_acl_teardown, create_redis
    ):
        username = "origin_username"
        password = "origin_password"
        new_username = "new_username"
        new_password = "new_password"
        r = r_acl_teardown(username)
        await init_acl_user(r, username, password)
        r2 = await create_redis(flushdb=False, username=username, password=password)
        assert await r2.ping() is True
        conn = await r2.connection_pool.get_connection()
        await conn.send_command("PING")
        assert str_if_bytes(await conn.read_response()) == "PONG"
        assert conn.username == username
        assert conn.password == password
        await init_acl_user(r, new_username, new_password)
        conn.password = new_password
        conn.username = new_username
        await conn.send_command("PING")
        assert str_if_bytes(await conn.read_response()) == "PONG"


@pytest.mark.asyncio
class TestUsernamePasswordCredentialProvider:
    async def test_user_pass_credential_provider_acl_user_and_pass(
        self, r_acl_teardown, create_redis
    ):
        username = "username"
        password = "password"
        r = r_acl_teardown(username)
        provider = UsernamePasswordCredentialProvider(username, password)
        assert provider.username == username
        assert provider.password == password
        assert provider.get_credentials() == (username, password)
        await init_acl_user(r, provider.username, provider.password)
        r2 = await create_redis(flushdb=False, credential_provider=provider)
        assert await r2.ping() is True

    async def test_user_pass_provider_only_password(
        self, r_required_pass_teardown, create_redis
    ):
        password = "password"
        provider = UsernamePasswordCredentialProvider(password=password)
        r = r_required_pass_teardown(password)
        assert provider.username == ""
        assert provider.password == password
        assert provider.get_credentials() == (password,)

        await init_required_pass(r, password)

        r2 = await create_redis(flushdb=False, credential_provider=provider)
        assert await r2.auth(provider.password) is True
        assert await r2.ping() is True


@pytest.mark.fixed_client
@pytest.mark.asyncio
@pytest.mark.skipif(not EntraIdCredentialsProvider, reason="requires redis-entraid")
class TestStreamingCredentialProvider:
    @pytest.mark.parametrize(
        "credential_provider",
        [
            {
                "cred_provider_class": EntraIdCredentialsProvider,
                "cred_provider_kwargs": {"expiration_refresh_ratio": 0.00005},
                "mock_idp": True,
            }
        ],
        indirect=True,
    )
    async def test_async_re_auth_all_connections(self, credential_provider):
        mock_connection = AsyncMock(spec=Connection)
        mock_connection.retry = Retry(NoBackoff(), 0)
        mock_another_connection = AsyncMock(spec=Connection)
        mock_pool = AsyncMock(spec=ConnectionPool)
        mock_pool.connection_kwargs = {
            "credential_provider": credential_provider,
        }
        mock_pool.get_connection.return_value = mock_connection
        mock_pool._available_connections = [mock_connection, mock_another_connection]
        mock_pool._lock = AsyncLock()
        auth_token = None

        async def re_auth_callback(token):
            nonlocal auth_token
            auth_token = token
            async with mock_pool._lock:
                for conn in mock_pool._available_connections:
                    await conn.send_command(
                        "AUTH", token.try_get("oid"), token.get_value()
                    )
                    await conn.read_response()

        mock_pool.re_auth_callback = re_auth_callback

        await Redis(
            connection_pool=mock_pool,
            credential_provider=credential_provider,
        )

        await credential_provider.get_credentials_async()

        # the re-auth runs in the background; poll instead of sleeping
        def reauth_done():
            assert auth_token is not None
            mock_connection.send_command.assert_has_calls(
                [call("AUTH", auth_token.try_get("oid"), auth_token.get_value())]
            )
            mock_another_connection.send_command.assert_has_calls(
                [call("AUTH", auth_token.try_get("oid"), auth_token.get_value())]
            )

        await _wait_until_passes(reauth_done)

    @pytest.mark.parametrize(
        "credential_provider",
        [
            {
                "cred_provider_class": EntraIdCredentialsProvider,
                "cred_provider_kwargs": {"expiration_refresh_ratio": 0.00005},
                "mock_idp": True,
            }
        ],
        indirect=True,
    )
    async def test_async_re_auth_partial_connections(self, credential_provider):
        mock_connection = AsyncMock(spec=Connection)
        mock_connection.retry = Retry(NoBackoff(), 3)
        mock_another_connection = AsyncMock(spec=Connection)
        mock_another_connection.retry = Retry(NoBackoff(), 3)
        mock_failed_connection = AsyncMock(spec=Connection)
        mock_failed_connection.read_response.side_effect = ConnectionError(
            "Failed auth"
        )
        mock_failed_connection.retry = Retry(NoBackoff(), 3)
        mock_pool = AsyncMock(spec=ConnectionPool)
        mock_pool.connection_kwargs = {
            "credential_provider": credential_provider,
        }
        mock_pool.get_connection.return_value = mock_connection
        mock_pool._available_connections = [
            mock_connection,
            mock_another_connection,
            mock_failed_connection,
        ]
        mock_pool._lock = AsyncLock()

        async def _raise(error: RedisError):
            pass

        async def re_auth_callback(token):
            async with mock_pool._lock:
                for conn in mock_pool._available_connections:
                    await conn.retry.call_with_retry(
                        lambda: conn.send_command(
                            "AUTH", token.try_get("oid"), token.get_value()
                        ),
                        lambda error: _raise(error),
                    )
                    await conn.retry.call_with_retry(
                        lambda: conn.read_response(), lambda error: _raise(error)
                    )

        mock_pool.re_auth_callback = re_auth_callback

        await Redis(
            connection_pool=mock_pool,
            credential_provider=credential_provider,
        )

        await credential_provider.get_credentials_async()

        # the re-auth runs in the background; poll instead of sleeping
        def reauth_done():
            mock_connection.read_response.assert_has_calls([call()])
            mock_another_connection.read_response.assert_has_calls([call()])
            mock_failed_connection.read_response.assert_has_calls(
                [call(), call(), call()]
            )

        await _wait_until_passes(reauth_done)

    @pytest.mark.parametrize(
        "credential_provider",
        [
            {
                "cred_provider_class": EntraIdCredentialsProvider,
                "cred_provider_kwargs": {"expiration_refresh_ratio": 0.00005},
                "mock_idp": True,
            }
        ],
        indirect=True,
    )
    async def test_re_auth_pub_sub_in_resp3(self, credential_provider):
        mock_pubsub_connection = AsyncMock(spec=Connection)
        mock_pubsub_connection.get_protocol.return_value = 3
        mock_pubsub_connection.credential_provider = credential_provider
        mock_pubsub_connection.retry = Retry(NoBackoff(), 3)
        mock_pubsub_connection.host = "localhost"
        mock_pubsub_connection.port = 6379
        mock_another_connection = AsyncMock(spec=Connection)
        mock_another_connection.retry = Retry(NoBackoff(), 3)
        mock_another_connection.host = "localhost"
        mock_another_connection.port = 6379

        mock_pool = AsyncMock(spec=ConnectionPool)
        mock_pool.connection_kwargs = {
            "credential_provider": credential_provider,
        }

        async def get_connection_side_effect():
            if not hasattr(get_connection_side_effect, "call_count"):
                get_connection_side_effect.call_count = 0
            result = [mock_pubsub_connection, mock_another_connection][
                get_connection_side_effect.call_count
            ]
            get_connection_side_effect.call_count += 1
            return result

        mock_pool.get_connection = AsyncMock(side_effect=get_connection_side_effect)
        mock_pool._available_connections = [mock_another_connection]
        mock_pool._lock = AsyncLock()
        auth_token = None

        async def re_auth_callback(token):
            nonlocal auth_token
            auth_token = token
            async with mock_pool._lock:
                for conn in mock_pool._available_connections:
                    await conn.send_command(
                        "AUTH", token.try_get("oid"), token.get_value()
                    )
                    await conn.read_response()

        mock_pool.re_auth_callback = re_auth_callback

        r = Redis(
            connection_pool=mock_pool,
            credential_provider=credential_provider,
        )
        p = r.pubsub()
        await p.subscribe("test")
        await credential_provider.get_credentials_async()

        # the re-auth runs in the background; poll instead of sleeping
        def reauth_done():
            assert auth_token is not None
            mock_pubsub_connection.send_command.assert_has_calls(
                [
                    call("SUBSCRIBE", "test", check_health=True),
                    call("AUTH", auth_token.try_get("oid"), auth_token.get_value()),
                ]
            )
            mock_another_connection.send_command.assert_has_calls(
                [call("AUTH", auth_token.try_get("oid"), auth_token.get_value())]
            )

        await _wait_until_passes(reauth_done)

    @pytest.mark.parametrize(
        "credential_provider",
        [
            {
                "cred_provider_class": EntraIdCredentialsProvider,
                "cred_provider_kwargs": {"expiration_refresh_ratio": 0.00005},
                "mock_idp": True,
            }
        ],
        indirect=True,
    )
    async def test_do_not_re_auth_pub_sub_in_resp2(self, credential_provider):
        mock_pubsub_connection = AsyncMock(spec=Connection)
        mock_pubsub_connection.get_protocol.return_value = 2
        mock_pubsub_connection.credential_provider = credential_provider
        mock_pubsub_connection.retry = Retry(NoBackoff(), 3)
        mock_pubsub_connection.host = "localhost"
        mock_pubsub_connection.port = 6379
        mock_another_connection = AsyncMock(spec=Connection)
        mock_another_connection.retry = Retry(NoBackoff(), 3)
        mock_another_connection.host = "localhost"
        mock_another_connection.port = 6379

        mock_pool = AsyncMock(spec=ConnectionPool)
        mock_pool.connection_kwargs = {
            "credential_provider": credential_provider,
        }

        async def get_connection_side_effect():
            if not hasattr(get_connection_side_effect, "call_count"):
                get_connection_side_effect.call_count = 0
            result = [mock_pubsub_connection, mock_another_connection][
                get_connection_side_effect.call_count
            ]
            get_connection_side_effect.call_count += 1
            return result

        mock_pool.get_connection = AsyncMock(side_effect=get_connection_side_effect)
        mock_pool._available_connections = [mock_another_connection]
        mock_pool._lock = AsyncLock()
        auth_token = None

        async def re_auth_callback(token):
            nonlocal auth_token
            auth_token = token
            async with mock_pool._lock:
                for conn in mock_pool._available_connections:
                    await conn.send_command(
                        "AUTH", token.try_get("oid"), token.get_value()
                    )
                    await conn.read_response()

        mock_pool.re_auth_callback = re_auth_callback

        r = Redis(
            connection_pool=mock_pool,
            credential_provider=credential_provider,
        )
        p = r.pubsub()
        await p.subscribe("test")
        await credential_provider.get_credentials_async()

        # the re-auth runs in the background; poll instead of sleeping
        def reauth_done():
            assert auth_token is not None
            mock_pubsub_connection.send_command.assert_has_calls(
                [
                    call("SUBSCRIBE", "test", check_health=True),
                ]
            )
            mock_another_connection.send_command.assert_has_calls(
                [call("AUTH", auth_token.try_get("oid"), auth_token.get_value())]
            )

        await _wait_until_passes(reauth_done)

    @pytest.mark.parametrize(
        "credential_provider",
        [
            {
                "cred_provider_class": EntraIdCredentialsProvider,
                "cred_provider_kwargs": {"expiration_refresh_ratio": 0.00005},
                "mock_idp": True,
            }
        ],
        indirect=True,
    )
    async def test_fails_on_token_renewal(self, credential_provider):
        credential_provider._token_mgr._idp.request_token.side_effect = [
            RequestTokenErr,
            RequestTokenErr,
            RequestTokenErr,
            RequestTokenErr,
        ]
        mock_connection = AsyncMock(spec=Connection)
        mock_connection.retry = Retry(NoBackoff(), 0)
        mock_another_connection = AsyncMock(spec=Connection)
        mock_pool = AsyncMock(spec=ConnectionPool)
        mock_pool.connection_kwargs = {
            "credential_provider": credential_provider,
        }
        mock_pool.get_connection.return_value = mock_connection
        mock_pool._available_connections = [mock_connection, mock_another_connection]

        await Redis(
            connection_pool=mock_pool,
            credential_provider=credential_provider,
        )

        with pytest.raises(RequestTokenErr):
            await credential_provider.get_credentials()


@pytest.mark.asyncio
@pytest.mark.onlynoncluster
@pytest.mark.cp_integration
@pytest.mark.skipif(not EntraIdCredentialsProvider, reason="requires redis-entraid")
class TestEntraIdCredentialsProvider:
    @pytest.mark.parametrize(
        "r_credential",
        [
            {
                "cred_provider_class": EntraIdCredentialsProvider,
            },
            {
                "cred_provider_class": EntraIdCredentialsProvider,
                "cred_provider_kwargs": {"block_for_initial": True},
            },
            {
                "cred_provider_class": EntraIdCredentialsProvider,
                "idp_kwargs": {"auth_type": AuthType.DEFAULT_AZURE_CREDENTIAL},
            },
        ],
        ids=["blocked", "non-blocked", "DefaultAzureCredential"],
        indirect=True,
    )
    @pytest.mark.asyncio
    @pytest.mark.onlynoncluster
    @pytest.mark.cp_integration
    async def test_async_auth_pool_with_credential_provider(self, r_credential: Redis):
        assert await r_credential.ping() is True

    @pytest.mark.parametrize(
        "r_credential",
        [
            {
                "cred_provider_class": EntraIdCredentialsProvider,
            },
            {
                "cred_provider_class": EntraIdCredentialsProvider,
                "cred_provider_kwargs": {"block_for_initial": True},
            },
        ],
        ids=["blocked", "non-blocked"],
        indirect=True,
    )
    @pytest.mark.asyncio
    @pytest.mark.onlynoncluster
    @pytest.mark.cp_integration
    async def test_async_pipeline_with_credential_provider(self, r_credential: Redis):
        pipe = r_credential.pipeline()

        await pipe.set("key", "value")
        await pipe.get("key")

        assert await pipe.execute() == [True, b"value"]

    @pytest.mark.parametrize(
        "r_credential",
        [
            {
                "cred_provider_class": EntraIdCredentialsProvider,
            },
        ],
        indirect=True,
    )
    @pytest.mark.asyncio
    @pytest.mark.onlynoncluster
    @pytest.mark.cp_integration
    async def test_async_auth_pubsub_with_credential_provider(
        self, r_credential: Redis
    ):
        p = r_credential.pubsub()
        await p.subscribe("entraid")

        await r_credential.publish("entraid", "test")
        await r_credential.publish("entraid", "test")

        msg1 = await p.get_message()

        assert msg1["type"] == "subscribe"


@pytest.mark.asyncio
@pytest.mark.onlycluster
@pytest.mark.cp_integration
@pytest.mark.skipif(not EntraIdCredentialsProvider, reason="requires redis-entraid")
class TestClusterEntraIdCredentialsProvider:
    @pytest.mark.parametrize(
        "r_credential",
        [
            {
                "cred_provider_class": EntraIdCredentialsProvider,
            },
            {
                "cred_provider_class": EntraIdCredentialsProvider,
                "cred_provider_kwargs": {"block_for_initial": True},
            },
            {
                "cred_provider_class": EntraIdCredentialsProvider,
                "idp_kwargs": {"auth_type": AuthType.DEFAULT_AZURE_CREDENTIAL},
            },
        ],
        ids=["blocked", "non-blocked", "DefaultAzureCredential"],
        indirect=True,
    )
    @pytest.mark.asyncio
    @pytest.mark.onlycluster
    @pytest.mark.cp_integration
    async def test_async_auth_pool_with_credential_provider(self, r_credential: Redis):
        assert await r_credential.ping() is True
