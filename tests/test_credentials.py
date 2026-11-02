import functools
import random
import string
import threading
import time
from time import sleep
from typing import Optional, Tuple, Union
from unittest.mock import Mock, call

import pytest
import redis
from redis import AuthenticationError, DataError, Redis, ResponseError
from redis.auth.err import RequestTokenErr
from redis.backoff import NoBackoff
from redis.connection import (
    ConnectionInterface,
    ConnectionPool,
)
from redis.credentials import CredentialProvider, UsernamePasswordCredentialProvider
from redis.exceptions import ConnectionError, RedisError
from redis.retry import Retry
from redis.utils import str_if_bytes
from tests.conftest import (
    _get_client,
    get_credential_provider,
    get_endpoint,
    skip_if_redis_enterprise,
)
from tests.entraid_utils import AuthType

try:
    from redis_entraid.cred_provider import EntraIdCredentialsProvider
except ImportError:
    EntraIdCredentialsProvider = None


def _wait_until_passes(check, timeout=5.0):
    """Re-run ``check`` until its assertions pass, or re-raise at the deadline."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            check()
            return
        except AssertionError:
            if time.monotonic() >= deadline:
                raise
            sleep(0.01)


@pytest.fixture()
def endpoint(request):
    endpoint_name = request.config.getoption("--endpoint-name")

    try:
        return get_endpoint(endpoint_name)
    except FileNotFoundError as e:
        pytest.skip(
            f"Skipping scenario test because endpoints file is missing: {str(e)}"
        )


@pytest.fixture()
def r_entra(request, endpoint):
    credential_provider = request.param.get("cred_provider_class", None)
    single_connection = request.param.get("single_connection_client", False)

    if credential_provider is not None:
        credential_provider = get_credential_provider(request)

    with _get_client(
        redis.Redis,
        request,
        credential_provider=credential_provider,
        single_connection_client=single_connection,
        from_url=endpoint,
    ) as client:
        yield client


class NoPassCredProvider(CredentialProvider):
    def get_credentials(self) -> Union[Tuple[str], Tuple[str, str]]:
        return "username", ""


class FlakyCredProvider(CredentialProvider):
    """Fails on the first fetch only, as a token or secrets backend can."""

    def __init__(self):
        self.calls = 0

    def get_credentials(self) -> Union[Tuple[str], Tuple[str, str]]:
        self.calls += 1
        if self.calls == 1:
            raise RequestTokenErr("token endpoint unavailable")
        return "username", "password"


class RandomAuthCredProvider(CredentialProvider):
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


def init_acl_user(r, request, username, password):
    # reset the user
    r.acl_deluser(username)
    if password:
        assert (
            r.acl_setuser(
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
            r.acl_setuser(
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

    if request is not None:

        def teardown():
            r.acl_deluser(username)

        request.addfinalizer(teardown)


def init_required_pass(r, request, password):
    r.config_set("requirepass", password)

    def teardown():
        try:
            r.auth(password)
        except (ResponseError, AuthenticationError):
            r.auth("default", "")
        r.config_set("requirepass", "")

    request.addfinalizer(teardown)


class TestCredentialsProvider:
    @skip_if_redis_enterprise()
    def test_only_pass_without_creds_provider(self, r, request):
        # test for default user (`username` is supposed to be optional)
        password = "password"
        init_required_pass(r, request, password)
        assert r.auth(password) is True

        r2 = _get_client(redis.Redis, request, flushdb=False, password=password)

        assert r2.ping() is True

    @skip_if_redis_enterprise()
    def test_user_and_pass_without_creds_provider(self, r, request):
        """
        Test backward compatibility with username and password
        """
        # test for other users
        username = "username"
        password = "password"

        init_acl_user(r, request, username, password)
        r2 = _get_client(
            redis.Redis, request, flushdb=False, username=username, password=password
        )

        assert r2.ping() is True

    @pytest.mark.parametrize("username", ["username", None])
    @skip_if_redis_enterprise()
    @pytest.mark.onlynoncluster
    def test_credential_provider_with_supplier(self, r, request, username):
        creds_provider = RandomAuthCredProvider(
            user=username,
            endpoint="localhost",
        )

        password = creds_provider.get_credentials()[-1]

        if username:
            init_acl_user(r, request, username, password)
        else:
            init_required_pass(r, request, password)

        r2 = _get_client(
            redis.Redis, request, flushdb=False, credential_provider=creds_provider
        )

        assert r2.ping() is True

    def test_credential_provider_no_password_success(self, r, request):
        init_acl_user(r, request, "username", "")
        r2 = _get_client(
            redis.Redis,
            request,
            flushdb=False,
            credential_provider=NoPassCredProvider(),
        )
        assert r2.ping() is True

    @pytest.mark.onlynoncluster
    def test_credential_provider_no_password_error(self, r, request):
        init_acl_user(r, request, "username", "password")
        with pytest.raises(AuthenticationError) as e:
            _get_client(
                redis.Redis,
                request,
                flushdb=False,
                credential_provider=NoPassCredProvider(),
            )
        assert e.match("invalid username-password")

    @pytest.mark.onlynoncluster
    def test_credential_provider_error_does_not_skip_authentication(self, r, request):
        init_acl_user(r, request, "username", "password")
        provider = FlakyCredProvider()
        r2 = _get_client(
            redis.Redis,
            request,
            flushdb=False,
            single_connection_client=False,
            credential_provider=provider,
        )

        with pytest.raises(RequestTokenErr):
            r2.ping()

        # The failed connection must not be reused as if the handshake had run
        assert r2.ping() is True
        assert provider.calls == 2
        assert "username" in [client["user"] for client in r.client_list()]

    @pytest.mark.onlynoncluster
    def test_password_and_username_together_with_cred_provider_raise_error(
        self, r, request
    ):
        init_acl_user(r, request, "username", "password")
        cred_provider = UsernamePasswordCredentialProvider(
            username="username", password="password"
        )
        with pytest.raises(DataError) as e:
            _get_client(
                redis.Redis,
                request,
                flushdb=False,
                username="username",
                password="password",
                credential_provider=cred_provider,
            )
        assert e.match(
            "'username' and 'password' cannot be passed along with "
            "'credential_provider'."
        )

    @pytest.mark.onlynoncluster
    def test_change_username_password_on_existing_connection(self, r, request):
        username = "origin_username"
        password = "origin_password"
        new_username = "new_username"
        new_password = "new_password"

        def teardown():
            r.acl_deluser(new_username)

        request.addfinalizer(teardown)

        init_acl_user(r, request, username, password)
        r2 = _get_client(
            redis.Redis, request, flushdb=False, username=username, password=password
        )
        assert r2.ping() is True
        conn = r2.connection_pool.get_connection()
        conn.send_command("PING")
        assert str_if_bytes(conn.read_response()) == "PONG"
        assert conn.username == username
        assert conn.password == password
        init_acl_user(r, request, new_username, new_password)
        conn.password = new_password
        conn.username = new_username
        conn.send_command("PING")
        assert str_if_bytes(conn.read_response()) == "PONG"


class TestUsernamePasswordCredentialProvider:
    def test_user_pass_credential_provider_acl_user_and_pass(self, r, request):
        username = "username"
        password = "password"
        provider = UsernamePasswordCredentialProvider(username, password)
        assert provider.username == username
        assert provider.password == password
        assert provider.get_credentials() == (username, password)
        init_acl_user(r, request, provider.username, provider.password)
        r2 = _get_client(
            redis.Redis, request, flushdb=False, credential_provider=provider
        )
        assert r2.ping() is True

    def test_user_pass_provider_only_password(self, r, request):
        password = "password"
        provider = UsernamePasswordCredentialProvider(password=password)
        assert provider.username == ""
        assert provider.password == password
        assert provider.get_credentials() == (password,)

        init_required_pass(r, request, password)

        r2 = _get_client(
            redis.Redis, request, flushdb=False, credential_provider=provider
        )
        assert r2.auth(provider.password) is True
        assert r2.ping() is True


@pytest.mark.fixed_client
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
    def test_re_auth_all_connections(self, credential_provider):
        mock_connection = Mock(spec=ConnectionInterface)
        mock_connection.retry = Retry(NoBackoff(), 0)
        mock_connection.host = "localhost"
        mock_connection.port = 6379
        mock_connection.db = 0
        mock_another_connection = Mock(spec=ConnectionInterface)
        mock_another_connection.host = "localhost"
        mock_another_connection.port = 6379
        mock_another_connection.db = 0
        mock_pool = Mock(spec=ConnectionPool)
        mock_pool.connection_kwargs = {
            "credential_provider": credential_provider,
        }
        mock_pool.get_connection.return_value = mock_connection
        mock_pool._available_connections = [mock_connection, mock_another_connection]
        mock_pool._lock = threading.RLock()
        auth_token = None

        def re_auth_callback(token):
            nonlocal auth_token
            auth_token = token
            with mock_pool._lock:
                for conn in mock_pool._available_connections:
                    conn.send_command("AUTH", token.try_get("oid"), token.get_value())
                    conn.read_response()

        mock_pool.re_auth_callback = re_auth_callback

        Redis(
            connection_pool=mock_pool,
            credential_provider=credential_provider,
        )

        credential_provider.get_credentials()

        # the re-auth runs in the background; poll instead of sleeping
        def reauth_done():
            assert auth_token is not None
            mock_connection.send_command.assert_has_calls(
                [call("AUTH", auth_token.try_get("oid"), auth_token.get_value())]
            )
            mock_another_connection.send_command.assert_has_calls(
                [call("AUTH", auth_token.try_get("oid"), auth_token.get_value())]
            )

        _wait_until_passes(reauth_done)

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
    def test_re_auth_partial_connections(self, credential_provider):
        mock_connection = Mock(spec=ConnectionInterface)
        mock_connection.retry = Retry(NoBackoff(), 3)
        mock_connection.host = "localhost"
        mock_connection.port = 6379
        mock_connection.db = 0
        mock_another_connection = Mock(spec=ConnectionInterface)
        mock_another_connection.retry = Retry(NoBackoff(), 3)
        mock_another_connection.host = "localhost"
        mock_another_connection.port = 6379
        mock_another_connection.db = 0
        mock_failed_connection = Mock(spec=ConnectionInterface)
        mock_failed_connection.read_response.side_effect = ConnectionError(
            "Failed auth"
        )
        mock_failed_connection.retry = Retry(NoBackoff(), 3)
        mock_failed_connection.host = "localhost"
        mock_failed_connection.port = 6379
        mock_failed_connection.db = 0
        mock_pool = Mock(spec=ConnectionPool)
        mock_pool.connection_kwargs = {
            "credential_provider": credential_provider,
        }
        mock_pool.get_connection.return_value = mock_connection
        mock_pool._available_connections = [
            mock_connection,
            mock_another_connection,
            mock_failed_connection,
        ]
        mock_pool._lock = threading.RLock()

        def _raise(error: RedisError):
            pass

        def re_auth_callback(token):
            with mock_pool._lock:
                for conn in mock_pool._available_connections:
                    conn.retry.call_with_retry(
                        lambda: conn.send_command(
                            "AUTH", token.try_get("oid"), token.get_value()
                        ),
                        lambda error: _raise(error),
                    )
                    conn.retry.call_with_retry(
                        lambda: conn.read_response(), lambda error: _raise(error)
                    )

        mock_pool.re_auth_callback = re_auth_callback

        Redis(
            connection_pool=mock_pool,
            credential_provider=credential_provider,
        )

        credential_provider.get_credentials()

        # the re-auth runs in the background; poll instead of sleeping
        def reauth_done():
            mock_connection.read_response.assert_has_calls([call()])
            mock_another_connection.read_response.assert_has_calls([call()])
            mock_failed_connection.read_response.assert_has_calls(
                [call(), call(), call()]
            )

        _wait_until_passes(reauth_done)

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
    def test_re_auth_pub_sub_in_resp3(self, credential_provider):
        mock_pubsub_connection = Mock(spec=ConnectionInterface)
        mock_pubsub_connection.get_protocol.return_value = 3
        mock_pubsub_connection.should_reconnect = Mock(return_value=False)
        mock_pubsub_connection.credential_provider = credential_provider
        mock_pubsub_connection.retry = Retry(NoBackoff(), 3)
        mock_pubsub_connection.host = "localhost"
        mock_pubsub_connection.port = 6379
        mock_pubsub_connection.db = 0
        mock_another_connection = Mock(spec=ConnectionInterface)
        mock_another_connection.retry = Retry(NoBackoff(), 3)
        mock_another_connection.host = "localhost"
        mock_another_connection.port = 6379
        mock_another_connection.db = 0

        mock_pool = Mock(spec=ConnectionPool)
        mock_pool.connection_kwargs = {
            "credential_provider": credential_provider,
        }
        mock_pool.get_connection.side_effect = [
            mock_pubsub_connection,
            mock_another_connection,
        ]
        mock_pool._available_connections = [mock_another_connection]
        mock_pool._lock = threading.RLock()
        auth_token = None

        def re_auth_callback(token):
            nonlocal auth_token
            auth_token = token
            with mock_pool._lock:
                for conn in mock_pool._available_connections:
                    conn.send_command("AUTH", token.try_get("oid"), token.get_value())
                    conn.read_response()

        mock_pool.re_auth_callback = re_auth_callback

        r = Redis(
            connection_pool=mock_pool,
            credential_provider=credential_provider,
        )
        p = r.pubsub()
        p.subscribe("test")
        credential_provider.get_credentials()

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

        _wait_until_passes(reauth_done)

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
    def test_do_not_re_auth_pub_sub_in_resp2(self, credential_provider):
        mock_pubsub_connection = Mock(spec=ConnectionInterface)
        mock_pubsub_connection.get_protocol.return_value = 2
        mock_pubsub_connection.should_reconnect = Mock(return_value=False)
        mock_pubsub_connection.credential_provider = credential_provider
        mock_pubsub_connection.retry = Retry(NoBackoff(), 3)
        mock_pubsub_connection.host = "localhost"
        mock_pubsub_connection.port = 6379
        mock_pubsub_connection.db = 0
        mock_another_connection = Mock(spec=ConnectionInterface)
        mock_another_connection.retry = Retry(NoBackoff(), 3)
        mock_another_connection.host = "localhost"
        mock_another_connection.port = 6379
        mock_another_connection.db = 0

        mock_pool = Mock(spec=ConnectionPool)
        mock_pool.connection_kwargs = {
            "credential_provider": credential_provider,
        }
        mock_pool.get_connection.side_effect = [
            mock_pubsub_connection,
            mock_another_connection,
        ]
        mock_pool._available_connections = [mock_another_connection]
        mock_pool._lock = threading.RLock()
        auth_token = None

        def re_auth_callback(token):
            nonlocal auth_token
            auth_token = token
            with mock_pool._lock:
                for conn in mock_pool._available_connections:
                    conn.send_command("AUTH", token.try_get("oid"), token.get_value())
                    conn.read_response()

        mock_pool.re_auth_callback = re_auth_callback

        r = Redis(
            connection_pool=mock_pool,
            credential_provider=credential_provider,
        )
        p = r.pubsub()
        p.subscribe("test")
        credential_provider.get_credentials()

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

        _wait_until_passes(reauth_done)

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
    def test_fails_on_token_renewal(self, credential_provider):
        credential_provider._token_mgr._idp.request_token.side_effect = [
            RequestTokenErr,
            RequestTokenErr,
            RequestTokenErr,
            RequestTokenErr,
        ]
        mock_connection = Mock(spec=ConnectionInterface)
        mock_connection.retry = Retry(NoBackoff(), 0)
        mock_connection.host = "localhost"
        mock_connection.port = 6379
        mock_connection.db = 0
        mock_another_connection = Mock(spec=ConnectionInterface)
        mock_another_connection.host = "localhost"
        mock_another_connection.port = 6379
        mock_another_connection.db = 0
        mock_pool = Mock(spec=ConnectionPool)
        mock_pool.connection_kwargs = {
            "credential_provider": credential_provider,
        }
        mock_pool.get_connection.return_value = mock_connection
        mock_pool._available_connections = [mock_connection, mock_another_connection]
        mock_pool._lock = threading.RLock()

        Redis(
            connection_pool=mock_pool,
            credential_provider=credential_provider,
        )

        with pytest.raises(RequestTokenErr):
            credential_provider.get_credentials()


@pytest.mark.onlynoncluster
@pytest.mark.cp_integration
@pytest.mark.skipif(not EntraIdCredentialsProvider, reason="requires redis-entraid")
class TestEntraIdCredentialsProvider:
    @pytest.mark.parametrize(
        "r_entra",
        [
            {
                "cred_provider_class": EntraIdCredentialsProvider,
                "single_connection_client": False,
            },
            {
                "cred_provider_class": EntraIdCredentialsProvider,
                "single_connection_client": True,
            },
            {
                "cred_provider_class": EntraIdCredentialsProvider,
                "idp_kwargs": {"auth_type": AuthType.DEFAULT_AZURE_CREDENTIAL},
            },
        ],
        ids=["pool", "single", "DefaultAzureCredential"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    @pytest.mark.cp_integration
    def test_auth_pool_with_credential_provider(self, r_entra: redis.Redis):
        assert r_entra.ping() is True

    @pytest.mark.parametrize(
        "r_entra",
        [
            {
                "cred_provider_class": EntraIdCredentialsProvider,
                "single_connection_client": False,
            },
            {
                "cred_provider_class": EntraIdCredentialsProvider,
                "single_connection_client": True,
            },
        ],
        ids=["pool", "single"],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    @pytest.mark.cp_integration
    def test_auth_pipeline_with_credential_provider(self, r_entra: redis.Redis):
        pipe = r_entra.pipeline()

        pipe.set("key", "value")
        pipe.get("key")

        assert pipe.execute() == [True, b"value"]

    @pytest.mark.parametrize(
        "r_entra",
        [
            {
                "cred_provider_class": EntraIdCredentialsProvider,
            },
        ],
        indirect=True,
    )
    @pytest.mark.onlynoncluster
    @pytest.mark.cp_integration
    def test_auth_pubsub_with_credential_provider(self, r_entra: redis.Redis):
        p = r_entra.pubsub()
        p.subscribe("entraid")

        r_entra.publish("entraid", "test")
        r_entra.publish("entraid", "test")

        assert p.get_message()["type"] == "subscribe"
        assert p.get_message()["type"] == "message"


@pytest.mark.onlycluster
@pytest.mark.cp_integration
@pytest.mark.skipif(not EntraIdCredentialsProvider, reason="requires redis-entraid")
class TestClusterEntraIdCredentialsProvider:
    @pytest.mark.parametrize(
        "r_entra",
        [
            {
                "cred_provider_class": EntraIdCredentialsProvider,
                "single_connection_client": False,
            },
            {
                "cred_provider_class": EntraIdCredentialsProvider,
                "single_connection_client": True,
            },
            {
                "cred_provider_class": EntraIdCredentialsProvider,
                "idp_kwargs": {"auth_type": AuthType.DEFAULT_AZURE_CREDENTIAL},
            },
        ],
        ids=["pool", "single", "DefaultAzureCredential"],
        indirect=True,
    )
    @pytest.mark.onlycluster
    @pytest.mark.cp_integration
    def test_auth_pool_with_credential_provider(self, r_entra: redis.Redis):
        assert r_entra.ping() is True
