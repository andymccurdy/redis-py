from datetime import datetime, timedelta, timezone
import warnings
import pytest
from redis.exceptions import DataError
import redis.utils
from redis.utils import (
    check_protocol_version,
    command_text,
    compare_versions,
    decode_field_value,
    DEFAULT_RESP_VERSION,
    deprecated_args,
    deprecated_function,
    dict_merge,
    ensure_string,
    experimental_args,
    experimental_method,
    extract_expire_flags,
    format_error_message,
    list_keys_to_dict,
    merge_result,
    pipeline,
    safe_str,
    SENTINEL,
    str_if_bytes,
    truncate_text,
)


@pytest.fixture
def filterable_args_calls(monkeypatch):
    """Count calls to the slow path the args decorators skip on clean calls."""
    calls = []
    orig_get = redis.utils._get_filterable_args

    def counting_get(*args, **kwargs):
        calls.append(args)
        return orig_get(*args, **kwargs)

    monkeypatch.setattr(redis.utils, "_get_filterable_args", counting_get)
    return calls


@pytest.mark.fixed_client
@pytest.mark.parametrize(
    "version1,version2,expected_res",
    [
        ("1.0.0", "0.9.0", -1),
        ("1.0.0", "1.0.0", 0),
        ("0.9.0", "1.0.0", 1),
        ("1.09.0", "1.9.0", 0),
        ("1.090.0", "1.9.0", -1),
        ("1", "0.9.0", -1),
        ("1", "1.0.0", 0),
    ],
    ids=[
        "version1 > version2",
        "version1 == version2",
        "version1 < version2",
        "version1 == version2 - different minor format",
        "version1 > version2 - different minor format",
        "version1 > version2 - major version only",
        "version1 == version2 - major version only",
    ],
)
def test_compare_versions(version1, version2, expected_res):
    assert compare_versions(version1, version2) == expected_res


@pytest.mark.fixed_client
class TestCheckProtocolVersion:
    """``check_protocol_version`` underpins protocol-gated features
    (caching, maintenance notifications, callback selection). It must
    treat ``None`` as ``DEFAULT_RESP_VERSION`` so callers using the empty
    default get the same answer as if they had pinned the wire protocol."""

    def test_none_resolves_to_default(self):
        # ``DEFAULT_RESP_VERSION`` is the wire version selected when the
        # caller does not pass ``protocol``; ``check_protocol_version``
        # must therefore treat ``None`` as that version.
        assert check_protocol_version(None, DEFAULT_RESP_VERSION) is True
        other = 2 if DEFAULT_RESP_VERSION == 3 else 3
        assert check_protocol_version(None, other) is False

    def test_sentinel_resolves_to_default(self):
        assert check_protocol_version(SENTINEL, DEFAULT_RESP_VERSION) is True
        other = 2 if DEFAULT_RESP_VERSION == 3 else 3
        assert check_protocol_version(SENTINEL, other) is False

    @pytest.mark.parametrize("protocol", [3, "3"])
    def test_resp3_matches(self, protocol):
        assert check_protocol_version(protocol, 3) is True
        assert check_protocol_version(protocol, 2) is False

    @pytest.mark.parametrize("protocol", [2, "2"])
    def test_resp2_matches(self, protocol):
        assert check_protocol_version(protocol, 2) is True
        assert check_protocol_version(protocol, 3) is False

    def test_invalid_string_returns_false(self):
        assert check_protocol_version("not-a-number", 3) is False


def redis_server_time(client):
    seconds, milliseconds = client.time()
    timestamp = float(f"{seconds}.{milliseconds}")
    return datetime.fromtimestamp(timestamp)


# Tests for deprecated_function decorator
@pytest.mark.fixed_client
class TestDeprecatedFunction:
    def test_sync_function_warns(self):
        @deprecated_function(reason="use new_func", version="1.0.0")
        def old_func():
            return "result"

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = old_func()
            assert result == "result"
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "old_func" in str(w[0].message)
            assert "use new_func" in str(w[0].message)
            assert "1.0.0" in str(w[0].message)

    def test_preserves_function_metadata(self):
        @deprecated_function()
        def documented_func():
            """This is the docstring."""
            pass

        assert documented_func.__name__ == "documented_func"
        assert documented_func.__doc__ == "This is the docstring."


# Tests for deprecated_args decorator
@pytest.mark.fixed_client
class TestDeprecatedArgs:
    def test_sync_function_warns_on_deprecated_arg(self):
        @deprecated_args(args_to_warn=["old_param"], reason="use new_param")
        def func_with_args(new_param=None, old_param=None):
            return new_param or old_param

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = func_with_args(old_param="value")
            assert result == "value"
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "old_param" in str(w[0].message)

    def test_positional_deprecated_arg_warns(self):
        @deprecated_args(args_to_warn=["old_param"])
        def func_with_args(new_param=None, old_param=None):
            return (new_param, old_param)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = func_with_args("new_val", "old_val")
            assert result == ("new_val", "old_val")
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "old_param" in str(w[0].message)

    def test_sync_function_no_warning_on_allowed_arg(self):
        @deprecated_args(args_to_warn=["*"], allowed_args=["allowed_param"])
        def func_with_allowed(allowed_param=None):
            return allowed_param

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = func_with_allowed(allowed_param="value")
            assert result == "value"
            assert len(w) == 0

    def test_positional_allowed_arg_no_warning(self):
        @deprecated_args(args_to_warn=["*"], allowed_args=["allowed_param"])
        def func_with_allowed(allowed_param=None):
            return allowed_param

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = func_with_allowed("value")
            assert result == "value"
            assert len(w) == 0

    def test_method_self_with_wildcard_no_warning(self):
        class Dummy:
            @deprecated_args(args_to_warn=["*"])
            def method(self, bad_param=None):
                return "ok"

        dummy = Dummy()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = dummy.method()
            assert result == "ok"
            assert len(w) == 0

    def test_wildcard_warns_all_args(self):
        @deprecated_args(args_to_warn=["*"])
        def func_all_deprecated(param1=None, param2=None):
            return (param1, param2)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = func_all_deprecated(param1="a", param2="b")
            assert result == ("a", "b")
            assert len(w) == 1
            assert "param1" in str(w[0].message) or "param2" in str(w[0].message)

    async def test_async_function_warns_on_deprecated_arg(self):
        @deprecated_args(args_to_warn=["old_param"])
        async def async_func(new_param=None, old_param=None):
            return (new_param, old_param)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = await async_func("new_val", old_param="old_val")
            assert result == ("new_val", "old_val")
            assert len(w) == 1
            assert "old_param" in str(w[0].message)

    async def test_async_function_no_warning_on_normal_call(self):
        @deprecated_args(args_to_warn=["old_param"])
        async def async_func(new_param=None, old_param=None):
            return new_param

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = await async_func("new_val")
            assert result == "new_val"
            assert len(w) == 0

    def test_fast_path_bypasses_filterable_args_construction(
        self, filterable_args_calls
    ):
        @deprecated_args(args_to_warn=["batch_size"])
        def record_metric(cmd, dur, host=None, port=None, batch_size=None):
            return (cmd, dur)

        assert record_metric("GET", 0.001, "localhost", 6379) == ("GET", 0.001)
        assert not filterable_args_calls, (
            "_get_filterable_args should not be called on clean hot path"
        )

    def test_mutable_args_input_snapshotting(self):
        # Mutating the list passed to args_to_warn or allowed_args after decorator
        # definition should not desynchronize the decorator configuration.
        warn_list = ["old_param"]
        allowed_list = ["allowed_param"]

        @deprecated_args(args_to_warn=warn_list, allowed_args=allowed_list)
        def func(allowed_param=None, old_param=None, new_param=None):
            return (allowed_param, old_param, new_param)

        warn_list.append("new_param")
        allowed_list.clear()

        # allowed_param remains allowed because allowed_list was snapshotted
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            res = func(allowed_param="val")
            assert res == ("val", None, None)
            assert len(w) == 0

        # old_param still triggers deprecation warning
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            res = func(old_param="val")
            assert res == (None, "val", None)
            assert len(w) == 1
            assert "old_param" in str(w[0].message)

    def test_wildcard_pool_get_connection_signature(self, filterable_args_calls):
        class Pool:
            @deprecated_args(args_to_warn=["*"])
            def get_connection(self, command_name=None, *keys, **options):
                return "conn"

        pool = Pool()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert pool.get_connection() == "conn"
            assert len(w) == 0
            assert not filterable_args_calls

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert pool.get_connection("GET", "key") == "conn"
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "command_name" in str(w[0].message)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert pool.get_connection(option="value") == "conn"
            assert len(w) == 1
            assert "option" in str(w[0].message)

    def test_wildcard_allowed_kwarg_skips_filterable_args(self, filterable_args_calls):
        @deprecated_args(args_to_warn=["*"], allowed_args=["allowed_param"])
        def func(allowed_param=None, old_param=None):
            return allowed_param

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert func(allowed_param="value") == "value"
            assert len(w) == 0
        assert not filterable_args_calls, (
            "_get_filterable_args should not be called for allowed args"
        )

    def test_keyword_only_and_positional_only_args(self, filterable_args_calls):
        @deprecated_args(args_to_warn=["old_pos", "old_kw"])
        def func(old_pos=None, /, new_param=None, *, old_kw=None, **options):
            return (old_pos, new_param, old_kw)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert func(new_param=1, other=2) == (None, 1, None)
            assert len(w) == 0
        assert not filterable_args_calls

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert func("val") == ("val", None, None)
            assert len(w) == 1
            assert "old_pos" in str(w[0].message)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert func(old_kw="val") == (None, None, "val")
            assert len(w) == 1
            assert "old_kw" in str(w[0].message)

    def test_wildcard_and_named_arg_warn_together(self):
        @deprecated_args(args_to_warn=["*", "old_param"])
        def func(new_param=None, old_param=None):
            return (new_param, old_param)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert func() == (None, None)
            assert len(w) == 0

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert func(old_param="val") == (None, "val")
            assert len(w) == 2
            assert all("old_param" in str(x.message) for x in w)

    def test_decorator_that_cannot_warn_keeps_wrapper(self, filterable_args_calls):
        def func(old_param=None, new_param=None):
            return (old_param, new_param)

        never_warns = deprecated_args(
            args_to_warn=["old_param"], allowed_args=["old_param"]
        )(func)
        assert never_warns is not func
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert never_warns("a", new_param="b") == ("a", "b")
            assert len(w) == 0
        assert not filterable_args_calls

        # The outer decorator sees the inner wrapper's (*args, **kwargs)
        # signature, so it warns only for keyword usage.
        stacked = deprecated_args(args_to_warn=["new_param"])(never_warns)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert stacked("a", "b") == ("a", "b")
            assert stacked(old_param="a") == ("a", None)
            assert len(w) == 0

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert stacked(new_param="b") == (None, "b")
            assert len(w) == 1
            assert "new_param" in str(w[0].message)


# Tests for experimental_method decorator
@pytest.mark.fixed_client
class TestExperimentalMethod:
    def test_sync_function_warns(self):
        @experimental_method()
        def experimental_func():
            return "experimental_result"

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = experimental_func()
            assert result == "experimental_result"
            assert len(w) == 1
            assert issubclass(w[0].category, UserWarning)
            assert "experimental_func" in str(w[0].message)


# Tests for experimental_args decorator
@pytest.mark.fixed_client
class TestExperimentalArgs:
    def test_sync_function_warns_on_experimental_arg(self):
        @experimental_args(args_to_warn=["beta_param"])
        def func_with_experimental(stable_param=None, beta_param=None):
            return stable_param or beta_param

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = func_with_experimental(beta_param="beta_value")
            assert result == "beta_value"
            assert len(w) == 1
            assert issubclass(w[0].category, UserWarning)
            assert "beta_param" in str(w[0].message)

    def test_positional_experimental_arg_warns(self):
        @experimental_args(args_to_warn=["beta_param"])
        def func_with_experimental(stable_param=None, beta_param=None):
            return (stable_param, beta_param)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = func_with_experimental("stable_val", "beta_val")
            assert result == ("stable_val", "beta_val")
            assert len(w) == 1
            assert "beta_param" in str(w[0].message)

    def test_no_warning_when_no_args_provided(self):
        @experimental_args(args_to_warn=["beta_param"])
        def func_no_args():
            return "no_args"

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = func_no_args()
            assert result == "no_args"
            assert len(w) == 0

    async def test_async_experimental_args(self):
        @experimental_args(args_to_warn=["beta_param"])
        async def async_func(stable=None, beta_param=None):
            return (stable, beta_param)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = await async_func("s", beta_param="b")
            assert result == ("s", "b")
            assert len(w) == 1
            assert "beta_param" in str(w[0].message)

    def test_experimental_fast_path_bypasses_filterable_args(
        self, filterable_args_calls
    ):
        @experimental_args(args_to_warn=["beta_param"])
        def stable_func(a, b, c=1, beta_param=None):
            return a + b + c

        assert stable_func(1, 2) == 4
        assert not filterable_args_calls, (
            "_get_filterable_args should not be called on clean hot path"
        )

    def test_wildcard_pool_get_connection_signature(self):
        class Pool:
            @experimental_args(args_to_warn=["*"])
            def get_connection(self, command_name=None, *keys, **options):
                return "conn"

        pool = Pool()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert pool.get_connection() == "conn"
            assert len(w) == 0

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert pool.get_connection("GET", "key") == "conn"
            assert len(w) == 1
            assert issubclass(w[0].category, UserWarning)
            assert "command_name" in str(w[0].message)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert pool.get_connection(option="value") == "conn"
            assert len(w) == 1
            assert "option" in str(w[0].message)


@pytest.mark.fixed_client
class TestStrIfBytes:
    def test_decodes_bytes(self):
        assert str_if_bytes(b"hello") == "hello"

    def test_passes_str_through(self):
        assert str_if_bytes("hello") == "hello"

    def test_replaces_invalid_utf8(self):
        # Undecodable bytes are replaced rather than raising.
        assert str_if_bytes(b"\xff") == "�"


@pytest.mark.fixed_client
class TestSafeStr:
    def test_stringifies_non_string_values(self):
        assert safe_str(123) == "123"
        assert safe_str(None) == "None"

    def test_decodes_bytes_first(self):
        assert safe_str(b"ab") == "ab"


@pytest.mark.fixed_client
class TestEnsureString:
    def test_decodes_bytes(self):
        assert ensure_string(b"key") == "key"

    def test_passes_str_through(self):
        assert ensure_string("key") == "key"

    def test_rejects_other_types(self):
        with pytest.raises(TypeError, match="string or bytes"):
            ensure_string(5)


@pytest.mark.fixed_client
class TestFormatErrorMessage:
    def test_no_args(self):
        assert format_error_message("host:1", Exception()) == (
            "Error connecting to host:1."
        )

    def test_single_arg(self):
        assert format_error_message("host:1", Exception("boom")) == (
            "Error boom connecting to host:1."
        )

    def test_two_args(self):
        assert format_error_message("host:1", Exception("code", "detail")) == (
            "Error code connecting to host:1. detail."
        )


@pytest.mark.fixed_client
class TestExtractExpireFlags:
    def test_no_flags_returns_empty(self):
        assert extract_expire_flags() == []

    def test_ex_as_int(self):
        assert extract_expire_flags(ex=10) == ["EX", 10]

    def test_ex_as_timedelta_uses_total_seconds(self):
        assert extract_expire_flags(ex=timedelta(minutes=1)) == ["EX", 60]

    def test_ex_as_digit_string(self):
        assert extract_expire_flags(ex="60") == ["EX", 60]

    def test_ex_invalid_raises(self):
        with pytest.raises(
            DataError, match="ex must be datetime.timedelta, int, or a digit string"
        ):
            extract_expire_flags(ex="not-a-number")

    def test_ex_float_raises_lists_accepted_types(self):
        with pytest.raises(
            DataError, match="ex must be datetime.timedelta, int, or a digit string"
        ):
            extract_expire_flags(ex=1.5)

    def test_px_as_int(self):
        assert extract_expire_flags(px=500) == ["PX", 500]

    def test_px_as_timedelta_uses_milliseconds(self):
        assert extract_expire_flags(px=timedelta(seconds=2)) == ["PX", 2000]

    def test_px_as_digit_string(self):
        assert extract_expire_flags(px="500") == ["PX", 500]

    def test_px_invalid_raises(self):
        with pytest.raises(
            DataError, match="px must be datetime.timedelta, int, or a digit string"
        ):
            extract_expire_flags(px="not-a-number")

    @pytest.mark.parametrize("option", ["ex", "px"])
    def test_non_decimal_digit_string_raises_data_error(self, option):
        # Superscript two passes isdigit() but cannot be parsed by int().
        with pytest.raises(DataError):
            extract_expire_flags(**{option: "\u00b2"})

    def test_exat_as_int(self):
        assert extract_expire_flags(exat=1700000000) == ["EXAT", 1700000000]

    def test_exat_as_datetime_uses_timestamp(self):
        when = datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc)
        assert extract_expire_flags(exat=when) == ["EXAT", int(when.timestamp())]

    def test_pxat_as_int(self):
        assert extract_expire_flags(pxat=1700000000000) == ["PXAT", 1700000000000]

    def test_ex_takes_precedence_over_px(self):
        # The flags are checked in order; ex wins when several are given.
        assert extract_expire_flags(ex=5, px=999) == ["EX", 5]


@pytest.mark.fixed_client
class TestTruncateText:
    def test_short_text_unchanged(self):
        assert truncate_text("hi") == "hi"

    def test_long_text_gets_ellipsis(self):
        result = truncate_text("hello world this is long", 10)
        assert result == "hello..."
        assert len(result) <= 10

    def test_default_max_length_truncates_long_text(self):
        # Every real call site relies on the default width of 100; make sure a
        # text longer than that is shortened without an explicit width argument.
        text = "word " * 40  # 200 characters
        result = truncate_text(text)
        assert len(result) <= 100
        assert result.endswith("...")

    def test_single_long_token_collapses_to_placeholder(self):
        # A single unbroken token wider than ``max_length``: ``textwrap.shorten``
        # has no whitespace to truncate on and does not split within a word (its
        # ``break_long_words`` does not apply to this case), so it drops the word
        # entirely and returns just the placeholder.
        assert truncate_text("a" * 200) == "..."


@pytest.mark.fixed_client
class TestCommandText:
    def test_regular_command_shows_arguments(self):
        assert command_text(("SET", b"key", 1)) == "SET key 1"

    @pytest.mark.parametrize(
        "command,expected",
        [
            (("AUTH", "user", "secret"), "AUTH <REDACTED>"),
            (("auth", b"secret"), "auth <REDACTED>"),
            (("HELLO", 3, "AUTH", "user", "secret"), "HELLO <REDACTED>"),
            (
                ("MIGRATE", "host", 6379, "", 0, 10, "AUTH", "secret", "KEYS", "k"),
                "MIGRATE <REDACTED>",
            ),
            (("ACL SETUSER", "user", ">secret"), "ACL SETUSER <REDACTED>"),
            (("ACL", "SETUSER", "user", ">secret"), "ACL SETUSER <REDACTED>"),
            (("CONFIG SET", "requirepass", "secret"), "CONFIG SET <REDACTED>"),
            (("config", "set", "masterauth", "secret"), "config set <REDACTED>"),
            (
                ("SENTINEL SET", "mymaster", "auth-pass", "secret"),
                "SENTINEL SET <REDACTED>",
            ),
        ],
    )
    def test_credential_command_arguments_redacted(self, command, expected):
        assert command_text(command) == expected

    def test_credential_command_without_arguments(self):
        assert command_text(("AUTH",)) == "AUTH"
        assert command_text(("ACL", "SETUSER")) == "ACL SETUSER"

    def test_other_acl_and_config_subcommands_show_arguments(self):
        assert command_text(("ACL", "GETUSER", "user")) == "ACL GETUSER user"
        assert command_text(("CONFIG GET", "maxmemory")) == "CONFIG GET maxmemory"

    def test_empty_command(self):
        assert command_text(()) == ""


@pytest.mark.fixed_client
class TestDecodeFieldValue:
    def test_non_bytes_value_returned_unchanged(self):
        assert decode_field_value("already-str") == "already-str"

    def test_bytes_default_decoded_to_str(self):
        assert decode_field_value(b"hi") == "hi"

    def test_per_field_encoding_is_applied(self):
        result = decode_field_value(
            b"caf\xe9", key="k", field_encodings={"k": "latin-1"}
        )
        assert result == "caf\xe9"

    def test_none_encoding_keeps_raw_bytes(self):
        assert (
            decode_field_value(b"raw", key="k", field_encodings={"k": None}) == b"raw"
        )

    def test_key_absent_from_encodings_uses_default(self):
        assert (
            decode_field_value(b"hi", key="x", field_encodings={"k": "utf-8"}) == "hi"
        )

    def test_undecodable_bytes_use_replacement_character(self):
        result = decode_field_value(b"\xff", key="k", field_encodings={"k": "utf-8"})
        assert result == "�"


@pytest.mark.fixed_client
class TestDictMerge:
    def test_merges_multiple_dicts(self):
        assert dict_merge({"a": 1, "b": 2}, {"c": 3}) == {"a": 1, "b": 2, "c": 3}

    def test_later_dicts_take_precedence(self):
        assert dict_merge({"a": 1, "b": 2}, {"b": 3, "c": 4}) == {
            "a": 1,
            "b": 3,
            "c": 4,
        }

    def test_no_args_returns_empty_dict(self):
        assert dict_merge() == {}

    def test_does_not_mutate_inputs(self):
        first = {"a": 1}
        dict_merge(first, {"a": 2})
        assert first == {"a": 1}


@pytest.mark.fixed_client
class TestMergeResult:
    def test_flattens_and_deduplicates_across_nodes(self):
        merged = merge_result("CMD", {"node1": [1, 2, 3], "node2": [3, 4]})
        assert sorted(merged) == [1, 2, 3, 4]

    def test_empty_values(self):
        assert merge_result("CMD", {"node1": []}) == []

    def test_returns_a_list(self):
        assert isinstance(merge_result("CMD", {"node1": [1], "node2": [2]}), list)


@pytest.mark.fixed_client
class TestListKeysToDict:
    def test_maps_every_key_to_the_callback(self):
        def callback():
            pass

        assert list_keys_to_dict(["GET", "SET"], callback) == {
            "GET": callback,
            "SET": callback,
        }


@pytest.mark.fixed_client
class TestPipelineContextManager:
    class _FakePipeline:
        def __init__(self):
            self.executed = False
            self.reset_calls = 0

        def execute(self):
            self.executed = True

        def reset(self):
            self.reset_calls += 1

    class _FakeRedis:
        def __init__(self, fake_pipeline):
            self._fake_pipeline = fake_pipeline

        def pipeline(self):
            return self._fake_pipeline

    def test_executes_and_resets_on_success(self):
        fake_pipeline = self._FakePipeline()
        with pipeline(self._FakeRedis(fake_pipeline)) as p:
            assert p is fake_pipeline
        assert fake_pipeline.executed is True
        assert fake_pipeline.reset_calls == 1

    def test_resets_but_does_not_execute_on_exception(self):
        # A pipeline using WATCH holds a real connection checked out from
        # the pool as soon as watch() is called - reset() is what returns
        # it. Without a guaranteed reset() on this path, that connection
        # stayed checked out until the pipeline object happened to be
        # garbage collected.
        fake_pipeline = self._FakePipeline()
        with pytest.raises(ValueError):
            with pipeline(self._FakeRedis(fake_pipeline)):
                raise ValueError("boom")
        assert fake_pipeline.executed is False
        assert fake_pipeline.reset_calls == 1
