import json
import gzip
from io import BytesIO
from typing import Any, Dict
from urllib.error import HTTPError
from urllib.parse import urlparse, parse_qs

import pytest

from redis.backoff import ExponentialWithJitterBackoff
from redis.http.http_client import HttpClient, HttpError
from redis.retry import Retry


class FakeResponse:
    def __init__(
        self, *, status: int, headers: Dict[str, str], url: str, content: bytes
    ):
        self.status = status
        self.headers = headers
        self._url = url
        self._content = content

    def read(self) -> bytes:
        return self._content

    def geturl(self) -> str:
        return self._url

    # Support context manager used by urlopen
    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


@pytest.mark.fixed_client
class TestHttpClient:
    def test_get_returns_parsed_json_and_uses_timeout(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Arrange
        base_url = "https://api.example.com/"
        path = "v1/items"
        params = {"limit": 5, "q": "hello world"}
        expected_url = f"{base_url}{path}?limit=5&q=hello+world"
        payload: Dict[str, Any] = {"items": [1, 2, 3], "ok": True}
        content = json.dumps(payload).encode("utf-8")

        captured_kwargs = {}

        def fake_urlopen(request, *, timeout=None, context=None):
            # Capture call details for assertions
            captured_kwargs["timeout"] = timeout
            captured_kwargs["context"] = context
            # Assert the request was constructed correctly
            assert getattr(request, "method", "").upper() == "GET"
            assert request.full_url == expected_url
            # Return a successful response
            return FakeResponse(
                status=200,
                headers={"Content-Type": "application/json; charset=utf-8"},
                url=expected_url,
                content=content,
            )

        # Patch the urlopen used inside HttpClient
        monkeypatch.setattr("redis.http.http_client.urlopen", fake_urlopen)

        client = HttpClient(base_url=base_url)

        # Act
        result = client.get(
            path, params=params, timeout=12.34
        )  # default expect_json=True

        # Assert
        assert result == payload
        assert pytest.approx(captured_kwargs["timeout"], rel=1e-6) == 12.34
        # HTTPS -> a context should be provided (created by ssl.create_default_context)
        assert captured_kwargs["context"] is not None

    def test_get_handles_gzip_response(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Arrange
        base_url = "https://api.example.com/"
        path = "gzip-endpoint"
        expected_url = f"{base_url}{path}"
        payload = {"message": "compressed ok"}
        raw = json.dumps(payload).encode("utf-8")
        gzipped = gzip.compress(raw)

        def fake_urlopen(request, *, timeout=None, context=None):
            # Return gzipped content with appropriate header
            return FakeResponse(
                status=200,
                headers={
                    "Content-Type": "application/json; charset=utf-8",
                    "Content-Encoding": "gzip",
                },
                url=expected_url,
                content=gzipped,
            )

        monkeypatch.setattr("redis.http.http_client.urlopen", fake_urlopen)

        client = HttpClient(base_url=base_url)

        # Act
        result = client.get(path)  # expect_json=True by default

        # Assert
        assert result == payload

    def test_get_retries_on_retryable_http_errors_and_succeeds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Arrange: configure limited retries so we can assert attempts
        retry_policy = Retry(
            backoff=ExponentialWithJitterBackoff(base=0, cap=0), retries=2
        )  # 2 retries -> up to 3 attempts
        base_url = "https://api.example.com/"
        path = "sometimes-busy"
        expected_url = f"{base_url}{path}"
        payload = {"ok": True}
        success_content = json.dumps(payload).encode("utf-8")

        call_count = {"n": 0}

        def make_http_error(url: str, code: int, body: bytes = b"busy"):
            # Provide a file-like object for .read() when HttpClient tries to read error content
            fp = BytesIO(body)
            return HTTPError(
                url=url,
                code=code,
                msg="Service Unavailable",
                hdrs={"Content-Type": "text/plain"},
                fp=fp,
            )

        def flaky_urlopen(request, *, timeout=None, context=None):
            call_count["n"] += 1
            # Fail with a retryable status (503) for the first two calls, then succeed
            if call_count["n"] <= 2:
                raise make_http_error(expected_url, 503)
            return FakeResponse(
                status=200,
                headers={"Content-Type": "application/json; charset=utf-8"},
                url=expected_url,
                content=success_content,
            )

        monkeypatch.setattr("redis.http.http_client.urlopen", flaky_urlopen)

        client = HttpClient(base_url=base_url, retry=retry_policy)

        # Act
        result = client.get(path)

        # Assert: should have retried twice (total 3 attempts) and finally returned parsed JSON
        assert result == payload
        assert call_count["n"] == retry_policy.get_retries() + 1

    def test_post_sends_json_body_and_parses_response(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Arrange
        base_url = "https://api.example.com/"
        path = "v1/create"
        expected_url = f"{base_url}{path}"
        send_payload = {"a": 1, "b": "x"}
        recv_payload = {"id": 10, "ok": True}
        recv_content = json.dumps(recv_payload, separators=(",", ":")).encode("utf-8")

        def fake_urlopen(request, *, timeout=None, context=None):
            # Verify method, URL and headers
            assert getattr(request, "method", "").upper() == "POST"
            assert request.full_url == expected_url
            # Content-Type should be auto-set for string JSON body
            assert (
                request.headers.get("Content-type") == "application/json; charset=utf-8"
            )
            # Body should be already UTF-8 encoded JSON with no spaces
            assert request.data == json.dumps(
                send_payload, ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8")
            return FakeResponse(
                status=200,
                headers={"Content-Type": "application/json; charset=utf-8"},
                url=expected_url,
                content=recv_content,
            )

        monkeypatch.setattr("redis.http.http_client.urlopen", fake_urlopen)

        client = HttpClient(base_url=base_url)

        # Act
        result = client.post(path, json_body=send_payload)

        # Assert
        assert result == recv_payload

    def test_post_with_raw_data_and_custom_headers(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Arrange
        base_url = "https://api.example.com/"
        path = "upload"
        expected_url = f"{base_url}{path}"
        raw_data = b"\x00\x01BINARY"
        custom_headers = {"Content-type": "application/octet-stream", "X-extra": "1"}
        recv_payload = {"status": "ok"}

        def fake_urlopen(request, *, timeout=None, context=None):
            assert getattr(request, "method", "").upper() == "POST"
            assert request.full_url == expected_url
            # Ensure our provided headers are present
            assert request.headers.get("Content-type") == "application/octet-stream"
            assert request.headers.get("X-extra") == "1"
            assert request.data == raw_data
            return FakeResponse(
                status=200,
                headers={"Content-Type": "application/json"},
                url=expected_url,
                content=json.dumps(recv_payload).encode("utf-8"),
            )

        monkeypatch.setattr("redis.http.http_client.urlopen", fake_urlopen)

        client = HttpClient(base_url=base_url)
        # Act
        result = client.post(path, data=raw_data, headers=custom_headers)

        # Assert
        assert result == recv_payload

    def test_delete_returns_http_response_when_expect_json_false(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Arrange
        base_url = "https://api.example.com/"
        path = "v1/resource/42"
        expected_url = f"{base_url}{path}"
        body = b"deleted"

        def fake_urlopen(request, *, timeout=None, context=None):
            assert getattr(request, "method", "").upper() == "DELETE"
            assert request.full_url == expected_url
            return FakeResponse(
                status=204,
                headers={"Content-Type": "text/plain"},
                url=expected_url,
                content=body,
            )

        monkeypatch.setattr("redis.http.http_client.urlopen", fake_urlopen)
        client = HttpClient(base_url=base_url)

        # Act
        resp = client.delete(path, expect_json=False)

        # Assert
        assert resp.status == 204
        assert resp.url == expected_url
        assert resp.content == body

    def test_put_raises_http_error_on_non_success(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Arrange
        base_url = "https://api.example.com/"
        path = "v1/update/1"
        expected_url = f"{base_url}{path}"

        def make_http_error(url: str, code: int, body: bytes = b"not found"):
            fp = BytesIO(body)
            return HTTPError(
                url=url,
                code=code,
                msg="Not Found",
                hdrs={"Content-Type": "text/plain"},
                fp=fp,
            )

        def fake_urlopen(request, *, timeout=None, context=None):
            raise make_http_error(expected_url, 404)

        monkeypatch.setattr("redis.http.http_client.urlopen", fake_urlopen)
        client = HttpClient(base_url=base_url)

        # Act / Assert
        with pytest.raises(HttpError) as exc:
            client.put(path, json_body={"x": 1})
        assert exc.value.status == 404
        assert exc.value.url == expected_url

    def test_patch_with_params_encodes_query(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Arrange
        base_url = "https://api.example.com/"
        path = "v1/edit"
        params = {"tag": ["a", "b"], "q": "hello world"}

        captured_url = {"u": None}

        def fake_urlopen(request, *, timeout=None, context=None):
            captured_url["u"] = request.full_url
            return FakeResponse(
                status=200,
                headers={"Content-Type": "application/json"},
                url=request.full_url,
                content=json.dumps({"ok": True}).encode("utf-8"),
            )

        monkeypatch.setattr("redis.http.http_client.urlopen", fake_urlopen)

        client = HttpClient(base_url=base_url)
        client.patch(path, params=params)  # We don't care about response here

        # Assert query parameters regardless of ordering
        parsed = urlparse(captured_url["u"])
        qs = parse_qs(parsed.query)
        assert qs["q"] == ["hello world"]
        assert qs["tag"] == ["a", "b"]

    def test_request_low_level_headers_auth_and_timeout_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Arrange: use plain HTTP to verify no TLS context, and check default timeout used
        base_url = "http://example.com/"
        path = "ping"
        captured = {
            "timeout": None,
            "context": "unset",
            "headers": None,
            "method": None,
        }

        def fake_urlopen(request, *, timeout=None, context=None):
            captured["timeout"] = timeout
            captured["context"] = context
            captured["headers"] = dict(request.headers)
            captured["method"] = getattr(request, "method", "").upper()
            return FakeResponse(
                status=200,
                headers={"Content-Type": "application/json"},
                url=request.full_url,
                content=json.dumps({"pong": True}).encode("utf-8"),
            )

        monkeypatch.setattr("redis.http.http_client.urlopen", fake_urlopen)

        client = HttpClient(base_url=base_url, auth_basic=("user", "pass"))
        resp = client.request("GET", path)

        # Assert
        assert resp.status == 200
        assert captured["method"] == "GET"
        assert captured["context"] is None  # no TLS for http
        assert (
            pytest.approx(captured["timeout"], rel=1e-6) == client.timeout
        )  # default used
        # Check some default headers and Authorization presence
        headers = {k.lower(): v for k, v in captured["headers"].items()}
        assert "authorization" in headers and headers["authorization"].startswith(
            "Basic "
        )
        assert headers.get("accept") == "application/json"
        assert "gzip" in headers.get("accept-encoding", "").lower()
        assert "user-agent" in headers
