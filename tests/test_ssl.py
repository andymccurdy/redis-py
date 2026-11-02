import datetime
import os
import socket
import ssl
import threading
import time
from urllib.parse import unquote, urlparse

import pytest
import redis
from redis.backoff import NoBackoff
from redis.exceptions import AuthorizationError, ConnectionError, RedisError
from redis.retry import Retry

from .conftest import (
    skip_if_cryptography,
    skip_if_nocryptography,
    skip_if_server_version_lt,
)
from .ssl_utils import CertificateType, get_tls_certificates, CN_USERNAME


@pytest.mark.ssl
class TestSSL:
    """Tests for SSL connections

    This relies on the --redis-ssl-url purely for rebuilding the client
    and connecting to the appropriate port.
    """

    @pytest.fixture(autouse=True)
    def _set_ssl_certs(self, request):
        self.tls_cert_subdir = request.session.config.REDIS_INFO["tls_cert_subdir"]
        self.client_certs = get_tls_certificates(self.tls_cert_subdir)
        self.server_certs = get_tls_certificates(
            self.tls_cert_subdir, cert_type=CertificateType.server
        )

    def test_ssl_with_invalid_cert(self, request):
        ssl_url = request.config.option.redis_ssl_url
        sslclient = redis.from_url(ssl_url)
        with pytest.raises(ConnectionError) as e:
            sslclient.ping()
        assert "SSL: CERTIFICATE_VERIFY_FAILED" in str(e.value)
        sslclient.close()

    def test_ssl_connection(self, request):
        ssl_url = request.config.option.redis_ssl_url
        p = urlparse(ssl_url)[1].split(":")

        r = redis.Redis(
            host=p[0],
            port=p[1],
            ssl=True,
            ssl_cert_reqs="none",
        )
        assert r.ping()
        r.close()

    def test_ssl_connection_without_ssl(self, request):
        ssl_url = request.config.option.redis_ssl_url
        p = urlparse(ssl_url)[1].split(":")
        # the TLS port closes a plaintext connection every time, so skip the
        # default backoff retries
        r = redis.Redis(host=p[0], port=p[1], ssl=False, retry=Retry(NoBackoff(), 0))

        with pytest.raises(ConnectionError) as e:
            r.ping()
        assert "Connection closed by server" in str(e.value)
        r.close()

    def test_validating_self_signed_certificate(self, request):
        ssl_url = request.config.option.redis_ssl_url
        p = urlparse(ssl_url)[1].split(":")
        r = redis.Redis(
            host=p[0],
            port=p[1],
            ssl=True,
            ssl_certfile=self.client_certs.certfile,
            ssl_keyfile=self.client_certs.keyfile,
            ssl_cert_reqs="required",
            ssl_ca_certs=self.client_certs.ca_certfile,
        )
        assert r.ping()
        r.close()

    def test_validating_self_signed_string_certificate(self, request):
        with open(self.client_certs.ca_certfile) as f:
            cert_data = f.read()
        ssl_url = request.config.option.redis_ssl_url
        p = urlparse(ssl_url)[1].split(":")
        r = redis.Redis(
            host=p[0],
            port=p[1],
            ssl=True,
            ssl_certfile=self.client_certs.certfile,
            ssl_keyfile=self.client_certs.keyfile,
            ssl_cert_reqs="required",
            ssl_ca_data=cert_data,
        )
        assert r.ping()
        r.close()

    def test_ssl_connection_tls12_custom_ciphers_invalid(self, request):
        ssl_url = request.config.option.redis_ssl_url
        p = urlparse(ssl_url)[1].split(":")
        r = redis.Redis(
            host=p[0],
            port=p[1],
            ssl=True,
            ssl_cert_reqs="none",
            ssl_min_version=ssl.TLSVersion.TLSv1_2,
            ssl_ciphers="foo:bar",
            # an invalid cipher list never connects, so skip the backoff retries
            retry=Retry(NoBackoff(), 0),
        )
        with pytest.raises(RedisError) as e:
            r.ping()
        assert "No cipher can be selected" in str(e.value)
        r.close()

    def test_ssl_connection_tls13_custom_ciphers(self, request):
        # TLSv1.3 suites cannot be set through the cipher list, so a list of only
        # TLSv1.3 suites leaves no cipher to select
        ssl_ciphers = "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256"
        ssl_url = request.config.option.redis_ssl_url
        p = urlparse(ssl_url)[1].split(":")
        r = redis.Redis(
            host=p[0],
            port=p[1],
            ssl=True,
            ssl_cert_reqs="none",
            ssl_min_version=ssl.TLSVersion.TLSv1_2,
            ssl_ciphers=ssl_ciphers,
            # the cipher list is rejected on every attempt, so skip the backoff
            retry=Retry(NoBackoff(), 0),
        )
        with pytest.raises(RedisError) as e:
            r.ping()
        assert "No cipher can be selected" in str(e.value)
        r.close()

    def _create_oscp_conn(self, request):
        ssl_url = request.config.option.redis_ssl_url
        p = urlparse(ssl_url)[1].split(":")
        r = redis.Redis(
            host=p[0],
            port=p[1],
            ssl=True,
            ssl_certfile=self.client_certs.certfile,
            ssl_keyfile=self.client_certs.keyfile,
            ssl_cert_reqs="required",
            ssl_ca_certs=self.client_certs.ca_certfile,
            ssl_validate_ocsp=True,
            # OCSP validation fails on every attempt, so skip the backoff retries
            retry=Retry(NoBackoff(), 0),
        )
        return r

    @skip_if_cryptography()
    def test_ssl_ocsp_called(self, request):
        r = self._create_oscp_conn(request)
        with pytest.raises(RedisError) as e:
            r.ping()
        assert "cryptography is not installed" in str(e.value)
        r.close()

    @skip_if_nocryptography()
    def test_ssl_ocsp_called_withcrypto(self, request):
        r = self._create_oscp_conn(request)
        with pytest.raises(ConnectionError) as e:
            assert r.ping()
        assert "No AIA information present in ssl certificate" in str(e.value)
        r.close()

    @pytest.mark.fixed_client
    @skip_if_nocryptography()
    def test_valid_ocsp_cert_http(self):
        from redis.ocsp import OCSPVerifier

        hostnames = ["github.com", "aws.amazon.com", "ynet.co.il"]
        for hostname in hostnames:
            context = ssl.create_default_context()
            with socket.create_connection((hostname, 443)) as sock:
                with context.wrap_socket(sock, server_hostname=hostname) as wrapped:
                    ocsp = OCSPVerifier(wrapped, hostname, 443)
                    assert ocsp.is_valid()

    @pytest.mark.fixed_client
    @skip_if_nocryptography()
    def test_revoked_ocsp_certificate(self):
        from redis.ocsp import OCSPVerifier

        context = ssl.create_default_context()
        hostname = "revoked.badssl.com"
        with socket.create_connection((hostname, 443)) as sock:
            with context.wrap_socket(sock, server_hostname=hostname) as wrapped:
                ocsp = OCSPVerifier(wrapped, hostname, 443)
                with pytest.raises(ConnectionError) as e:
                    assert ocsp.is_valid()
                assert "REVOKED" in str(e.value)

    @pytest.mark.fixed_client
    @skip_if_nocryptography()
    def test_unauthorized_ocsp(self):
        from redis.ocsp import OCSPVerifier

        context = ssl.create_default_context()
        hostname = "stackoverflow.com"
        with socket.create_connection((hostname, 443)) as sock:
            with context.wrap_socket(sock, server_hostname=hostname) as wrapped:
                ocsp = OCSPVerifier(wrapped, hostname, 443)
                with pytest.raises(ConnectionError):
                    ocsp.is_valid()

    @pytest.mark.fixed_client
    @skip_if_nocryptography()
    def test_ocsp_not_present_in_response(self):
        from redis.ocsp import OCSPVerifier

        context = ssl.create_default_context()
        hostname = "google.co.il"
        with socket.create_connection((hostname, 443)) as sock:
            with context.wrap_socket(sock, server_hostname=hostname) as wrapped:
                ocsp = OCSPVerifier(wrapped, hostname, 443)
                with pytest.raises(ConnectionError) as e:
                    assert ocsp.is_valid()
                assert "from the" in str(e.value)

    @skip_if_nocryptography()
    def test_mock_ocsp_staple(self, request):
        import OpenSSL

        ssl_url = request.config.option.redis_ssl_url
        p = urlparse(ssl_url)[1].split(":")
        r = redis.Redis(
            host=p[0],
            port=p[1],
            ssl=True,
            ssl_certfile=self.client_certs.certfile,
            ssl_keyfile=self.client_certs.keyfile,
            ssl_cert_reqs="required",
            ssl_ca_certs=self.client_certs.ca_certfile,
            ssl_validate_ocsp=True,
            ssl_ocsp_context=p,  # just needs to not be none
            # OCSP validation fails on every attempt, so skip the backoff retries
            retry=Retry(NoBackoff(), 0),
        )

        with pytest.raises(RedisError):
            r.ping()
        r.close()

        ctx = OpenSSL.SSL.Context(OpenSSL.SSL.SSLv23_METHOD)
        ctx.use_certificate_file(self.client_certs.certfile)
        ctx.use_privatekey_file(self.client_certs.keyfile)

        r = redis.Redis(
            host=p[0],
            port=p[1],
            ssl=True,
            ssl_certfile=self.client_certs.certfile,
            ssl_keyfile=self.client_certs.keyfile,
            ssl_cert_reqs="required",
            ssl_ca_certs=self.client_certs.ca_certfile,
            ssl_ocsp_context=ctx,
            ssl_ocsp_expected_cert=open(self.server_certs.ca_certfile, "rb").read(),
            ssl_validate_ocsp_stapled=True,
            # no OCSP response is ever stapled, so skip the backoff retries
            retry=Retry(NoBackoff(), 0),
        )

        with pytest.raises(ConnectionError) as e:
            r.ping()
        assert "no ocsp response present" in str(e.value)
        r.close()

        r = redis.Redis(
            host=p[0],
            port=p[1],
            ssl=True,
            ssl_certfile=self.client_certs.certfile,
            ssl_keyfile=self.client_certs.keyfile,
            ssl_cert_reqs="required",
            ssl_ca_certs=self.client_certs.ca_certfile,
            ssl_validate_ocsp_stapled=True,
            # no OCSP response is ever stapled, so skip the backoff retries
            retry=Retry(NoBackoff(), 0),
        )

        with pytest.raises(ConnectionError) as e:
            r.ping()
        assert "no ocsp response present" in str(e.value)
        r.close()

    def test_cert_reqs_none_with_check_hostname(self, request):
        """Test that when ssl_cert_reqs=none is used with ssl_check_hostname=True,
        the connection is created successfully with check_hostname internally set to False"""
        ssl_url = request.config.option.redis_ssl_url
        parsed_url = urlparse(ssl_url)
        r = redis.Redis(
            host=parsed_url.hostname,
            port=parsed_url.port,
            ssl=True,
            ssl_cert_reqs="none",
            # Check that ssl_check_hostname is ignored, when ssl_cert_reqs=none
            ssl_check_hostname=True,
        )
        try:
            # Connection should be successful
            assert r.ping()
            # check_hostname should have been automatically set to False
            assert r.connection_pool.connection_class == redis.SSLConnection
            conn = r.connection_pool.make_connection()
            assert conn.check_hostname is False
        finally:
            r.close()

    def test_ssl_verify_flags_applied_to_context(self, request):
        """
        Test that ssl_include_verify_flags and ssl_exclude_verify_flags
        are properly applied to the SSL context
        """
        ssl_url = request.config.option.redis_ssl_url
        parsed_url = urlparse(ssl_url)

        # Test with specific SSL verify flags
        ssl_include_verify_flags = [
            ssl.VerifyFlags.VERIFY_CRL_CHECK_LEAF,  # Disable strict verification
            ssl.VerifyFlags.VERIFY_CRL_CHECK_CHAIN,  # Enable partial chain
        ]

        ssl_exclude_verify_flags = [
            ssl.VerifyFlags.VERIFY_X509_STRICT,  # Disable trusted first
        ]

        r = redis.Redis(
            host=parsed_url.hostname,
            port=parsed_url.port,
            ssl=True,
            ssl_cert_reqs="none",
            ssl_include_verify_flags=ssl_include_verify_flags,
            ssl_exclude_verify_flags=ssl_exclude_verify_flags,
        )

        try:
            # Get the connection to trigger SSL context creation
            conn = r.connection_pool.get_connection()
            assert isinstance(conn, redis.SSLConnection)

            # Verify the flags were processed by checking they're stored in connection
            assert conn.ssl_include_verify_flags is not None
            assert len(conn.ssl_include_verify_flags) == 2

            assert conn.ssl_exclude_verify_flags is not None
            assert len(conn.ssl_exclude_verify_flags) == 1

            # Check each flag individually
            for flag in ssl_include_verify_flags:
                assert flag in conn.ssl_include_verify_flags, (
                    f"Flag {flag} not found in stored ssl_include_verify_flags"
                )
            for flag in ssl_exclude_verify_flags:
                assert flag in conn.ssl_exclude_verify_flags, (
                    f"Flag {flag} not found in stored ssl_exclude_verify_flags"
                )

            # Test the actual SSL context created by the connection
            # We need to create a mock socket and call _wrap_socket_with_ssl to get the context
            import socket
            import unittest.mock

            mock_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                # Mock the wrap_socket method to capture the context
                captured_context = None

                def capture_context_wrap_socket(context_self, sock, **_kwargs):
                    nonlocal captured_context
                    captured_context = context_self
                    # Don't actually wrap the socket, just return the original socket
                    # to avoid connection errors
                    return sock

                with unittest.mock.patch.object(
                    ssl.SSLContext, "wrap_socket", capture_context_wrap_socket
                ):
                    try:
                        conn._wrap_socket_with_ssl(mock_sock)
                    except Exception:
                        # We expect this to potentially fail since we're not actually connecting
                        # but we should have captured the context
                        pass

                # Validate that we captured a context and it has the correct flags applied
                assert captured_context is not None, "SSL context was not captured"

                # Verify that VERIFY_X509_STRICT was disabled (bit cleared)
                assert not (
                    captured_context.verify_flags & ssl.VerifyFlags.VERIFY_X509_STRICT
                ), "VERIFY_X509_STRICT should be disabled but is enabled"

                # Verify that VERIFY_CRL_CHECK_CHAIN was enabled (bit set)
                assert (
                    captured_context.verify_flags
                    & ssl.VerifyFlags.VERIFY_CRL_CHECK_CHAIN
                ), "VERIFY_CRL_CHECK_CHAIN should be enabled but is disabled"

            finally:
                mock_sock.close()

        finally:
            r.close()

    @skip_if_server_version_lt("8.5.0")
    def test_ssl_authenticate_with_client_cert(self, request, r):
        """Test that when client certificate is used for authentication,
        the connection is created successfully"""

        try:
            # Non SSL client, to setup ACL
            assert r.acl_setuser(
                CN_USERNAME,
                enabled=True,
                reset=True,
                passwords=["+clientpass"],
                keys=["*"],
                commands=["+acl"],
            )
        finally:
            r.close()

        ssl_url = request.config.option.redis_ssl_url
        p = urlparse(ssl_url)[1].split(":")
        client_cn_cert, client_cn_key, ca_cert = get_tls_certificates(
            self.tls_cert_subdir, CertificateType.client_cn
        )
        r = redis.Redis(
            host=p[0],
            port=p[1],
            ssl=True,
            ssl_certfile=client_cn_cert,
            ssl_keyfile=client_cn_key,
            ssl_cert_reqs="required",
            ssl_ca_certs=ca_cert,
        )
        try:
            assert r.acl_whoami() == CN_USERNAME
        finally:
            r.close()


@pytest.mark.fixed_client
class TestOCSPCertificateBinding:
    """An ocsp response is only about the certificate its CertID names, and
    is only current between its this_update and next_update times."""

    @staticmethod
    def _make_certificate(
        common_name,
        serial,
        signing_key=None,
        ca=False,
        ocsp_signing=False,
        rsa_key=False,
        rsa_padding=None,
        expired=False,
        aia=False,
    ):
        pytest.importorskip("cryptography")

        from cryptography import x509
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import ec, rsa
        from cryptography.x509.oid import (
            AuthorityInformationAccessOID,
            ExtendedKeyUsageOID,
            NameOID,
        )

        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        if expired:
            not_before = now - datetime.timedelta(days=400)
            not_after = now - datetime.timedelta(days=30)
        else:
            not_before = now - datetime.timedelta(days=1)
            not_after = now + datetime.timedelta(days=365)
        if rsa_key:
            key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        else:
            key = ec.generate_private_key(ec.SECP256R1())
        builder = (
            x509.CertificateBuilder()
            .subject_name(
                x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
            )
            .issuer_name(
                x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Test CA")])
            )
            .public_key(key.public_key())
            .serial_number(serial)
            .not_valid_before(not_before)
            .not_valid_after(not_after)
        )
        if ca:
            builder = builder.add_extension(
                x509.BasicConstraints(ca=True, path_length=None), critical=True
            )
        if ocsp_signing:
            builder = builder.add_extension(
                x509.ExtendedKeyUsage([ExtendedKeyUsageOID.OCSP_SIGNING]),
                critical=False,
            )
        if aia:
            builder = builder.add_extension(
                x509.AuthorityInformationAccess(
                    [
                        x509.AccessDescription(
                            AuthorityInformationAccessOID.OCSP,
                            x509.UniformResourceIdentifier("http://ocsp.test/"),
                        ),
                        x509.AccessDescription(
                            AuthorityInformationAccessOID.CA_ISSUERS,
                            x509.UniformResourceIdentifier("http://ca.test/ca.der"),
                        ),
                    ]
                ),
                critical=False,
            )
        if rsa_padding is not None:
            signed = builder.sign(
                signing_key or key, hashes.SHA256(), rsa_padding=rsa_padding
            )
        else:
            signed = builder.sign(signing_key or key, hashes.SHA256())
        return signed, key

    @staticmethod
    def _good_response(ca, ca_key, target, this_update, next_update, responder=None):
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.x509 import ocsp

        # A delegated responder signs with its own key and ships its
        # certificate in the response.
        signer, signer_key = responder or (ca, ca_key)
        builder = (
            ocsp.OCSPResponseBuilder()
            .add_response(
                cert=target,
                issuer=ca,
                algorithm=hashes.SHA256(),
                cert_status=ocsp.OCSPCertStatus.GOOD,
                this_update=this_update,
                next_update=next_update,
                revocation_time=None,
                revocation_reason=None,
            )
            .responder_id(ocsp.OCSPResponderEncoding.NAME, signer)
        )
        if responder is not None:
            builder = builder.certificates([signer])
        response = builder.sign(signer_key, hashes.SHA256())
        return response.public_bytes(serialization.Encoding.DER)

    @classmethod
    def _certificates_and_responses(cls):
        """Build a CA, the certificate under check, an unrelated certificate
        from the same issuer, and a GOOD ocsp response for each of them.
        """
        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        ca, ca_key = cls._make_certificate("Test CA", 1, ca=True)
        cert, _ = cls._make_certificate("node", 111, signing_key=ca_key)
        other, _ = cls._make_certificate("other", 999, signing_key=ca_key)

        # this_update a day back rather than an hour so the pair stays current
        # regardless of the host timezone.
        this_update = now - datetime.timedelta(days=1)
        next_update = now + datetime.timedelta(days=1)
        good_cert = cls._good_response(ca, ca_key, cert, this_update, next_update)
        good_other = cls._good_response(ca, ca_key, other, this_update, next_update)
        return ca, cert, good_cert, good_other

    def test_check_certificate_accepts_matching_response(self):
        from redis.ocsp import _check_certificate

        ca, cert, good_for_cert, _ = self._certificates_and_responses()

        assert _check_certificate(ca, cert, good_for_cert) is True

    def test_check_certificate_rejects_response_for_other_serial(self):
        # A validly signed GOOD response for a different, non-revoked
        # certificate from the same issuer must not be accepted for the
        # certificate under check.
        from redis.ocsp import _check_certificate

        ca, cert, _, good_for_other = self._certificates_and_responses()

        with pytest.raises(ConnectionError) as e:
            _check_certificate(ca, cert, good_for_other)
        assert "serial number does not match" in str(e.value)

    def test_check_certificate_accepts_delegated_responder(self):
        from redis.ocsp import _check_certificate

        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        ca, ca_key = self._make_certificate("Test CA", 1, ca=True)
        cert, _ = self._make_certificate("node", 111, signing_key=ca_key)
        responder = self._make_certificate(
            "Test OCSP", 5, signing_key=ca_key, ocsp_signing=True
        )
        response = self._good_response(
            ca,
            ca_key,
            cert,
            now - datetime.timedelta(days=1),
            now + datetime.timedelta(days=1),
            responder=responder,
        )

        assert _check_certificate(ca, cert, response) is True

    def test_check_certificate_rejects_expired_delegated_responder(self):
        # A delegate is normally exempt from revocation checking, so once its
        # certificate has expired its key must no longer be trusted.
        from redis.ocsp import _check_certificate

        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        ca, ca_key = self._make_certificate("Test CA", 1, ca=True)
        cert, _ = self._make_certificate("node", 111, signing_key=ca_key)
        responder = self._make_certificate(
            "Test OCSP", 5, signing_key=ca_key, ocsp_signing=True, expired=True
        )
        response = self._good_response(
            ca,
            ca_key,
            cert,
            now - datetime.timedelta(days=1),
            now + datetime.timedelta(days=1),
            responder=responder,
        )

        with pytest.raises(ConnectionError) as e:
            _check_certificate(ca, cert, response)
        assert "outside its validity period" in str(e.value)

    def test_check_certificate_accepts_rsa_pss_signed_certificate(self):
        # An RSA issuer may sign with RSASSA-PSS rather than PKCS1v15; the
        # issuance check must accept it.
        pytest.importorskip("cryptography", minversion="41.0.0")

        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding

        from redis.ocsp import _check_certificate

        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        pss = padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        )
        ca, ca_key = self._make_certificate("Test CA", 1, ca=True, rsa_key=True)
        cert, _ = self._make_certificate(
            "node", 111, signing_key=ca_key, rsa_padding=pss
        )
        response = self._good_response(
            ca,
            ca_key,
            cert,
            now - datetime.timedelta(days=1),
            now + datetime.timedelta(days=1),
        )

        assert _check_certificate(ca, cert, response) is True

    def test_check_certificate_rejects_delegated_responder_not_from_issuer(self):
        # A self-signed responder certificate that only claims the issuer's
        # name, and so can sign anything, must not be trusted as a delegate.
        from redis.ocsp import _check_certificate

        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        ca, ca_key = self._make_certificate("Test CA", 1, ca=True)
        cert, _ = self._make_certificate("node", 111, signing_key=ca_key)
        forged = self._make_certificate("Test OCSP", 5, ocsp_signing=True)
        response = self._good_response(
            ca,
            ca_key,
            cert,
            now - datetime.timedelta(days=1),
            now + datetime.timedelta(days=1),
            responder=forged,
        )

        with pytest.raises(ConnectionError) as e:
            _check_certificate(ca, cert, response)
        assert "was not issued by" in str(e.value)

    def test_check_certificate_rejects_issuer_that_did_not_sign_cert(self):
        # The issuer certificate comes from the peer chain or an AIA fetch; one
        # that merely carries the right name must not vouch for the cert.
        from redis.ocsp import _check_certificate

        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        _, ca_key = self._make_certificate("Test CA", 1, ca=True)
        cert, _ = self._make_certificate("node", 111, signing_key=ca_key)
        fake_ca, fake_ca_key = self._make_certificate("Test CA", 1, ca=True)
        response = self._good_response(
            fake_ca,
            fake_ca_key,
            cert,
            now - datetime.timedelta(days=1),
            now + datetime.timedelta(days=1),
        )

        with pytest.raises(ConnectionError) as e:
            _check_certificate(fake_ca, cert, response)
        assert "was not issued by" in str(e.value)

    def test_check_certificate_rejects_stale_response_west_of_utc(self):
        # next_update is already in the past: the response is stale and must be
        # rejected. this_update sits well back so it clears the "issued in the
        # future" check even in a far-west timezone. The freshness comparison
        # must be done in UTC; comparing against local time accepted this stale
        # response by the host's offset west of UTC.
        if not hasattr(time, "tzset"):
            pytest.skip("tzset is not available on this platform")

        from redis.ocsp import _check_certificate

        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        ca, ca_key = self._make_certificate("Test CA", 1, ca=True)
        cert, _ = self._make_certificate("node", 111, signing_key=ca_key)
        stale = self._good_response(
            ca,
            ca_key,
            cert,
            now - datetime.timedelta(hours=13),
            now - datetime.timedelta(hours=1),
        )

        original_tz = os.environ.get("TZ")
        try:
            # Etc/GMT+12 is UTC-12; the stale next_update (an hour back in UTC)
            # still looked an hour in the future to a local-time comparison.
            os.environ["TZ"] = "Etc/GMT+12"
            time.tzset()
            with pytest.raises(ConnectionError) as e:
                _check_certificate(ca, cert, stale)
            assert "in the past" in str(e.value)
        finally:
            if original_tz is None:
                os.environ.pop("TZ", None)
            else:
                os.environ["TZ"] = original_tz
            time.tzset()

    @staticmethod
    def _serve_tls(handshakes, sni=None):
        """Accept one loopback TLS connection per (certificate, key, issuer,
        staple) entry, in order, and return the listening port. A client that
        sends a server name found in sni is given that entry instead."""
        OpenSSL = pytest.importorskip("OpenSSL")

        def context(cert, key, issuer, staple):
            ctx = OpenSSL.SSL.Context(OpenSSL.SSL.TLS_SERVER_METHOD)
            ctx.use_certificate(OpenSSL.crypto.X509.from_cryptography(cert))
            ctx.use_privatekey(OpenSSL.crypto.PKey.from_cryptography_key(key))
            ctx.add_extra_chain_cert(OpenSSL.crypto.X509.from_cryptography(issuer))
            if staple is not None:
                ctx.set_ocsp_server_callback(lambda *_, s=staple: s)
            return ctx

        sni_contexts = {name: context(*entry) for name, entry in (sni or {}).items()}

        def select_context(con):
            ctx = sni_contexts.get(con.get_servername())
            if ctx is not None:
                con.set_context(ctx)

        listener = socket.create_server(("127.0.0.1", 0))

        def serve():
            accepted = []
            with listener:
                for entry in handshakes:
                    raw, _ = listener.accept()
                    accepted.append(raw)
                    ctx = context(*entry)
                    ctx.set_tlsext_servername_callback(select_context)
                    con = OpenSSL.SSL.Connection(ctx, raw)
                    con.set_accept_state()
                    try:
                        con.do_handshake()
                    except OpenSSL.SSL.Error:
                        pass
            # Hold each socket until the client is done with it, so closing
            # here does not race the probe's shutdown.
            for raw in accepted:
                raw.settimeout(5)
                try:
                    while raw.recv(4096):
                        pass
                except OSError:
                    pass
                raw.close()

        threading.Thread(target=serve, daemon=True).start()
        return listener.getsockname()[1]

    def _stapled_connection(self, port, ca, host="127.0.0.1"):
        import OpenSSL
        from cryptography.hazmat.primitives import serialization

        return redis.SSLConnection(
            host=host,
            port=port,
            ssl_ca_data=ca.public_bytes(serialization.Encoding.PEM).decode(),
            ssl_check_hostname=False,
            ssl_exclude_verify_flags=[ssl.VERIFY_X509_STRICT],
            ssl_validate_ocsp_stapled=True,
            ssl_ocsp_context=OpenSSL.SSL.Context(OpenSSL.SSL.TLS_CLIENT_METHOD),
        )

    def _stapled_handshake_material(self):
        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        ca, ca_key = self._make_certificate("Test CA", 1, ca=True)
        cert, key = self._make_certificate("node", 111, signing_key=ca_key)
        other, other_key = self._make_certificate("other", 999, signing_key=ca_key)
        staples = {
            serial: self._good_response(
                ca,
                ca_key,
                target,
                now - datetime.timedelta(days=1),
                now + datetime.timedelta(days=1),
            )
            for serial, target in ((111, cert), (999, other))
        }
        return ca, (cert, key, staples[111]), (other, other_key, staples[999])

    def test_stapled_ocsp_accepts_staple_for_connection_certificate(self):
        pytest.importorskip("OpenSSL")
        ca, (cert, key, staple), _ = self._stapled_handshake_material()
        port = self._serve_tls([(cert, key, ca, None), (cert, key, ca, staple)])

        sock = self._stapled_connection(port, ca)._connect()
        sock.close()

    def test_stapled_ocsp_rejects_staple_for_other_certificate(self):
        # The staple is fetched over a second connection. A GOOD staple for
        # whatever certificate that connection presents says nothing about the
        # certificate on the socket being returned, which may be revoked.
        pytest.importorskip("OpenSSL")
        ca, (cert, key, _), (other, other_key, other_staple) = (
            self._stapled_handshake_material()
        )
        port = self._serve_tls(
            [(cert, key, ca, None), (other, other_key, ca, other_staple)]
        )

        with pytest.raises(ConnectionError) as e:
            self._stapled_connection(port, ca)._connect()
        assert "does not match" in str(e.value)

    def test_stapled_ocsp_probe_sends_server_name(self):
        # Behind SNI routing the default certificate differs from the one
        # sslsock asked for by name; the probe must ask for the same one.
        pytest.importorskip("OpenSSL")
        ca, node, (other, other_key, other_staple) = self._stapled_handshake_material()
        default = (other, other_key, ca, other_staple)
        port = self._serve_tls(
            [default, default], sni={b"localhost": (node[0], node[1], ca, node[2])}
        )

        sock = self._stapled_connection(port, ca, host="localhost")._connect()
        sock.close()

    def test_stapled_ocsp_rejects_forged_issuer_on_probe(self):
        # The probe presents the right leaf, but with a self-made issuer that
        # only copies the CA's name and signs a GOOD staple for it.
        pytest.importorskip("OpenSSL")
        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        ca, (cert, key, _), _ = self._stapled_handshake_material()
        fake_ca, fake_ca_key = self._make_certificate("Test CA", 1, ca=True)
        forged_staple = self._good_response(
            fake_ca,
            fake_ca_key,
            cert,
            now - datetime.timedelta(days=1),
            now + datetime.timedelta(days=1),
        )
        port = self._serve_tls(
            [(cert, key, ca, None), (cert, key, fake_ca, forged_staple)]
        )

        with pytest.raises(ConnectionError) as e:
            self._stapled_connection(port, ca)._connect()
        assert "was not issued by" in str(e.value)

    def test_unauthorized_ocsp_fails_closed(self, monkeypatch):
        # An unsigned UNAUTHORIZED status must not let the check move on to a
        # certificate fetched over a second handshake, GOOD as that one may be.
        import base64

        from cryptography.hazmat.primitives import serialization
        from cryptography.x509 import ocsp

        import redis.ocsp

        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        ca, ca_key = self._make_certificate("Test CA", 1, ca=True)
        cert, _ = self._make_certificate("node", 111, signing_key=ca_key, aia=True)
        other, _ = self._make_certificate("other", 999, signing_key=ca_key, aia=True)
        unauthorized = ocsp.OCSPResponseBuilder.build_unsuccessful(
            ocsp.OCSPResponseStatus.UNAUTHORIZED
        ).public_bytes(serialization.Encoding.DER)
        queries = []

        class Response:
            ok = True

            def __init__(self, content):
                self.content = content

        def get(url, headers=None):
            if url == "http://ca.test/ca.der":
                return Response(ca.public_bytes(serialization.Encoding.DER))
            request = ocsp.load_der_ocsp_request(
                base64.b64decode(unquote(urlparse(url).path.lstrip("/")))
            )
            queries.append(request.serial_number)
            if len(queries) == 1:
                return Response(unauthorized)
            target = cert if request.serial_number == cert.serial_number else other
            return Response(
                self._good_response(
                    ca,
                    ca_key,
                    target,
                    now - datetime.timedelta(days=1),
                    now + datetime.timedelta(days=1),
                )
            )

        class Socket:
            def getpeercert(self, binary_form=False):
                return cert.public_bytes(serialization.Encoding.DER)

        monkeypatch.setattr(redis.ocsp.requests, "get", get)
        direct_fetches = []

        def get_server_certificate(addr, ca_certs=None):
            direct_fetches.append(addr)
            return other.public_bytes(serialization.Encoding.PEM).decode()

        monkeypatch.setattr(
            redis.ocsp.ssl, "get_server_certificate", get_server_certificate
        )
        with pytest.raises(AuthorizationError):
            redis.ocsp.OCSPVerifier(Socket(), "localhost", 6666).is_valid()
        assert direct_fetches == []
        assert queries == [cert.serial_number]

    @pytest.mark.parametrize("server", ["http://ocsp.test", "http://ocsp.test/"])
    def test_build_certificate_url_round_trips_request(self, server):
        # The serial is the last field of the request, so whatever the keys,
        # this one makes the base64 end in "Agh/////++++/w==", so it always
        # contains "//" and "+".
        import base64

        from cryptography.x509 import ocsp

        from redis.ocsp import OCSPVerifier

        ca, ca_key = self._make_certificate("Test CA", 1, ca=True)
        cert, _ = self._make_certificate("node", 0x7FFFFFFFFBEFBEFF, signing_key=ca_key)

        url = OCSPVerifier(None, "localhost", 6666).build_certificate_url(
            server, cert, ca
        )

        assert url.startswith("http://ocsp.test/")
        encoded = url.removeprefix("http://ocsp.test/")
        assert "/" not in encoded and "+" not in encoded
        request = unquote(encoded)
        assert "//" in request and "+" in request
        loaded = ocsp.load_der_ocsp_request(base64.b64decode(request))
        assert loaded.serial_number == cert.serial_number
