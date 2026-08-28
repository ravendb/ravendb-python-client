"""SSO certificate editing and certificate metadata tests (RavenDB-26155)."""

import unittest

from ravendb.http.server_node import ServerNode
from ravendb.serverwide.operations.certificates import (
    CertificateMetadata,
    CertificateUsage,
    DatabaseAccess,
    EditClientCertificateOperation,
    SecurityClearance,
    SsoIdentifier,
    SsoProvider,
)


def _edit_body(parameters: EditClientCertificateOperation.Parameters) -> dict:
    operation = EditClientCertificateOperation(parameters)
    command = operation.get_command(None)
    request = command.create_request(ServerNode("http://localhost:8080", "db"))
    return request.data


class TestSsoCertificateEditing(unittest.TestCase):
    def _parameters(self, **kwargs):
        return EditClientCertificateOperation.Parameters(
            thumbprint="A" * 64,
            permissions={"db": DatabaseAccess.READ_WRITE},
            name="cert-name",
            clearance=SecurityClearance.VALID_USER,
            **kwargs,
        )

    def test_no_sso_fields_when_not_provided(self):
        body = _edit_body(self._parameters())
        self.assertNotIn("SsoServerPublicKeyPinningHashes", body)
        self.assertNotIn("AllowAnySsoServer", body)
        self.assertNotIn("SsoIdentifiers", body)

    def test_sso_fields_written_when_provided(self):
        body = _edit_body(
            self._parameters(
                sso_server_public_key_pinning_hashes=["hash1"],
                allow_any_sso_server=True,
                sso_identifiers=[
                    SsoIdentifier(
                        provider=SsoProvider.GITHUB,
                        domain="example.com",
                        identifier="user",
                    )
                ],
            )
        )
        self.assertEqual(["hash1"], body["SsoServerPublicKeyPinningHashes"])
        self.assertTrue(body["AllowAnySsoServer"])
        self.assertEqual(
            [{"Provider": "Github", "Domain": "example.com", "Identifier": "user"}],
            body["SsoIdentifiers"],
        )

    def test_empty_list_clears_and_domain_omitted_when_empty(self):
        body = _edit_body(
            self._parameters(
                sso_server_public_key_pinning_hashes=[],
                sso_identifiers=[SsoIdentifier(provider=SsoProvider.GOOGLE, identifier="user")],
            )
        )
        self.assertEqual([], body["SsoServerPublicKeyPinningHashes"])
        self.assertEqual([{"Provider": "Google", "Identifier": "user"}], body["SsoIdentifiers"])

    def test_usage_enum_values(self):
        self.assertEqual(0, CertificateUsage.RAVEN_SERVER.value)
        self.assertEqual(2, CertificateUsage.CLIENT.value)
        self.assertEqual(3, CertificateUsage.SSO_SERVER.value)
        self.assertEqual(4, CertificateUsage.SSO_CLIENT.value)
        self.assertEqual(5, CertificateUsage.WELL_KNOWN_ISSUER.value)


class TestCertificateMetadataSsoFields(unittest.TestCase):
    """GetCertificateMetadataOperation reads the SSO fields the server writes
    on CertificateMetadata (reference CertificateMetadata.ToJson)."""

    _METADATA = {
        "Name": "cert-name",
        "SecurityClearance": "ValidUser",
        "Thumbprint": "A" * 64,
        "Permissions": {"db": "ReadWrite"},
        "Disabled": False,
        "Usage": "SsoClient",
        "SsoServerPublicKeyPinningHashes": ["hash1", "hash2"],
        "AllowAnySsoServer": True,
        "SsoIdentifiers": [
            {"Provider": "Github", "Domain": "example.com", "Identifier": "user"},
            {"Provider": "Windows", "Identifier": "win-user"},
        ],
    }

    def test_from_json_binds_usage_and_sso_fields(self):
        metadata = CertificateMetadata.from_json(self._METADATA)
        self.assertEqual(CertificateUsage.SSO_CLIENT, metadata.usage)
        self.assertEqual(["hash1", "hash2"], metadata.sso_server_public_key_pinning_hashes)
        self.assertTrue(metadata.allow_any_sso_server)
        self.assertEqual(2, len(metadata.sso_identifiers))
        first = metadata.sso_identifiers[0]
        self.assertEqual(SsoProvider.GITHUB, first.provider)
        self.assertEqual("example.com", first.domain)
        self.assertEqual("user", first.identifier)
        self.assertEqual(SsoProvider.WINDOWS, metadata.sso_identifiers[1].provider)

    def test_absent_usage_and_sso_fields_are_none(self):
        metadata = CertificateMetadata.from_json(
            {
                "Name": "cert-name",
                "SecurityClearance": "ValidUser",
                "Thumbprint": "A" * 64,
            }
        )
        self.assertIsNone(metadata.usage)
        self.assertIsNone(metadata.sso_server_public_key_pinning_hashes)
        self.assertIsNone(metadata.allow_any_sso_server)
        self.assertIsNone(metadata.sso_identifiers)

    def test_sso_provider_enum_values(self):
        self.assertEqual("Github", SsoProvider.GITHUB.value)
        self.assertEqual("Google", SsoProvider.GOOGLE.value)
        self.assertEqual("Microsoft", SsoProvider.MICROSOFT.value)
        self.assertEqual("Windows", SsoProvider.WINDOWS.value)
