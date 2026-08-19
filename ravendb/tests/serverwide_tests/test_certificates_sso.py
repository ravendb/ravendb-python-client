"""Tests for the SSO certificate surface: CertificateMetadata
SSO fields, the CertificateUsage/SsoProvider enums, SsoIdentifier, and the
edit-operation body rules.
"""

import unittest

from ravendb.http.server_node import ServerNode
from ravendb.http.topology import RaftCommand
from ravendb.serverwide.operations.certificates import (
    CertificateDefinition,
    CertificateMetadata,
    CertificateUsage,
    DatabaseAccess,
    EditClientCertificateOperation,
    SecurityClearance,
    SsoIdentifier,
    SsoProvider,
)


class TestSsoEnums(unittest.TestCase):
    def test_certificate_usage_values_are_csharp_names(self):
        self.assertEqual(
            ["RavenServer", "RavenServerForCommunication", "Client", "SsoServer", "SsoClient", "WellKnownIssuer"],
            [m.value for m in CertificateUsage],
        )

    def test_sso_provider_values_are_csharp_names(self):
        self.assertEqual(["Github", "Google", "Microsoft", "Windows"], [m.value for m in SsoProvider])

    def test_sso_identifier_round_trip(self):
        sso = SsoIdentifier(provider=SsoProvider.GITHUB, domain="github.com", identifier="alice")
        self.assertEqual({"Provider": "Github", "Domain": "github.com", "Identifier": "alice"}, sso.to_json())
        back = SsoIdentifier.from_json({"Provider": "Google", "Domain": None, "Identifier": "bob"})
        self.assertEqual(SsoProvider.GOOGLE, back.provider)
        self.assertIsNone(back.domain)
        self.assertEqual("bob", back.identifier)


class TestCertificateMetadataSsoFields(unittest.TestCase):
    def test_to_json_writes_sso_keys(self):
        definition = CertificateDefinition(
            name="n",
            usage=CertificateUsage.SSO_CLIENT,
            sso_server_public_key_pinning_hashes=["hash1"],
            allow_any_sso_server=True,
            sso_identifiers=[SsoIdentifier(provider=SsoProvider.WINDOWS, domain="corp", identifier="win\\u")],
        )
        result = definition.to_json()
        self.assertEqual("SsoClient", result["Usage"])
        self.assertEqual(["hash1"], result["SsoServerPublicKeyPinningHashes"])
        self.assertIs(True, result["AllowAnySsoServer"])
        self.assertEqual([{"Provider": "Windows", "Domain": "corp", "Identifier": "win\\u"}], result["SsoIdentifiers"])

    def test_to_json_usage_null_and_defaults(self):
        definition = CertificateDefinition(name="n")
        result = definition.to_json()
        self.assertIsNone(result["Usage"])
        self.assertEqual([], result["SsoServerPublicKeyPinningHashes"])
        self.assertIs(False, result["AllowAnySsoServer"])
        self.assertEqual([], result["SsoIdentifiers"])

    def test_from_json_parses_sso_fields(self):
        metadata = CertificateMetadata.from_json(
            {
                "Name": "n",
                "SecurityClearance": "ClusterAdmin",
                "Usage": "SsoServer",
                "SsoServerPublicKeyPinningHashes": ["h"],
                "AllowAnySsoServer": True,
                "SsoIdentifiers": [{"Provider": "Github", "Domain": None, "Identifier": "id"}],
            }
        )
        self.assertEqual(CertificateUsage.SSO_SERVER, metadata.usage)
        self.assertEqual(["h"], metadata.sso_server_public_key_pinning_hashes)
        self.assertIs(True, metadata.allow_any_sso_server)
        self.assertEqual(1, len(metadata.sso_identifiers))
        self.assertEqual(SsoProvider.GITHUB, metadata.sso_identifiers[0].provider)

    def test_from_json_missing_sso_keys_use_defaults(self):
        metadata = CertificateMetadata.from_json({"Name": "n", "SecurityClearance": "ClusterAdmin"})
        self.assertIsNone(metadata.usage)
        self.assertEqual([], metadata.sso_server_public_key_pinning_hashes)
        self.assertIs(False, metadata.allow_any_sso_server)
        self.assertEqual([], metadata.sso_identifiers)

    def test_definition_from_json_parses_sso_fields(self):
        definition = CertificateDefinition.from_json(
            {
                "Certificate": None,
                "Name": "n",
                "SecurityClearance": "ClusterAdmin",
                "Thumbprint": "tp",
                "NotAfter": None,
                "Permissions": {"db": "ReadWrite"},
                "CollectionSecondaryKeys": [],
                "CollectionPrimaryKey": "",
                "PublicKeyPinningHash": None,
                "Usage": "Client",
                "SsoServerPublicKeyPinningHashes": ["h"],
                "AllowAnySsoServer": False,
                "SsoIdentifiers": [],
            }
        )
        self.assertEqual(CertificateUsage.CLIENT, definition.usage)
        self.assertEqual(["h"], definition.sso_server_public_key_pinning_hashes)


class TestEditClientCertificateOperationSso(unittest.TestCase):
    def setUp(self):
        self.node = ServerNode("http://localhost:8080", "db1")

    def _body(self, **parameters_kwargs):
        parameters = EditClientCertificateOperation.Parameters(
            thumbprint="tp",
            permissions={"db1": DatabaseAccess.ADMIN},
            name="n",
            clearance=SecurityClearance.CLUSTER_ADMIN,
            **parameters_kwargs,
        )
        operation = EditClientCertificateOperation(parameters)
        command = operation.get_command(None)
        request = command.create_request(self.node)
        return request, command

    def test_base_keys_always_present(self):
        request, command = self._body()
        self.assertEqual("POST", request.method)
        self.assertEqual("http://localhost:8080/admin/certificates/edit", request.url)
        body = request.data
        self.assertEqual("tp", body["Thumbprint"])
        self.assertEqual("n", body["Name"])
        self.assertEqual("ClusterAdmin", body["SecurityClearance"])
        self.assertIs(False, body["Disabled"])
        self.assertEqual({"db1": "Admin"}, body["Permissions"])

    def test_sso_keys_omitted_when_none(self):
        request, _ = self._body()
        body = request.data
        self.assertNotIn("SsoServerPublicKeyPinningHashes", body)
        self.assertNotIn("AllowAnySsoServer", body)
        self.assertNotIn("SsoIdentifiers", body)

    def test_sso_keys_written_when_provided(self):
        request, _ = self._body(
            sso_server_public_key_pinning_hashes=["h1", "h2"],
            allow_any_sso_server=True,
            sso_identifiers=[SsoIdentifier(provider=SsoProvider.GOOGLE, domain="g.com", identifier="id")],
        )
        body = request.data
        self.assertEqual(["h1", "h2"], body["SsoServerPublicKeyPinningHashes"])
        self.assertIs(True, body["AllowAnySsoServer"])
        self.assertEqual([{"Provider": "Google", "Identifier": "id", "Domain": "g.com"}], body["SsoIdentifiers"])

    def test_empty_list_sent_to_clear(self):
        request, _ = self._body(sso_server_public_key_pinning_hashes=[])
        self.assertEqual([], request.data["SsoServerPublicKeyPinningHashes"])

    def test_domain_omitted_when_empty_in_edit_body(self):
        request, _ = self._body(
            sso_identifiers=[SsoIdentifier(provider=SsoProvider.WINDOWS, domain="", identifier="id")]
        )
        self.assertEqual([{"Provider": "Windows", "Identifier": "id"}], request.data["SsoIdentifiers"])

    def test_body_not_built_from_certificate_definition_to_json(self):
        # The edit body must never carry the unconditional SSO keys that
        # CertificateDefinition.to_json writes for its own serialization.
        request, _ = self._body()
        body = request.data
        for key in ("Usage", "SsoServerPublicKeyPinningHashes", "AllowAnySsoServer", "SsoIdentifiers"):
            self.assertNotIn(key, body)

    def test_command_is_raft_command(self):
        request, command = self._body()
        self.assertIsInstance(command, RaftCommand)
        self.assertTrue(command.get_raft_unique_request_id())

    def test_constructor_validation(self):
        with self.assertRaises(ValueError):
            EditClientCertificateOperation(None)


if __name__ == "__main__":
    unittest.main()
