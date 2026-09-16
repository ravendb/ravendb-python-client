"""
Tests for the certificate SSO fields added in 7.2.5: the metadata a certificate now
carries, and the opt-in write semantics of EditClientCertificateOperation.
"""

import unittest

from ravendb.documents.conventions import DocumentConventions
from ravendb.http.server_node import ServerNode
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


class TestSsoIdentifier(unittest.TestCase):
    def test_identifier_round_trips(self):
        payload = {"Provider": "Google", "Domain": "ravendb.net", "Identifier": "gracjan@ravendb.net"}
        identifier = SsoIdentifier.from_json(payload)

        self.assertEqual(SsoProvider.GOOGLE, identifier.provider)
        self.assertEqual("ravendb.net", identifier.domain)
        self.assertEqual(payload, identifier.to_json())

    def test_a_domainless_identifier_is_allowed(self):
        identifier = SsoIdentifier.from_json({"Provider": "Github", "Identifier": "gracjan"})

        self.assertEqual(SsoProvider.GITHUB, identifier.provider)
        self.assertIsNone(identifier.domain)

    def test_every_provider_the_server_knows_is_mapped(self):
        for name in ("Github", "Google", "Microsoft", "Windows"):
            self.assertEqual(name, SsoProvider(name).value)


class TestCertificateSsoMetadata(unittest.TestCase):
    PAYLOAD = {
        "Name": "sso-user",
        "SecurityClearance": "ValidUser",
        "Thumbprint": "TP",
        "Permissions": {},
        "Disabled": False,
        "Usage": "SsoClient",
        "AllowAnySsoServer": True,
        "SsoServerPublicKeyPinningHashes": ["hash-a", "hash-b"],
        "SsoIdentifiers": [{"Provider": "Microsoft", "Identifier": "gracjan@ravendb.net", "Domain": "ravendb.net"}],
    }

    def test_metadata_parses_the_sso_fields(self):
        metadata = CertificateMetadata.from_json(self.PAYLOAD)

        self.assertEqual(CertificateUsage.SSO_CLIENT, metadata.usage)
        self.assertTrue(metadata.allow_any_sso_server)
        self.assertEqual(["hash-a", "hash-b"], metadata.sso_server_public_key_pinning_hashes)
        self.assertEqual(SsoProvider.MICROSOFT, metadata.sso_identifiers[0].provider)

    def test_a_certificate_from_an_older_server_gets_the_neutral_defaults(self):
        metadata = CertificateMetadata.from_json({"Name": "n", "SecurityClearance": "Operator", "Permissions": {}})

        self.assertIsNone(metadata.usage)
        self.assertFalse(metadata.allow_any_sso_server)
        self.assertEqual([], metadata.sso_server_public_key_pinning_hashes)
        self.assertEqual([], metadata.sso_identifiers)

    def test_definition_parses_and_writes_the_sso_fields(self):
        definition = CertificateDefinition.from_json(
            {
                **self.PAYLOAD,
                "Certificate": "cert",
                "NotAfter": None,
                "CollectionSecondaryKeys": [],
                "CollectionPrimaryKey": "",
                "PublicKeyPinningHash": "h",
            }
        )
        serialized = definition.to_json()

        self.assertEqual("SsoClient", serialized["Usage"])
        self.assertTrue(serialized["AllowAnySsoServer"])
        self.assertEqual(["hash-a", "hash-b"], serialized["SsoServerPublicKeyPinningHashes"])
        self.assertEqual("gracjan@ravendb.net", serialized["SsoIdentifiers"][0]["Identifier"])

    def test_a_definition_with_no_usage_writes_none(self):
        self.assertIsNone(CertificateDefinition().to_json()["Usage"])

    def test_every_usage_the_server_knows_is_mapped(self):
        for name in (
            "RavenServer",
            "RavenServerForCommunication",
            "Client",
            "SsoServer",
            "SsoClient",
            "WellKnownIssuer",
        ):
            self.assertEqual(name, CertificateUsage(name).value)


class TestEditClientCertificateSsoPayload(unittest.TestCase):
    NODE = ServerNode("http://localhost:8080", "db")

    def _payload(self, **kwargs):
        parameters = EditClientCertificateOperation.Parameters(
            thumbprint="TP",
            permissions={"orders": DatabaseAccess.READ_WRITE},
            name="sso-user",
            clearance=SecurityClearance.VALID_USER,
            **kwargs,
        )
        request = EditClientCertificateOperation(parameters).get_command(None).create_request(self.NODE)
        return request.data

    def test_the_base_payload_carries_only_what_the_server_edits(self):
        payload = self._payload()

        self.assertEqual({"Thumbprint", "Name", "SecurityClearance", "Disabled", "Permissions"}, set(payload))
        self.assertEqual("TP", payload["Thumbprint"])
        self.assertFalse(payload["Disabled"])

    def test_permissions_and_clearance_serialize_by_their_server_side_names(self):
        payload = self._payload()
        encoded = DocumentConventions.json_default(payload["Permissions"]["orders"])

        self.assertEqual("ReadWrite", encoded)
        self.assertEqual("ValidUser", DocumentConventions.json_default(payload["SecurityClearance"]))

    def test_a_plain_edit_leaves_the_stored_sso_configuration_alone(self):
        # Omitting the SSO fields is how a regular edit avoids clobbering an SSO user.
        payload = self._payload(disabled=True)

        self.assertNotIn("SsoServerPublicKeyPinningHashes", payload)
        self.assertNotIn("AllowAnySsoServer", payload)
        self.assertNotIn("SsoIdentifiers", payload)
        self.assertTrue(payload["Disabled"])

    def test_an_empty_list_is_sent_so_it_can_clear_the_stored_value(self):
        payload = self._payload(sso_server_public_key_pinning_hashes=[], sso_identifiers=[])

        self.assertEqual([], payload["SsoServerPublicKeyPinningHashes"])
        self.assertEqual([], payload["SsoIdentifiers"])

    def test_setting_the_sso_fields_writes_them_all(self):
        payload = self._payload(
            sso_server_public_key_pinning_hashes=["hash-a"],
            allow_any_sso_server=True,
            sso_identifiers=[SsoIdentifier(SsoProvider.GITHUB, "gracjan")],
        )

        self.assertEqual(["hash-a"], payload["SsoServerPublicKeyPinningHashes"])
        self.assertTrue(payload["AllowAnySsoServer"])
        self.assertEqual({"Provider": "Github", "Domain": None, "Identifier": "gracjan"}, payload["SsoIdentifiers"][0])

    def test_allow_any_sso_server_can_be_turned_off_explicitly(self):
        payload = self._payload(allow_any_sso_server=False)

        self.assertIn("AllowAnySsoServer", payload)
        self.assertFalse(payload["AllowAnySsoServer"])

    def test_the_operation_still_validates_its_required_arguments(self):
        with self.assertRaises(ValueError):
            EditClientCertificateOperation(None)
        with self.assertRaises(ValueError):
            EditClientCertificateOperation(
                EditClientCertificateOperation.Parameters("TP", {}, None, SecurityClearance.VALID_USER)
            )
        with self.assertRaises(ValueError):
            EditClientCertificateOperation(
                EditClientCertificateOperation.Parameters(None, {}, "n", SecurityClearance.VALID_USER)
            )
        with self.assertRaises(ValueError):
            EditClientCertificateOperation(
                EditClientCertificateOperation.Parameters("TP", None, "n", SecurityClearance.VALID_USER)
            )
