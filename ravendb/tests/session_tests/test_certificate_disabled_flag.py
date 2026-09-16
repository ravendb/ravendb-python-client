"""
Unit tests for the 7.2.3 certificate `disabled` flag added to
CertificateMetadata and EditClientCertificateOperation.Parameters.
"""

import unittest

from ravendb.http.server_node import ServerNode
from ravendb.serverwide.operations.certificates import (
    CertificateDefinition,
    CertificateMetadata,
    DatabaseAccess,
    EditClientCertificateOperation,
    SecurityClearance,
)


class TestCertificateDisabledFlag(unittest.TestCase):
    def test_metadata_default_is_false(self):
        meta = CertificateMetadata()
        self.assertFalse(meta.disabled)

    def test_metadata_round_trip_through_from_json(self):
        meta = CertificateMetadata.from_json(
            {
                "Name": "n",
                "SecurityClearance": "ValidUser",
                "Disabled": True,
            }
        )
        self.assertTrue(meta.disabled)

    def test_definition_includes_disabled_in_to_json(self):
        d = CertificateDefinition()
        d.disabled = True
        self.assertTrue(d.to_json()["Disabled"])

    def test_definition_init_accepts_disabled(self):
        d = CertificateDefinition(disabled=True)
        self.assertTrue(d.disabled)

    def test_definition_round_trips_disabled(self):
        # Round-trip: deserialize a server-style payload, verify the flag survives.
        payload = {
            "Certificate": "c",
            "Password": None,
            "Name": "n",
            "SecurityClearance": "ValidUser",
            "Thumbprint": "tp",
            "NotAfter": None,
            "Permissions": {},
            "CollectionSecondaryKeys": [],
            "CollectionPrimaryKey": "",
            "PublicKeyPinningHash": None,
            "Disabled": True,
        }
        d = CertificateDefinition.from_json(payload)
        self.assertTrue(d.disabled)

    def test_definition_from_json_disabled_defaults_false(self):
        # Older servers won't emit the field at all; we should default to False.
        payload = {
            "Certificate": "c",
            "Password": None,
            "Name": "n",
            "SecurityClearance": "ValidUser",
            "Thumbprint": "tp",
            "NotAfter": None,
            "Permissions": {},
            "CollectionSecondaryKeys": [],
            "CollectionPrimaryKey": "",
            "PublicKeyPinningHash": None,
        }
        d = CertificateDefinition.from_json(payload)
        self.assertFalse(d.disabled)

    def test_edit_operation_parameters_carries_disabled(self):
        params = EditClientCertificateOperation.Parameters(
            thumbprint="abc",
            permissions={"db1": DatabaseAccess.READ},
            name="my-cert",
            clearance=SecurityClearance.VALID_USER,
            disabled=True,
        )
        op = EditClientCertificateOperation(params)
        request = op.get_command(None).create_request(ServerNode("http://localhost:8080", "db"))

        # The disabled flag flows through the command into the request body.
        self.assertTrue(request.data["Disabled"])


if __name__ == "__main__":
    unittest.main()
