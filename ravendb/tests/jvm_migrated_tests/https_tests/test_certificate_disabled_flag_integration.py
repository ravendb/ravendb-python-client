"""
Integration tests against a live RavenDB 7.2.x HTTPS server for the
certificate `disabled` flag added in 7.2.3.

Verifies that the flag round-trips through:
  * EditClientCertificateOperation outbound (disabled in `Parameters`)
  * GetCertificatesOperation / GetCertificateOperation inbound (Disabled in
    JSON → CertificateDefinition.disabled)
  * GetCertificateMetadataOperation inbound (Disabled in JSON →
    CertificateMetadata.disabled)
"""

import unittest

from ravendb.serverwide.operations.certificates import (
    CreateClientCertificateOperation,
    DatabaseAccess,
    EditClientCertificateOperation,
    GetCertificateMetadataOperation,
    GetCertificateOperation,
    GetCertificatesOperation,
    SecurityClearance,
)
from ravendb.tests.test_base import TestBase


class TestCertificateDisabledFlagIntegration(TestBase):
    def test_edit_can_disable_certificate(self):
        with self.secured_document_store as store:
            # Create a fresh client certificate to disable.
            create_op = CreateClientCertificateOperation(
                "test-disable-cert",
                {"test_db": DatabaseAccess.READ_WRITE},
                SecurityClearance.VALID_USER,
            )
            store.maintenance.server.send(create_op)

            # Find its thumbprint from the listing.
            all_certs = store.maintenance.server.send(GetCertificatesOperation(0, 200))
            mine = next(c for c in all_certs if c.name == "test-disable-cert")
            self.assertFalse(mine.disabled)  # baseline

            # Disable via EditClientCertificateOperation.
            store.maintenance.server.send(
                EditClientCertificateOperation(
                    EditClientCertificateOperation.Parameters(
                        thumbprint=mine.thumbprint,
                        permissions={"test_db": DatabaseAccess.READ_WRITE},
                        name="test-disable-cert",
                        clearance=SecurityClearance.VALID_USER,
                        disabled=True,
                    )
                )
            )

            # Read back via GetCertificateOperation; Disabled must round-trip.
            single = store.maintenance.server.send(GetCertificateOperation(mine.thumbprint))
            self.assertIsNotNone(single)
            self.assertTrue(single.disabled)

            # Read back via GetCertificatesOperation listing too.
            all_certs = store.maintenance.server.send(GetCertificatesOperation(0, 200))
            mine_after = next(c for c in all_certs if c.thumbprint == mine.thumbprint)
            self.assertTrue(mine_after.disabled)

            # Read back via GetCertificateMetadataOperation (uses
            # CertificateMetadata.from_json, the other deserialization path).
            metadata = store.maintenance.server.send(GetCertificateMetadataOperation(mine.thumbprint))
            self.assertIsNotNone(metadata)
            self.assertTrue(metadata.disabled)

    def test_edit_can_re_enable_certificate(self):
        with self.secured_document_store as store:
            create_op = CreateClientCertificateOperation(
                "test-reenable-cert",
                {"test_db": DatabaseAccess.READ},
                SecurityClearance.VALID_USER,
            )
            store.maintenance.server.send(create_op)

            certs = store.maintenance.server.send(GetCertificatesOperation(0, 200))
            mine = next(c for c in certs if c.name == "test-reenable-cert")

            # Disable, then re-enable.
            for desired_state in (True, False):
                store.maintenance.server.send(
                    EditClientCertificateOperation(
                        EditClientCertificateOperation.Parameters(
                            thumbprint=mine.thumbprint,
                            permissions={"test_db": DatabaseAccess.READ},
                            name="test-reenable-cert",
                            clearance=SecurityClearance.VALID_USER,
                            disabled=desired_state,
                        )
                    )
                )
                round_tripped = store.maintenance.server.send(GetCertificateOperation(mine.thumbprint))
                self.assertEqual(desired_state, round_tripped.disabled)


if __name__ == "__main__":
    unittest.main()
