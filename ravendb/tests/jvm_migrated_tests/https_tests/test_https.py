import base64
import io
import tempfile
import unittest
from typing import Tuple
from zipfile import ZipFile, ZIP_DEFLATED

from ravendb.exceptions.exceptions import AuthorizationException
from ravendb.documents.store.definition import DocumentStore
from ravendb.serverwide.operations.certificates import (
    ReplaceClusterCertificateOperation,
    CreateClientCertificateOperation,
    SecurityClearance,
    DatabaseAccess,
    GetCertificatesOperation,
    GetCertificateOperation,
    CertificateRawData,
    PutClientCertificateOperation,
    DeleteCertificateOperation,
    EditClientCertificateOperation,
    GetCertificateMetadataOperation,
    GetCertificatesMetadataOperation,
)
from ravendb.tests.test_base import TestBase, User


class HttpsTest(TestBase):
    def setUp(self):
        super(HttpsTest, self).setUp()

    def test_can_connect_with_certificate(self):
        with self.secured_document_store as store:
            with store.open_session() as session:
                user = User("user1")
                session.store(user, "users/1")
                session.save_changes()

    @unittest.skip("Exception dispatcher")
    def test_can_replace_certificate(self):
        with self.secured_document_store as sec_store:
            self.assertRaisesWithMessage(
                sec_store.maintenance.server.send,
                Exception,
                "Unable to load the provided certificate",
                ReplaceClusterCertificateOperation(bytes([1, 2, 3, 4]), True),
            )

    def read_key_and_certificate(self, certificate_raw_data: CertificateRawData) -> Tuple[str, bytes]:
        cert_bytes = None
        key_string = None
        with ZipFile(io.BytesIO(certificate_raw_data.raw_data), "r", ZIP_DEFLATED) as zfile:
            file_names = zfile.namelist()
            for file_name in file_names:
                if file_name.endswith(".crt"):
                    cert_bytes = zfile.read(file_name)
                if file_name.endswith(".key"):
                    key_string = zfile.read(file_name).decode("utf-8")

            if cert_bytes is None:
                raise RuntimeError("Unable to find certificate file!")

            if key_string is None:
                raise RuntimeError("Unable to find private key file!")

        return key_string, cert_bytes

    def extract_certificate_b64(self, certificate_raw_data: CertificateRawData) -> str:
        key_string, cert_bytes = self.read_key_and_certificate(certificate_raw_data)
        return base64.b64encode(cert_bytes).decode("utf-8")

    def test_can_crud_certificates(self):
        with self.secured_document_store as store:
            cert1thumbprint = None
            cert2thumbprint = None
            try:
                # create cert1
                cert1 = store.maintenance.server.send(
                    CreateClientCertificateOperation("cert1", {}, SecurityClearance.OPERATOR)
                )

                self.assertIsNotNone(cert1)
                self.assertIsNotNone(cert1.raw_data)

                clearance = {store.database: DatabaseAccess.READ_WRITE}
                cert2 = store.maintenance.server.send(
                    CreateClientCertificateOperation("cert2", clearance, SecurityClearance.VALID_USER)
                )

                # create cert2
                self.assertIsNotNone(cert2)
                self.assertIsNotNone(cert2.raw_data)

                # list certs
                certificate_definitions = store.maintenance.server.send(GetCertificatesOperation(0, 20))
                self.assertGreaterEqual(len(certificate_definitions), 2)

                self.assertIn("cert1", list(map(lambda x: x.name, certificate_definitions)))
                self.assertIn("cert2", list(map(lambda x: x.name, certificate_definitions)))

                cert1thumbprint = list(filter(lambda x: x.name == "cert1", certificate_definitions))[0].thumbprint
                cert2thumbprint = list(filter(lambda x: x.name == "cert2", certificate_definitions))[0].thumbprint

                # delete cert1
                store.maintenance.server.send(DeleteCertificateOperation(cert1thumbprint))

                # get cert by thumbprint

                definition = store.maintenance.server.send(GetCertificateOperation(cert1thumbprint))
                self.assertIsNone(definition)

                definition2 = store.maintenance.server.send(GetCertificateOperation(cert2thumbprint))
                self.assertIsNotNone(definition2)
                self.assertEqual("cert2", definition2.name)

                # list again
                certificate_definitions = store.maintenance.server.send(GetCertificatesOperation(0, 20))
                names = list(map(lambda x: x.name, certificate_definitions))
                self.assertIn("cert2", names)
                self.assertNotIn("cert1", names)

                # extract public key from generated private key
                public_key = self.extract_certificate_b64(cert1)

                # put cert1 again, using put certificate command
                put_operation = PutClientCertificateOperation("cert3", public_key, {}, SecurityClearance.CLUSTER_ADMIN)
                store.maintenance.server.send(put_operation)
                certificate_definitions = store.maintenance.server.send(GetCertificatesOperation(0, 20))
                names = list(map(lambda x: x.name, certificate_definitions))
                self.assertIn("cert2", names)
                self.assertNotIn("cert1", names)
                self.assertIn("cert3", names)

                # and try to use edit

                parameters = EditClientCertificateOperation.Parameters(
                    cert1thumbprint, {}, "cert3-newName", SecurityClearance.VALID_USER
                )

                store.maintenance.server.send(EditClientCertificateOperation(parameters))
                certificate_definitions = store.maintenance.server.send(GetCertificatesOperation(0, 20))
                names = list(map(lambda x: x.name, certificate_definitions))
                self.assertIn("cert3-newName", names)
                self.assertNotIn("cert3", names)

                certificate_metadata = store.maintenance.server.send(GetCertificateMetadataOperation(cert1thumbprint))
                self.assertIsNotNone(certificate_metadata)
                self.assertEqual(SecurityClearance.VALID_USER, certificate_metadata.security_clearance)

                certificates_metadata = store.maintenance.server.send(
                    GetCertificatesMetadataOperation(certificate_metadata.name)
                )

                self.assertEqual(1, len(certificates_metadata))
                self.assertIsNotNone(certificates_metadata[0])
                self.assertEqual(SecurityClearance.VALID_USER, certificates_metadata[0].security_clearance)
            finally:
                # try to clean up
                if cert1thumbprint:
                    store.maintenance.server.send(DeleteCertificateOperation(cert1thumbprint))

                if cert2thumbprint:
                    store.maintenance.server.send(DeleteCertificateOperation(cert2thumbprint))

    def test_can_use_server_generated_certificate(self):
        with self.secured_document_store as store:
            certificate_raw_data = store.maintenance.server.send(
                CreateClientCertificateOperation("user-auth-test", {}, SecurityClearance.OPERATOR)
            )

            key = self.read_key_and_certificate(certificate_raw_data)

            with DocumentStore(store.urls, store.database) as store_with_out_cert:
                store_with_out_cert.trust_store_path = store.trust_store_path
                tmp = tempfile.mkstemp()
                with open(tmp[1], "wb") as file:
                    file.write(key[0].encode("utf-8"))
                    file.write(key[1])
                store_with_out_cert.certificate_pem_path = tmp[1]
                store_with_out_cert.initialize()

                with store_with_out_cert.open_session() as session:
                    user = session.load("users/1", User)

    def test_should_throw_authorization_exception_when_not_autorized(self):
        with self.secured_document_store as store:
            certificate_raw_data = store.maintenance.server.send(
                CreateClientCertificateOperation(
                    "user-auth-test", {"db1": DatabaseAccess.READ_WRITE}, SecurityClearance.VALID_USER
                )
            )

            key, cert = self.read_key_and_certificate(certificate_raw_data)

            with DocumentStore(store.urls, store.database) as store_wit_out_cert:
                store_wit_out_cert.trust_store_path = store.trust_store_path
                tmp = tempfile.mkstemp()
                with open(tmp[1], "wb") as file:
                    file.write(key.encode("utf-8"))
                    file.write(cert)
                store_wit_out_cert.certificate_pem_path = tmp[1]
                store_wit_out_cert.initialize()
                with self.assertRaises(AuthorizationException):
                    with store_wit_out_cert.open_session() as session:
                        user = session.load("users/1", User)
