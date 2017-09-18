from enum import Enum


class CertificateDefinition:
    def __init__(self, certificate, server_admin=False, thumbprint=None, permissions=None):
        """
        @param certificate: X509 byte array object encoded to base64
        :type str
        @param server_admin:  A part of the permissions if True get all the permissions (admin use only)
        :type bool
        @param thumbprint: The thump print of the certificate file
        :type str
        @param permissions: A dict of database_access
        :type dict[str:DatabaseAccess]
        """
        self.certificate = certificate
        self.server_admin = server_admin
        self.thumbprint = thumbprint
        self.permissions = {} if permissions is None else permissions

    def to_json(self):
        for key, value in self.permissions.items():
            self.permissions[key] = str(value)
        return {"Certificate": self.certificate, "Thumbprint": self.thumbprint, "Permissions": self.permissions}


class DatabaseAccess(Enum):
    read_write = "ReadWrite"
    admin = "Admin"

    def __str__(self):
        return self.value


class SecurityClearance(Enum):
    unauthenticated_clients = "UnauthenticatedClients"
    cluster_admin = "ClusterAdmin"
    operator = "Operator"
    valid_user = "ValidUser"

    def __str__(self):
        return self.value
