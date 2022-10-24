import base64
import socket
import ssl
from typing import Tuple, Optional

from ravendb.documents.commands.subscriptions import TcpConnectionInfo


class TcpUtils:
    @staticmethod
    def connect(
        url_string: str,
        server_certificate_base64: Optional[str] = None,
        client_certificate_pem_path: Optional[str] = None,
        certificate_private_key_password: Optional[str] = None,
    ) -> socket.socket:
        hostname, port = url_string.replace("tcp://", "").split(":")
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        is_ssl_socket = server_certificate_base64 and client_certificate_pem_path
        if server_certificate_base64 and client_certificate_pem_path:
            context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            context.load_cert_chain(client_certificate_pem_path, password=certificate_private_key_password)
            s = context.wrap_socket(s)
        s.connect((hostname, int(port)))
        if is_ssl_socket and base64.b64decode(server_certificate_base64) != s.getpeercert(True):
            raise ConnectionError("Failed to validate public server certificate.")
        return s

    @staticmethod
    def connect_with_priority(
        info: TcpConnectionInfo,
        server_cert_base64: Optional[str] = None,
        client_certificate_pem_path: Optional[str] = None,
        certificate_private_key_password: Optional[str] = None,
    ) -> Tuple[socket.socket, str]:
        if info.urls:
            for url in info.urls:
                try:
                    s = TcpUtils.connect(
                        url,
                        server_cert_base64,
                        client_certificate_pem_path,
                        certificate_private_key_password,
                    )
                    return s, url
                except Exception as e:
                    # ignored
                    pass

        s = TcpUtils.connect(
            info.url,
            server_cert_base64,
            client_certificate_pem_path,
            certificate_private_key_password,
        )

        return s, info.url
