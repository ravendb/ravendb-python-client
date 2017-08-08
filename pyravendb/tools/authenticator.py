from pyravendb.custom_exceptions import exceptions
import base64
import requests
import json


class ApiKeyAuthenticator(object):
    def __init__(self):
        self._serverPublicKeys = {}

    def get_server_pk(self, url):
        with requests.session() as session:
            response = session.request("GET", url=url + "/api-key/public-key")
            if response.status_code != 200:
                raise exceptions.AuthenticationException(
                    "Bad response from server {0} when trying to get public key".format(response.status_code))

            result = response.json()
            binary = base64.standard_b64decode(result["PublicKey"])
            return binary

    def authenticate(self, url, api_key, headers):
        if not api_key:
            return None

        server_pk = self._serverPublicKeys.get(url, None)
        if server_pk is None:
            server_pk = self.get_server_pk(url)
            self._serverPublicKeys[url] = server_pk

        name, secret = api_key.split('/')

        data, sk = self.build_server_request(secret, server_pk)

        with requests.session() as session:
            response = session.request("POST", url=url + "/api-key/validate?apiKey=" + name, data=json.dumps(data),
                                       headers=headers)

            if response.status_code == 417:
                # server pk changed, need to retry, once
                server_pk = self.get_server_pk(url)
                self._serverPublicKeys[url] = server_pk
                data, sk = self.build_server_request(secret, server_pk)
                response = session.request("POST", url=url + "/api-key/validate?apiKey=" + name, data=json.dumps(data),
                                           headers=headers)

            if response.status_code != 200 and response.status_code != 403 and response.status_code != 500:
                raise exceptions.AuthenticationException("Bad response from server {0}".format(response.status_code))

        result = response.json()
        if "Error" in result:
            raise exceptions.AuthenticationException(result["Error"])

        token = base64.standard_b64decode(result["Token"])
        nonce = base64.standard_b64decode(result["Nonce"])
        token = pysodium.crypto_box_open(token, nonce, server_pk, sk)

        return token

    def build_server_request(self, secret, server_pk):
        pk, sk = pysodium.crypto_box_keypair()
        nonce = pysodium.randombytes(pysodium.crypto_box_NONCEBYTES)
        data_bytes = secret.encode('utf-8')
        data_padded = data_bytes + pysodium.randombytes((64 - (len(data_bytes) % 64)))
        encrypted_secret = pysodium.crypto_box(data_padded, nonce, server_pk, sk)
        data = {'Secret': base64.b64encode(encrypted_secret).decode('utf-8'),
                'PublicKey': base64.b64encode(pk).decode('utf-8'),
                'Nonce': base64.b64encode(nonce).decode('utf-8'),
                'ServerKey': base64.b64encode(server_pk).decode('utf-8'), }
        return data, sk
