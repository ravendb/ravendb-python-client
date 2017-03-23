from pysodium import crypto_box_keypair, crypto_box_SECRETKEYBYTES
from pysodium import crypto_box_PUBLICKEYBYTES, crypto_generichash_blake2b_BYTES_MAX


class ApiKeyAuthenticator(object):
    def authenticate(self, url, api_key):
        print("authenticator not implemented")
        return None
