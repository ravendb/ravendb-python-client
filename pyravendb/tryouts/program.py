from pyravendb.store.document_store import DocumentStore
from pyravendb.raven_operations.timeseries_operations import \
    GetTimeSeriesOperation, TimeSeriesRange, TimeSeriesBatchOperation, TimeSeriesOperation
from pyravendb.raven_operations.counters_operations import *
from pyravendb.raven_operations.maintenance_operations import PutIndexesOperation, IndexDefinition, \
    PutConnectionStringOperation, ConnectionString, UpdateExternalReplicationOperation, ExternalReplication, \
    PutPullReplicationAsHubOperation, PullReplicationDefinition, UpdatePullReplicationAsSinkOperation, \
    PullReplicationAsSink
from datetime import datetime, timedelta, timezone
from time import sleep
import OpenSSL

class User:
    def __init__(self, name, address):
        self.Id = None
        self.name = name
        self.address = address


class Address:
    def __init__(self, street):
        self.street = street


def get_user(key, value):
    if key == "address":
        return Address(**value)
    return value

def pkcs12_to_pem(pkcs12_data, password):
    if isinstance(password, str):
        password_bytes = password.encode('utf8')
    else:
        password_bytes = password
    p12 = OpenSSL.crypto.load_pkcs12(pkcs12_data, password_bytes)
    p12_cert = p12.get_certificate()
    p12_key = p12.get_privatekey()
    pem_cert = OpenSSL.crypto.dump_certificate(OpenSSL.crypto.FILETYPE_PEM, p12_cert)
    pem_key = OpenSSL.crypto.dump_privatekey(OpenSSL.crypto.FILETYPE_PEM, p12_key)
    pem = pem_cert + pem_key
    return pem

if __name__ == "__main__":
    path = "C:\\Users\\ayende\\Downloads\\free.roll.client.certificate (1)\\PEM\\"
    pfx = path + "..\\free.roll.client.certificate.with.password.pfx"
    with open(pfx, 'rb') as pkcs12_file:
        pkcs12_data = pkcs12_file.read()
        #c = pkcs12_to_pem(pkcs12_data, "E9302BBF95723A2EC0E44FEF48DD22")

    with DocumentStore(urls=["https://a.free.roll.ravendb.cloud"], database="demo",
                       certificate={"pfx": pkcs12_data, "password": "E9302BBF95723A2EC0E44FEF48DD22"}) as store:
        store.initialize()

        with store.open_session() as s:
            s.store(User("Oren", "Binyamina"))
            s.save_changes()
