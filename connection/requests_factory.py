from data.document_convention import DocumentConvention
import requests
import sys


class HttpRequestsFactory(object):
    def __init__(self, url, database, convention=None, api_key=None):
        self.url = url
        self.database = database
        self.api_key = api_key
        self.version_info = sys.version_info.major
        self.convention = convention
        if self.convention is None:
            self.convention = DocumentConvention()
        self.headers = {"Accept": "application/json", "Has-Api-key": True if self.api_key is not None else False,
                        "Raven-Client-Version": "3.0.0.0"}

    def http_request_handler(self, path, method, data=None, headers=None, admin=False):
        if admin:
            url = "{0}/admin/databases/{1}".format(self.url, path)
        else:
            url = "{0}/databases/{1}/{2}".format(self.url, self.database, path)
        if headers is not None:
            headers.update(self.headers)
        else:
            headers = self.headers
        with requests.session() as session:
            response = session.request(method, url=url, json=data, headers=headers)
            return response

    def database_open_request(self, path):
        url = "{0}/docs?{1}".format(self.url, path)
        with requests.session() as session:
            return session.request("GET", url)

    def call_hilo(self, hilo_id):
        get_url = "queries/?&id=Raven%2FHilo%2F{0}&id=Raven%2FServerPrefixForHilo".format(hilo_id)
        response = self.http_request_handler(get_url, "GET").json()
        value = response["Results"][0]
        if value is None:
            value = 0
        else:
            value = value["Max"]
        put_url = "docs/Raven%2FHilo%2F{0}".format(hilo_id)
        self.http_request_handler(put_url, "PUT", data={
            "Max": value + self.convention.max_ids_to_catch})
        return value
