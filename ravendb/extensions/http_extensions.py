from typing import Union, Dict

import requests

from ravendb import constants


class HttpExtensions:
    @staticmethod
    def get_required_etag_header(response: requests.Response) -> str:
        if constants.Headers.ETAG in response.headers:
            headers = response.headers[constants.Headers.ETAG]
            if headers:
                return HttpExtensions.etag_header_to_change_vector(headers)

    @staticmethod
    def get_etag_header(response_or_headers: Union[requests.Response, Dict[str, str]]) -> Union[None, str]:
        headers = (
            response_or_headers.headers if isinstance(response_or_headers, requests.Response) else response_or_headers
        )
        if constants.Headers.ETAG in headers:
            headers = headers.get(constants.Headers.ETAG)
            if headers:
                return HttpExtensions.etag_header_to_change_vector(headers)
        return None

    @staticmethod
    def etag_header_to_change_vector(response_header: str) -> str:
        if not response_header:
            raise ValueError("Response didn't had an ETag header")
        if response_header.startswith('"'):
            return response_header[1:-1]
        return response_header

    @staticmethod
    def get_boolean_header(response: requests.Response, header: str) -> Union[None, bool]:
        if header in response.headers:
            first_header = response.headers.get(header)
            return bool(first_header)
        return None
