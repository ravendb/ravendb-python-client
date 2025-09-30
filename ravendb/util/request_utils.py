import requests


class RequestUtils:

    # https://issues.hibernatingrhinos.com/issue/RDBC-940
    # If user has installed module 'zstd' or 'zstandard',
    # 'requests' module will automatically add 'zstd' to 'Accept-Encoding' header.
    # This causes exceptions. Excluding 'zstd' from the header in this workaround,
    # while we keep investigating cause of the issue.
    @staticmethod
    def remove_zstd_encoding(request: requests.PreparedRequest) -> None:
        accept_encoding = request.headers.get("Accept-Encoding")

        if "zstd" in accept_encoding:
            encodings = [
                encoding.strip() for encoding in accept_encoding.split(",") if encoding.strip().lower() != "zstd"
            ]
            new_header_value = ", ".join(encodings)
            request.headers["Accept-Encoding"] = new_header_value
