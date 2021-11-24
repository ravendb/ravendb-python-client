from typing import Union


class GetDocumentResult:
    def __init__(self):
        self.includes: Union[None, dict] = None
        self.results: Union[None, list] = None
        self.counter_includes: Union[None, dict] = None
        self.time_series_includes: Union[None, dict] = None
        self.compare_exchange_value_includes: Union[None, dict] = None
        self.next_page_start: Union[None, int] = None
