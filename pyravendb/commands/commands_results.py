class GetDocumentsResult:
    def __init__(
        self,
        includes: dict,
        results: list,
        counter_includes: dict,
        time_series_includes: dict,
        compare_exchange_includes: dict,
        next_page_start: int,
    ):
        self.includes = includes
        self.results = results
        self.counter_includes = counter_includes
        self.time_series_includes = time_series_includes
        self.compare_exchange_includes = compare_exchange_includes
        self.next_page_start = next_page_start
