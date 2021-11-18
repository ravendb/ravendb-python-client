class BatchCommandResult:
    def __init__(self, results, transaction_index):
        self.results: [None, list] = results
        self.transaction_index: [None, int] = transaction_index
