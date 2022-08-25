class OperationCancelledException(RuntimeError):
    def __init__(self, *args):
        super(OperationCancelledException, self).__init__(*args)
