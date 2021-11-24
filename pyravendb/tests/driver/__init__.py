class RavenTestDriver:
    debug = False

    def __init__(self):
        self._disposed = False

    @property
    def disposed(self):
        return self._disposed
