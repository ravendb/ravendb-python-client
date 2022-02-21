from abc import abstractmethod


class ConnectionString:
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def get_type(self):
        pass
