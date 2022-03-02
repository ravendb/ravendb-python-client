class User:
    def __init__(
        self,
        Id: str = None,
        name: str = None,
        last_name: str = None,
        address_id: str = None,
        count: int = None,
        age: int = None,
    ):
        self.Id = Id
        self.name = name
        self.last_name = last_name
        self.address_id = address_id
        self.count = count
        self.age = age
