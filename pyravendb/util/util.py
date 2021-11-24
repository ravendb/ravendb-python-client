import uuid


class RaftIdGenerator:
    def __init__(self):
        pass

    @staticmethod
    def new_id() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def dont_care_id() -> str:
        return ""
