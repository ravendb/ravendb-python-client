import uuid


class RaftIdGenerator:
    @staticmethod
    def new_id() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def dont_care_id() -> str:
        return ""
