from enum import Enum


class AttachmentType(Enum):
    document = 1
    revision = 2

    def __str__(self):
        return self.name
