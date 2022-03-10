from enum import Enum


class AttachmentType(Enum):
    document = "Document"
    revision = "Revision"

    def __str__(self):
        return self.value
