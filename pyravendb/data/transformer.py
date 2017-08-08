from enum import Enum, IntEnum


class TransformerLockMode(Enum):
    unlock = "Unlock"
    locked_ignore = "LockedIgnore"
    locked_error = "LockedError"

    def __str__(self):
        return self.value


class TransformerDefinitionCompareDifferences(IntEnum):
    none = 0
    etag = 1
    transform_results = 2
    lock_mode = 4

    All = (etag | transform_results | lock_mode)


class TransformerDefinition(object):
    def __init__(self, name, transform_result, etag=0, lock_mode=TransformerLockMode.unlock):
        """
        @param name:  Transformer name.
        :type str
        @param transformer_result: Projection function.
        :type str
        @param etag: Transformer etag (internal).
        :type int
        @param lock_mode: Transformer lock
        :type TransformerLockMode
        """
        self.name = name
        self.transform_result = transform_result
        self.etag = etag
        self.lock_mode = lock_mode

    def __str__(self):
        return self.transform_result

    def to_json(self):
        return {"Name": self.name,
                "TransformResults": self.transform_result,
                "Etag": self.etag,
                "LockMode": str(self.lock_mode)}
