from enum import Enum


class VectorEmbeddingType(Enum):
    SINGLE = "Single"  # float
    INT8 = "Int8"  # quantized int
    BINARY = "Binary"  # 1/0 quantized int
    TEXT = "Text"  # str
