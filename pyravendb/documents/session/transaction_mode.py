from enum import Enum


class TransactionMode(Enum):
    SINGLE_NODE = "single_node"
    CLUSTER_WIDE = "cluster_wide"

    def __str__(self):
        return self.value
