from __future__ import annotations

from typing import Optional


class ClientChangeVectorUtils:
    SEPARATOR = "|"

    @staticmethod
    def get_etag_by_id(change_vector: Optional[str], id: str) -> int:
        """Returns the etag for the given cluster-transaction id in a change-vector
        part, or 0 when the id is not present (C# GetEtagById)."""
        if change_vector is None:
            return 0

        if id is None:
            raise ValueError("id cannot be None")

        if ClientChangeVectorUtils.SEPARATOR in change_vector:
            raise ValueError(
                f"Change vector contains '{ClientChangeVectorUtils.SEPARATOR}', "
                "which is not supported for this operation."
            )

        index = change_vector.find("-" + id)
        if index == -1:
            return 0

        end = index - 1
        start = change_vector.rfind(":", 0, end + 1) + 1

        return int(change_vector[start : end + 1])
