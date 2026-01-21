from __future__ import annotations
import struct
from typing import List


class VectorQuantizer:
    @staticmethod
    def to_int8(raw_embedding: List[float]) -> List[int]:
        """
        Converts a list of floats to a list of signed 8-bit integers (int8).
        The returned list contains the quantized values followed by the
        max_component float encoded as 4 signed bytes.

        Args:
            raw_embedding (List[float]): List of floating-point numbers to be quantized.

        Returns:
            List[int]: List of signed integers (-128 to 127) representing the quantized vector.
                      Includes both the quantized values and the 4 bytes of the max_component float.
        """
        if not raw_embedding:
            return []

        # Find the maximum absolute value in the input array
        max_component: float = max(abs(x) for x in raw_embedding)

        # If all elements are zero, set quantized to all zeros
        if max_component == 0:
            quantized: List[int] = [0] * len(raw_embedding)
        else:
            # Scale all elements to the range [-127, 127]
            scale_factor: float = 127.0 / max_component
            quantized: List[int] = [int(x * scale_factor) for x in raw_embedding]

        # Pack the quantized values into signed bytes (int8)
        packed: bytes = struct.pack("b" * len(quantized), *quantized)

        # Append the max_component as a little-endian float
        packed += struct.pack("<f", max_component)

        # Convert to list of signed integers
        return VectorQuantizer._bytes_to_int8_list(packed)

    @staticmethod
    def to_int1(raw_embedding: List[float]) -> List[int]:
        """
        Converts a list of floats to a list of binary values (0 or 1).
        Each value in the input is converted to 1 if non-negative, 0 if negative.

        Args:
            raw_embedding (List[float]): List of floating-point numbers to be quantized.

        Returns:
            List[int]: List of 0s and 1s representing the binary-quantized vector.
        """
        if not raw_embedding:
            return []

        # Calculate the number of bytes needed to store the binary-packed values
        output_length: int = (len(raw_embedding) + 7) // 8

        # Initialize a bytearray to store the packed bits
        bytes_list: bytearray = bytearray(output_length)

        # Iterate over each float value and pack it into the appropriate bit
        for i, val in enumerate(raw_embedding):
            if val >= 0:
                byte_index: int = i // 8  # Determine which byte to modify
                bit_pos: int = 7 - (i % 8)  # Determine the bit position within the byte
                bytes_list[byte_index] |= 1 << bit_pos  # Set the bit to 1 if the value is non-negative

        # Convert to list of 0s and 1s
        return VectorQuantizer._bytes_to_int1_list(bytes(bytes_list), len(raw_embedding))

    @staticmethod
    def _bytes_to_int8_list(packed_bytes: bytes) -> List[int]:
        """
        Protected method to convert packed bytes to a list of signed int8 values.
        Includes all bytes (quantized values + max_component float bytes).

        Args:
            packed_bytes (bytes): Packed byte array from to_int8().

        Returns:
            List[int]: List of signed integers (-128 to 127).
        """
        if not packed_bytes:
            return []

        # Unpack ALL bytes as signed int8 (including the 4-byte max_component)
        return list(struct.unpack("b" * len(packed_bytes), packed_bytes))

    @staticmethod
    def _bytes_to_int1_list(packed_bytes: bytes, original_length: int) -> List[int]:
        """
        Protected method to convert packed binary bytes to a list of 0s and 1s.

        Args:
            packed_bytes (bytes): Packed byte array from to_int1().
            original_length (int): Original number of float values (to trim padding).

        Returns:
            List[int]: List of 0s and 1s.
        """
        result = []
        for byte_val in packed_bytes:
            for bit_pos in range(7, -1, -1):
                result.append(1 if (byte_val & (1 << bit_pos)) else 0)

        # Trim to original length (remove padding bits)
        return result[:original_length]
