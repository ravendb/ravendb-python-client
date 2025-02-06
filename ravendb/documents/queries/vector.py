import struct
from typing import List, Tuple


class VectorQuantizer:
    @staticmethod
    def to_int8(raw_embedding: List[float]) -> bytes:
        """
        Converts a list of floats to a packed byte array of signed 8-bit integers (int8).
        The maximum absolute value is appended as a 4-byte float at the end.

        Args:
            raw_embedding (List[float]): List of floating-point numbers to be quantized.

        Returns:
            bytes: Packed byte array containing the quantized int8 values and the max component.
        """
        if not raw_embedding:
            return b""

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

        return packed

    @staticmethod
    def to_int1(raw_embedding: List[float]) -> bytes:
        """
        Converts a list of floats to a packed byte array of binary values (int1).
        Each byte represents 8 consecutive float values, where each bit corresponds to
        whether the float is non-negative (1) or negative (0).

        Args:
            raw_embedding (List[float]): List of floating-point numbers to be quantized.

        Returns:
            bytes: Packed byte array containing the binary-packed values.
        """
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

        return bytes(bytes_list)
