"""
quiet, compatible uuencode and uudecode in pure python
"""

# imports
import binascii
import io
from typing import BinaryIO, Union

# constants
UU_CHUNK_SIZE = 45


def uuencode(
    input_buffer: Union[str, bytes, BinaryIO], name: str = "file", mode: int = 0o666
) -> bytes:
    """
    uuencode an input buffer and return the bytes.

    Args:
        input_buffer (BinaryIO): The input buffer to uuencode.
        name (str): The name to use in the uuencode header.
        mode (int): The mode to use in the uuencode header.

    Returns:
        bytes: The uuencoded bytes.
    """
    # handle input types
    if isinstance(input_buffer, str):
        input_buffer = input_buffer.encode()

    if isinstance(input_buffer, bytes):
        input_buffer = io.BytesIO(input_buffer)

    buffer = f"begin {mode:03o} {name}\n".encode()
    while True:
        chunk = input_buffer.read(45)
        if not chunk:
            break
        encoded_chunk = binascii.b2a_uu(chunk)
        buffer += encoded_chunk
    buffer += b"end\n"

    return buffer


def uudecode(input_buffer: Union[str, bytes, BinaryIO]) -> tuple[str, bytes]:
    """
    uudecode an input buffer and return the name and bytes.

    Args:
        input_buffer (Union[str, bytes, BinaryIO]): The input buffer to uudecode.
        output_buffer (Optional[BinaryIO]): The output buffer to write the decoded bytes to.

    Returns:
        tuple[str, bytes]: The name and decoded bytes.
    """
    if isinstance(input_buffer, str):
        input_buffer = input_buffer.encode()

    if isinstance(input_buffer, bytes):
        input_buffer = io.BytesIO(input_buffer)

    # read the header
    header = input_buffer.readline().decode()
    if not header.startswith("begin "):
        raise ValueError("Invalid uuencoded input")

    # parse the header but skip the mode
    _, name = header.split()[1:]

    # read the data
    data = b""
    while True:
        line = input_buffer.readline().decode()
        if line == "end\n":
            break
        try:
            data += binascii.a2b_uu(line)
        except binascii.Error as e:
            raise ValueError("Invalid uuencoded input") from e

    return name, data
