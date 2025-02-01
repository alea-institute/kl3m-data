"""
Filter out lines like this:

VerDate Mar<15>2010 16:40 Jun 14, 2010 ...
VerDate Mar<15>2010 18:36 Oct 04, 2010 ...
"""


def filter_buffer(buffer: str | bytes) -> str | bytes:
    """
    Filter and return by type.

    Args:
        buffer (str | bytes): The buffer to filter.

    Returns:
        str | bytes: The filtered buffer.
    """
    if isinstance(buffer, str):
        return "\n".join(
            line for line in buffer.split("\n") if not line.startswith("VerDate")
        )

    return b"\n".join(
        line for line in buffer.split(b"\n") if not line.startswith(b"VerDate")
    )
