"""
Tika utils
"""

# packages
import httpx

# project
from kl3m_data.logger import LOGGER


DEFAULT_TIKA_TIMEOUT = 300


def get_recursive_meta_html(
    tika_url: str,
    buffer: bytes,
) -> list[dict]:
    """
    Get the metadata from the Tika server.
    """
    # init the metadata
    metadata = {}

    # init the tika client
    client = httpx.Client()

    # get the metadata
    try:
        request_url = f"{tika_url.rstrip('/')}/rmeta/html"
        response = client.put(
            request_url,
            headers={
                "Accept": "application/json",
                "X-Tika-Timeout-Millis": str(DEFAULT_TIKA_TIMEOUT * 1000),
            },
            content=buffer,
            timeout=DEFAULT_TIKA_TIMEOUT,
        )
        metadata = response.json()
    except Exception as e:
        LOGGER.error("Error getting metadata: %s", e)
    finally:
        client.close()

    return metadata


def get_html_contents(
    tika_url: str,
    buffer: bytes,
) -> list[str]:
    """
    Get the contents from the Tika server.
    """
    return [
        doc.get("X-TIKA:content")
        for doc in get_recursive_meta_html(tika_url, buffer)
        if doc.get("X-TIKA:content", None) is not None
        and len(doc.get("X-TIKA:content")) > 0
    ]
