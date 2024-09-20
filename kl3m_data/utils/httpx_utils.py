"""
Utilities for working with httpx
"""

# imports
from typing import Any, Optional

# packages
import httpx

# project
from kl3m_data.config import CONFIG


def get_httpx_limits(
    keepalive_connections: Optional[int] = None,
    connections: Optional[int] = None,
) -> httpx.Limits:
    """
    Get httpx.Limits object with default values.

    Returns:
        httpx.Limits: An httpx.Limits object.
    """
    return httpx.Limits(
        max_keepalive_connections=keepalive_connections
        or CONFIG.default_httpx_limit_keepalive,
        max_connections=connections or CONFIG.default_httpx_limit_connections,
    )


def get_httpx_timeout(
    network_timeout: Optional[int] = None,
    connect_timeout: Optional[int] = None,
    read_timeout: Optional[int] = None,
    write_timeout: Optional[int] = None,
) -> httpx.Timeout:
    """
    Get httpx.Timeout object with default values.

    Returns:
        httpx.Timeout: An httpx.Timeout object.
    """
    return httpx.Timeout(
        network_timeout or CONFIG.default_httpx_network_timeout,
        connect=connect_timeout or CONFIG.default_httpx_connect_timeout,
        read=read_timeout or CONFIG.default_httpx_read_timeout,
        write=write_timeout or CONFIG.default_httpx_write_timeout,
    )


def get_default_headers(
    user_agent: Optional[str] = None,
) -> dict[str, Any]:
    """
    Get default headers for httpx requests.

    Args:
        user_agent (str): User-Agent string

    Returns:
        dict: A dictionary of headers
    """
    return {
        "User-Agent": user_agent or CONFIG.user_agent,
    }
