"""
Base source to standardize retrieval/update interface for all resources.
"""

# imports
import abc
import datetime
import json
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Optional, Generator

# packages
import httpx
import lxml.etree
import lxml.html
from playwright.sync_api import sync_playwright


# project
from kl3m_data.config import CONFIG
from kl3m_data.logger import LOGGER
from kl3m_data.utils.httpx_utils import (
    get_default_headers,
    get_httpx_limits,
    get_httpx_timeout,
)
from kl3m_data.utils.s3_utils import get_s3_client, check_prefix_exists


class SourceDownloadStatus(Enum):
    """
    Source download status to track whether the resource was:
    - already existed
    - downloaded successfully
    - partially downloaded
    - failed to download
    - not found
    """

    EXISTED = auto()
    SUCCESS = auto()
    PARTIAL = auto()
    FAILURE = auto()
    NOT_FOUND = auto()


# dataclass for SourceProgressStatus
@dataclass
class SourceProgressStatus:
    """
    Source progress status
    """

    # current description
    description: str
    # log item/message to display once
    message: Optional[str] = None
    # total number of items if known
    total: Optional[int] = None
    # current item number
    current: int = 0
    # number of successes
    success: int = 0
    # number of failures
    failure: int = 0
    # whether any errors occurred
    status: bool = True
    # whether the download is done
    done: bool = False
    # extra data to include/show
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class SourceMetadata:
    """
    Source metadata
    """

    dataset_id: str
    dataset_home: str
    dataset_description: str
    dataset_license: str


class BaseSource(abc.ABC):
    """
    Base source to standardize retrieval/update interface for all resources.
    """

    metadata: SourceMetadata

    # init method with a default httpx Client and AsyncClient configured using KL3MDataConfig
    def __init__(self, metadata: SourceMetadata):
        """
        Initialize the source.

        Args:
            metadata (SourceMetadata): Metadata for the source
        """
        # set metadata
        self.metadata = metadata

        # initialize the client and async_client with default values
        self.client: httpx.Client = self._init_httpx_client()
        self.async_client: httpx.AsyncClient = self._init_httpx_async_client()
        self.s3_client = get_s3_client()

        # rate limit if needed
        self.rate_limit_limit: Optional[int] = None  # x-ratelimit-limit
        self.rate_limit_remaining: Optional[int] = None  # x-ratelimit-remaining
        self.prior_rate_limit_remaining: Optional[int] = None  # prior value of above

        # log it
        LOGGER.info("Initialized source %s", self.metadata.dataset_id)

    def _init_httpx_client(self) -> httpx.Client:
        """
        Initialize the httpx Client with default values.

        Returns:
            httpx.Client: An httpx Client object.
        """
        return httpx.Client(
            http1=True,
            http2=True,
            verify=False,
            follow_redirects=True,
            limits=get_httpx_limits(),
            timeout=get_httpx_timeout(),
            headers=get_default_headers(),
        )

    def _init_httpx_async_client(self) -> httpx.AsyncClient:
        """
        Initialize the httpx AsyncClient with default values.

        Returns:
            httpx.AsyncClient: An httpx AsyncClient object.
        """
        return httpx.AsyncClient(
            http1=True,
            http2=True,
            verify=False,
            follow_redirects=True,
            limits=get_httpx_limits(),
            timeout=get_httpx_timeout(),
            headers=get_default_headers(),
        )

    def close(self):
        """
        Close the httpx clients.
        """
        self.client.close()

    def __del__(self):
        """
        Close the httpx clients when the object is deleted.
        """
        self.close()

    def __enter__(self):
        """
        Return the source object.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Close the source object.
        """
        self.close()

    def __getstate__(self) -> dict[str, Any]:
        """
        Return the state of the object for pickling.
        """
        state: dict[str, Any] = self.__dict__.copy()
        for attr in ("client", "async_client", "s3_client"):
            state.pop(attr, None)
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        """
        Restore the state of the object from pickling.
        """
        self.__dict__.update(state)
        self.client = self._init_httpx_client()
        self.async_client = self._init_httpx_async_client()
        self.s3_client = get_s3_client()

    def _get_response(
        self,
        url: str,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, Any]] = None,
    ) -> httpx.Response:
        """
        Perform a GET request to the specified URL.

        Args:
            url (str): URL to GET.
            params (Optional[dict[str, Any]]): Parameters to include in the request.
            headers (Optional[dict[str, Any]]): Headers to include in the request.

        Returns:
            httpx.Response: Response object.
        """
        # merge headers
        request_headers = self.client.headers.copy()
        if headers:
            request_headers.update(headers)

        # get the response
        try:
            LOGGER.info("GET %s", url)
            response = self.client.get(url, headers=request_headers, params=params)
            # check the response for the x-rate-limit headers
            for key, value in response.headers.items():
                if key.lower() == "x-ratelimit-limit":
                    self.rate_limit_limit = int(value)
                elif key.lower() == "x-ratelimit-remaining":
                    self.prior_rate_limit_remaining = self.rate_limit_remaining
                    self.rate_limit_remaining = int(value)

            # check the response
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            LOGGER.error("HTTP Status Error: %s", e)
            raise e

        return response

    def _post_response(
        self,
        url: str,
        data: Optional[dict[str, Any]] = None,
        json_data: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, Any]] = None,
    ) -> httpx.Response:
        """
        Perform a POST request to the specified URL.

        Args:
            url (str): URL to POST.
            data (str | bytes | Optional[dict[str, Any]]): Data to include in the request.
            json_data (Optional[dict[str, Any]]): Data to include in the request as JSON.
            params (Optional[dict[str, Any]]): Parameters to include in the request.
            headers (Optional[dict[str, Any]]): Headers to include in the request.

        Returns:
            httpx.Response: Response object.
        """
        # merge headers
        request_headers = self.client.headers.copy()
        if headers:
            request_headers.update(headers)

        # get the response
        try:
            LOGGER.info("POST %s", url)
            response = self.client.post(
                url, data=data, json=json_data, params=params, headers=request_headers
            )

            # check the response
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            LOGGER.error("HTTP Status Error: %s", e)
            raise e

        return response

    def _get(
        self,
        url: str,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, Any]] = None,
    ) -> bytes:
        """
        Perform a GET request to the specified URL.

        Args:
            url (str): URL to GET.
            params (Optional[dict[str, Any]]): Parameters to include in the request.
            headers (Optional[dict[str, Any]]): Headers to include in the request.

        Returns:
            bytes: Response content.
        """
        request_headers = dict(self.client.headers.copy())
        if headers:
            request_headers.update(headers)

        return self._get_response(
            url=url, params=params, headers=request_headers
        ).content

    def _post(
        self,
        url: str,
        data: Optional[dict[str, Any]] = None,
        json_data: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, Any]] = None,
    ) -> bytes:
        """
        Perform a POST request to the specified URL.

        Args:
            url (str): URL to POST.
            data (Optional[dict[str, Any]]): Data to include in the request.
            json_data (Optional[dict[str, Any]]): Data to include in the request as JSON.
            params (Optional[dict[str, Any]]): Parameters to include in the request.
            headers (Optional[dict[str, Any]]): Headers to include in the request.

        Returns:
            bytes: Response content.
        """
        request_headers = dict(self.client.headers.copy())
        if headers:
            request_headers.update(headers)

        return self._post_response(
            url=url,
            data=data,
            json_data=json_data,
            params=params,
            headers=request_headers,
        ).content

    def _get_json(
        self,
        url: str,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """
        Perform a GET request to the specified URL and return the JSON response.

        Args:
            url (str): URL to GET.
            params (Optional[dict[str, Any]]): Parameters to include in the request.
            headers (Optional[dict[str, Any]]): Headers to include in the request.

        Returns:
            dict[str, Any]: JSON response content.
        """
        request_headers = dict(self.client.headers.copy())
        if headers:
            request_headers.update(headers)
        return json.loads(self._get(url=url, params=params, headers=request_headers))

    def _get_json_list(
        self,
        url: str,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        """
        Perform a GET request to the specified URL and return the JSON response as a list.

        Args:
            url (str): URL to GET.
            params (Optional[dict[str, Any]]): Parameters to include in the request.
            headers (Optional[dict[str, Any]]): Headers to include in the request.

        Returns:
            list[dict[str, Any]]: JSON response content as a list.
        """
        json_data = self._get_json(url=url, params=params, headers=headers)
        if isinstance(json_data, list):
            return json_data
        return [json_data]

    def _post_json(
        self,
        url: str,
        data: Optional[dict[str, Any]] = None,
        json_data: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """
        Perform a POST request to the specified URL and return the JSON response.

        Args:
            url (str): URL to POST.
            data (Optional[dict[str, Any]]): Data to include in the request.
            json_data (Optional[dict[str, Any]]): Data to include in the request as JSON.
            params (Optional[dict[str, Any]]): Parameters to include in the request.
            headers (Optional[dict[str, Any]]): Headers to include in the request.

        Returns:
            dict[str, Any]: JSON response content.
        """
        request_headers = dict(self.client.headers.copy())
        if headers:
            request_headers.update(headers)
        return json.loads(
            self._post(
                url=url,
                data=data,
                json_data=json_data,
                params=params,
                headers=request_headers,
            )
        )

    def _get_html(
        self,
        url: str,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, Any]] = None,
    ) -> lxml.html.HtmlElement:
        """
        Perform a GET request to the specified URL and return the HTML response.

        Args:
            url (str): URL to GET.
            params (Optional[dict[str, Any]]): Parameters to include in the request.
            headers (Optional[dict[str, Any]]): Headers to include in the request.

        Returns:
            lxml.html.HtmlElement: HTML response content.
        """
        request_headers = dict(self.client.headers.copy())
        if headers:
            request_headers.update(headers)
        return lxml.html.fromstring(
            self._get(url=url, params=params, headers=request_headers)
        )

    def _get_playwright_html(
        self, url: str, headers: Optional[dict[str, Any]] = None
    ) -> lxml.html.HtmlElement:
        """
        Perform a GET request to the specified URL and return the HTML response using Playwright.

        Args:
            url (str): URL to GET.
            headers (Optional[dict[str, Any]]): Headers to include in the request.

        Returns:
            lxml.html.HtmlElement: HTML response content.
        """
        request_headers = dict(self.client.headers.copy())
        if headers:
            request_headers.update(headers)

        # get the page content
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(
                extra_http_headers=request_headers,
                user_agent=self.client.headers["User-Agent"],
            )
            LOGGER.info("playwright GET %s", url)
            page.goto(url, timeout=CONFIG.default_http_timeout * 1000)
            LOGGER.info("playwright waiting for network idle")
            page.wait_for_load_state(
                "networkidle", timeout=CONFIG.default_http_timeout * 1000
            )
            content = page.content()
            LOGGER.info("playwright retrieved %d bytes", len(content))
            browser.close()

        return lxml.html.fromstring(content)

    def _get_xml(
        self,
        url: str,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, Any]] = None,
    ) -> lxml.etree.Element:
        """
        Perform a GET request to the specified URL and return the XML response.

        Args:
            url (str): URL to GET.
            params (Optional[dict[str, Any]]): Parameters to include in the request.
            headers (Optional[dict[str, Any]]): Headers to include in the request.

        Returns:
            lxml.etree.Element: XML response content.
        """
        # create huge tree recovering parser
        parser = lxml.etree.XMLParser(recover=True, huge_tree=True)
        return lxml.etree.fromstring(
            self._get(url=url, params=params, headers=headers), parser=parser
        )

    def _post_xml(
        self,
        url: str,
        data: Optional[dict[str, Any]] = None,
        json_data: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, Any]] = None,
    ) -> lxml.etree.Element:
        """
        Perform a POST request to the specified URL and return the XML response.

        Args:
            url (str): URL to POST.
            data (Optional[dict[str, Any]]): Data to include in the request.
            params (Optional[dict[str, Any]]): Parameters to include in the request.
            headers (Optional[dict[str, Any]]): Headers to include in the request.

        Returns:
            lxml.etree.Element: XML response content.
        """
        return lxml.etree.fromstring(
            self._post(
                url=url, data=data, json_data=json_data, params=params, headers=headers
            )
        )

    def check_id(self, document_id: int | str) -> bool:
        """
        Check if a document exists in the source.

        Args:
            document_id (int | str): Document ID.

        Returns:
            bool: Whether the document exists.
        """
        key_prefix = f"documents/{self.metadata.dataset_id}/{document_id}"
        return check_prefix_exists(self.s3_client, CONFIG.default_s3_bucket, key_prefix)

    @abc.abstractmethod
    def download_id(
        self, document_id: int | str, **kwargs: dict[str, Any]
    ) -> SourceDownloadStatus:
        """
        Download data from the source for a specific id.

        Args:
            document_id (int | str): Document ID.
            **kwargs: Additional keyword arguments.
        """

    @abc.abstractmethod
    def download_date(
        self, date: datetime.date, **kwargs: dict[str, Any]
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Download data from the source for a specific date.

        Args:
            date (str): Date to download data for.
            kwargs (dict): Additional keyword arguments.

        Returns:
            Generator[dict, None, None]: A generator of status/progress messages.
        """

    @abc.abstractmethod
    def download_date_range(
        self,
        start_date: datetime.date | datetime.datetime,
        end_date: datetime.date | datetime.datetime,
        **kwargs: dict[str, Any],
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Download data from the source for a specific date range.

        Args:
            start_date (str): Start date to download data for.
            end_date (str): End date to download data for.
            kwargs (dict): Additional keyword arguments.

        Returns:
            Generator[dict, None, None]: A generator of status/progress messages.
        """

    @abc.abstractmethod
    def download_all(
        self, **kwargs: dict[str, Any]
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Download all data from the source.

        Args:
            kwargs (dict): Additional keyword arguments.

        Returns:
            Generator[dict, None, None]: A generator of status/progress messages.
        """
