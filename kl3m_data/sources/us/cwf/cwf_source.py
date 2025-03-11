"""
This module contains the CIA World Factbook source.

References:
    -
"""

import hashlib

# imports
import datetime
import time
import urllib.request
import urllib.parse
from typing import Any, Generator

# packages
import lxml.html

# project
from kl3m_data.logger import LOGGER
from kl3m_data.sources import Document
from kl3m_data.sources.base_source import (
    BaseSource,
    SourceMetadata,
    SourceDownloadStatus,
    SourceProgressStatus,
)

ARCHIVE_URL = "https://www.cia.gov/the-world-factbook/about/archives/2024/"


class CWFSource(BaseSource):
    """
    Represents a source for the current United States Code.
    """

    def __init__(self, **kwargs):
        """
        Initialize the source.

        Args:
            update (bool): Whether to update the source.
            delay (int): Delay between requests
        """
        # set the metadata
        metadata = SourceMetadata(
            dataset_id="cia-world-factbook",
            dataset_home="https://www.cia.gov/the-world-factbook/",
            dataset_description="The most current version of the CIA World Factbook.",
            dataset_license="Not subject to copyright under 17 U.S.C. 105.",
        )

        # call the super
        super().__init__(metadata)

        # track seen pages
        self.seen_pages: set[str] = set()

    def download_id(
        self, document_id: int | str, **kwargs: dict[str, Any]
    ) -> SourceDownloadStatus:
        """
        Download a document by its ID.

        Args:
            document_id (int | str): The document ID.
            **kwargs: Additional keyword arguments.

        Returns:
            SourceDownloadStatus: The download status.
        """
        pass

    def download_date(
        self, date: datetime.date, **kwargs: dict[str, Any]
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Download a document by its date.

        Args:
            date (datetime.date): The document date.
            **kwargs: Additional keyword arguments.

        Returns:
            SourceDownloadStatus: The download status.
        """
        pass

    def download_date_range(
        self,
        start_date: datetime.date | datetime.datetime,
        end_date: datetime.date | datetime.datetime,
        **kwargs: dict[str, Any],
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Download documents within a date range.

        Args:
            start_date (datetime.date | datetime.datetime): The start date.
            end_date (datetime.date | datetime.datetime): The end date.
            **kwargs: Additional keyword arguments.

        Returns:
            SourceDownloadStatus: The download status.
        """
        pass

    def _fetch_and_continue(self, url: str) -> Generator[Document, None, None]:
        """
        Fetch the page and continue to the next link.

        Args:
            url (str): The URL to fetch.

        Returns:
            dict: The page content.
        """
        # don't retrieve again
        if url in self.seen_pages:
            LOGGER.info("Already seen %s", url)
            return

        # mark as seen at the start
        self.seen_pages.add(url)

        # get the page
        LOGGER.info("Fetching %s", url)
        page_response = self.client.get(url)
        page_doc = lxml.html.fromstring(page_response.content)

        # get all links that are either on the same domain or relative
        for link in page_doc.xpath("//a/@href"):
            # get the full url either as relative or absolute
            if link.startswith("http"):
                full_url = link
            elif link.startswith("/"):
                full_url = f"https://www.cia.gov{link}"
            else:
                full_url = urllib.parse.urljoin(url, link)

            # fetch the page and yield
            if ARCHIVE_URL in full_url and full_url not in self.seen_pages:
                LOGGER.info("Continuing into %s", full_url)
                for child_result in self._fetch_and_continue(full_url):
                    yield child_result
                    time.sleep(0.5)

        # get the html title
        try:
            title = page_doc.xpath("//title/text()")
            title = title[0] if title else None
        except Exception as e:  # pylint: disable=broad-except
            LOGGER.error("Error getting title: %s", str(e))
            title = None

        # get id as relative path
        path_id = url.replace(ARCHIVE_URL, "").strip()

        # yield the page content
        doc = Document(
            dataset_id=self.metadata.dataset_id,
            id=path_id,
            blake2b=hashlib.blake2b(page_response.content).hexdigest()
            if page_response.content
            else None,
            content=page_response.content,
            size=len(page_response.content) if page_response.content else None,
            title=title,
            identifier=url,
            format="text/html",
            creator=[
                "CIA",
            ],
            publisher="CIA",
            description=title,
            subject=["CIA World Factbook"],
        )

        # push to s3
        doc.to_s3()

        # yield the page
        return doc

    def download_all(
        self, **kwargs: dict[str, Any]
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Download all documents.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            SourceDownloadStatus: The download status.
        """
        # init progress
        current_progress = SourceProgressStatus(
            total=None,
            description="Downloading CIA World Factbook documents",
        )

        # start from /countries/ page
        for doc in self._fetch_and_continue(ARCHIVE_URL):
            # update progress
            if doc is None:
                current_progress.failure += 1
                current_progress.status = False
                continue

            # update progress
            current_progress.success += 1
            yield current_progress


if __name__ == "__main__":
    source = CWFSource()
    for x in source.download_all():
        print(x)
