"""
regulations.gov documents package
"""

# imports
import datetime
import hashlib
import os
import time
from typing import Any, Generator


# packages
import httpx

# project
from kl3m_data.logger import LOGGER
from kl3m_data.sources.base_document import Document
from kl3m_data.sources.base_source import (
    BaseSource,
    SourceMetadata,
    SourceDownloadStatus,
    SourceProgressStatus,
)
from kl3m_data.utils.s3_utils import get_s3_client

# constants
BASE_API_URL = "https://api.regulations.gov/v4"
REG_MIN_DATE = datetime.date(2010, 2, 1)
REG_MAX_DATE = datetime.date.today()


class RegulationsDocSource(BaseSource):
    """
    Regulations.gov document source
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
            dataset_id="reg_docs",
            dataset_home="regulations.gov",
            dataset_description="Documents submitted as addenda or comments to regulations.gov",
            dataset_license="https://www.regulations.gov/user-notice; unrestricted third party use",
        )

        # call the super
        super().__init__(metadata)

        # set the base url
        self.base_url = BASE_API_URL
        if "base_url" in kwargs:
            self.base_url = kwargs["base_url"]

        # set api key from kwarg or env var
        self.api_key = kwargs.get("api_key", os.getenv("REGULATIONS_API_KEY", None))
        if not self.api_key:
            raise ValueError(
                "API key is required for regulations.gov source; please set REGULATIONS_API_KEY environment variable."
            )

        self.update = kwargs.get("update", False)
        self.delay = kwargs.get("delay", 0)
        self.rate_limit = 0

        # initialize the s3 client
        self.s3_client = get_s3_client()

        # dedupe some objects as we go
        self.seen_hashes: set[str] = set()

        # set the date range
        self.min_date = REG_MIN_DATE
        if "min_date" in kwargs:
            self.min_date = kwargs["min_date"]

        self.max_date = REG_MAX_DATE
        if "max_date" in kwargs:
            self.max_date = kwargs["max_date"]

    def update_rate_limit(self, headers: httpx.Headers) -> None:
        """Update the rate limit tracking and dynamically adjust the delay based on
        whether the number of remaining requests has increased or decreased.

        Args:
            headers (httpx.Headers): The headers from the response.
        """
        # parse headers
        try:
            new_rate_limit = int(headers["x-ratelimit-limit"])
            self.rate_limit = new_rate_limit
        except KeyError:
            LOGGER.warning("Rate limit headers not found in response")

        # update the rate limit remaining
        try:
            new_rate_limit_remaining = int(headers["x-ratelimit-remaining"])
        except KeyError:
            LOGGER.warning("Rate limit remaining not found in response")
            return

        if self.rate_limit_remaining:
            if new_rate_limit_remaining > self.rate_limit_remaining:
                # decrease delay by 2.5%
                self.delay = max(3.0, self.delay * 0.975)
            elif new_rate_limit_remaining < self.rate_limit_remaining:
                # increase delay by 5%
                self.delay = min(7.0, self.delay * 1.05)
            else:
                # decrease by 0.5%
                self.delay = max(4.0, self.delay * 0.995)

        self.rate_limit_remaining = new_rate_limit_remaining
        LOGGER.info(
            "Rate limit: %s/%s; Delay: %ss",
            self.rate_limit_remaining,
            self.rate_limit,
            self.delay,
        )

    def get_url(self, url: str) -> str:
        """
        Get the full URL for an API endpoint.

        Args:
            url: The endpoint URL.

        Returns:
            str: The full URL.
        """
        return f"{self.base_url}/{url.lstrip('/')}"

    def search_date(self, date: datetime.date, page_size: int = 250) -> list[dict]:
        """
        Search for documents by date.

        Args:
            date: The date to search for.
            page_size: The number of documents to return in each page.

        Returns:
            list[dict]: The search results.
        """
        # set up query params
        date_string = date.strftime("%Y-%m-%d")
        page_number = 1

        # aggregate the results
        results = []
        while True:
            # execute the search
            search_url = self.get_url(
                f"documents?filter[postedDate]={date_string}"
                "&include=attachments"
                f"&page[size]={page_size}"
                f"&page[number]={page_number}"
                f"&api_key={self.api_key}"
            )
            search_response = self._get_response(search_url)
            search_data = search_response.json()

            # update the delay and sleep
            self.update_rate_limit(search_response.headers)
            time.sleep(self.delay)

            # add the data to the results
            results.extend(search_data.get("data", []))

            # break if no data
            if not search_data["meta"]["hasNextPage"]:
                break

            # otherwise, bump the page number
            page_number += 1

        return results

    def download_comments(
        self, document_id: int | str, page_size: int = 250
    ) -> list[dict]:
        """
        Download comments for a document.

        https://api.regulations.gov/v4/comments?filter[commentOnId]=09000064846eebaf

        Args:
            document_id: The document ID.
            page_size: The number of comments to return in each page.

        Returns:
            list[dict]: The comments.
        """
        # set up query params
        page_number = 1

        # aggregate the results
        results = []
        while True:
            # execute the search
            search_url = self.get_url(
                f"comments?"
                f"filter[commentOnId]={document_id}"
                f"&page[size]={page_size}"
                f"&page[number]={page_number}"
                f"&api_key={self.api_key}"
            )
            search_response = self._get_response(search_url)
            search_data = search_response.json()

            # update the delay and sleep
            self.update_rate_limit(search_response.headers)
            time.sleep(self.delay)

            # add the data to the results
            results.extend(search_data.get("data", []))

            # break if no data
            if not search_data["meta"]["hasNextPage"]:
                break

            # otherwise, bump the page number
            page_number += 1

        return results

    def download_file_formats(
        self, file_format_records: list[dict], metadata: dict
    ) -> SourceDownloadStatus:
        """
        Download all file formats for a document.

        Args:
            file_format_records: The file format records.
            metadata: The metadata for the document.

        Returns:
            SourceDownloadStatus: The download status.
        """
        try:
            for file_format_record in file_format_records:
                # download and parse this
                file_url = file_format_record.get("fileUrl")
                if isinstance(file_url, list):
                    file_url = file_url[0]

                # set the id as the last two parts of the url
                id_ = file_url.replace("https://downloads.regulations.gov/", "").strip()
                identifier = file_url

                # check if it exists
                if self.check_id(id_):
                    LOGGER.info("Document %s already exists", id_)
                    continue

                # get the content if we don't already have it
                file_response = self._get_response(file_url)
                self.update_rate_limit(file_response.headers)
                time.sleep(self.delay)

                # get the content type
                content_type = "application/pdf"
                for key, value in file_response.headers.items():
                    if key.lower() == "content-type":
                        content_type = value

                # get the title
                title = metadata.get("title", None)

                # try to parse the postedDate
                try:
                    date_string = metadata.get("attributes", {}).get("postedDate")
                    posted_date = datetime.datetime.fromisoformat(date_string)
                except (ValueError, TypeError):
                    posted_date = None

                creators = []
                if metadata.get("agencyId"):
                    creators.append(metadata.get("agencyId"))

                # create the document
                document = Document(
                    dataset_id=self.metadata.dataset_id,
                    id=id_,
                    identifier=identifier,
                    title=title,
                    date=posted_date,
                    format=content_type,
                    content=file_response.content,
                    size=len(file_response.content),
                    blake2b=hashlib.blake2b(file_response.content).hexdigest(),
                    publisher="eRulemaking Program Management Office",
                    creator=creators,
                    source="https://regulations.gov/",
                    extra={
                        **metadata,
                    },
                )

                # upload to s3
                document.to_s3()

            return SourceDownloadStatus.SUCCESS
        except Exception as e:  # pylint: disable=broad-except
            LOGGER.error("Error downloading records: %s", e)
            return SourceDownloadStatus.FAILURE

    def download_id(
        self, document_id: int | str, **kwargs: dict[str, Any]
    ) -> SourceDownloadStatus:
        """
        Download a document by its ID, which is the regulation ID:
        EPA-HQ-OPP-2007-1024-0726
        * "fileUrl" : "https://downloads.regulations.gov/EPA-HQ-OPP-2007-1024-0726/content.pdf"

        Args:
            document_id: The document ID.
            **kwargs: Additional arguments.

        Returns:
            SourceDownloadStatus: The download status.
        """
        metadata = kwargs.get("metadata", None)
        if not metadata:
            raise ValueError("Metadata is required for regulations.gov source.")

        # get the full detail url with all attachment URLs
        detail_url = self.get_url(
            f"documents/{document_id}?include=attachments&api_key={self.api_key}"
        )

        # get the document data
        document_response = self._get_response(detail_url)
        document_data = document_response.json()

        # update the delay and sleep
        self.update_rate_limit(document_response.headers)
        time.sleep(self.delay)

        # iterate through all fileFormats
        statuses = []
        try:
            # do the main body file format records
            file_format_records = (
                document_data.get("data", {})
                .get("attributes", {})
                .get("fileFormats", [])
                or []
            )
            statuses.append(self.download_file_formats(file_format_records, metadata))

            # add any included/attributes/fileFormats to the list
            included_records = document_data.get("included", [])
            if len(included_records) > 0:
                for include in included_records:
                    file_format_records = (
                        include.get("attributes", {}).get("fileFormats", []) or []
                    )
                    attachment_metadata = metadata.copy()
                    for key in include.keys():
                        attachment_metadata[key] = include[key]
                    statuses.append(
                        self.download_file_formats(
                            file_format_records, attachment_metadata
                        )
                    )

            # return the status based on all statuses
            if all(
                status in (SourceDownloadStatus.SUCCESS, SourceDownloadStatus.EXISTED)
                for status in statuses
            ):
                return SourceDownloadStatus.SUCCESS
            return SourceDownloadStatus.FAILURE
        except Exception as e:  # pylint: disable=broad-except
            LOGGER.error("Error downloading document %s: %s", document_id, e)
            return SourceDownloadStatus.FAILURE

    def download_date(
        self, date: datetime.date, **kwargs: dict[str, Any]
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Get all documents for a specific date.

        Args:
            date: The date to download.
            **kwargs: Additional arguments.

        Returns:
            SourceProgressStatus: The download status.
        """
        # get current progress
        current_progress = SourceProgressStatus(
            total=None,
            description=f"Downloading regulations.gov docs for {date}",
        )

        # get all documents for this date
        date_results = self.search_date(date)
        current_progress.total = len(date_results)

        # iterate through all documents
        for document in date_results:
            try:
                # download the document and update progress
                status = self.download_id(document["id"], metadata=document)
                if status in (
                    SourceDownloadStatus.SUCCESS,
                    SourceDownloadStatus.EXISTED,
                ):
                    current_progress.success += 1
                else:
                    current_progress.failure += 1
            except Exception as e:  # pylint: disable=broad-except
                LOGGER.error("Error downloading document %s: %s", document["id"], e)
                current_progress.message = str(e)
                current_progress.failure += 1
                current_progress.status = False
            finally:
                current_progress.current += 1
                yield current_progress
                current_progress.message = None

        # finalize
        current_progress.done = True
        yield current_progress

    def download_date_range(
        self,
        start_date: datetime.date | datetime.datetime,
        end_date: datetime.date | datetime.datetime,
        **kwargs: dict[str, Any],
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Download all documents in a date range.

        Args:
            start_date: The start date.
            end_date: The end date.
            **kwargs: Additional arguments.

        Returns:
            SourceProgressStatus: The download status.
        """
        # init progress
        current_progress = SourceProgressStatus(
            total=None,
            description="Downloading regulations.gov docs",
        )

        # iterate through all dates
        current_date = start_date
        while current_date <= end_date:
            yield from self.download_date(current_date, **kwargs)
            current_date += datetime.timedelta(days=1)

        # return the final status
        current_progress.done = True
        yield current_progress

    def download_all(
        self, **kwargs: dict[str, Any]
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Iterate through all objects in the S3 bucket and create the relevant Documents.

        Args:
            **kwargs: Additional arguments.

        Returns:
            SourceDownloadStatus: The download status.
        """
        # init progress
        current_progress = SourceProgressStatus(
            total=None,
            description="Downloading regulations.gov docs",
        )

        # get all dates
        current_date = self.min_date
        while current_date <= self.max_date:
            yield from self.download_date(current_date, **kwargs)
            current_date += datetime.timedelta(days=1)

        # return the final status
        current_progress.done = True
        yield current_progress
