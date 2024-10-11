"""
UK legislation sources:

Documentation:
https://cdn.nationalarchives.gov.uk/documents/cas-82049-legislation-date.pdf
"""

# imports
import datetime
import hashlib
import re
import zipfile
from typing import Any, Generator

# packages


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


# path to revised-all-versions-htm.zip
# assume local symlink as part of setup
# wget http://leggovuk-ldn.s3-website.eu-west-2.amazonaws.com/texts/revised-all-versions/html/revised-all-versions-html.zip
DEFAULT_ZIP_PATH = "/nas3/data/legal/uk/revised-all-versions-htm.zip"

# regex to extract title from HTML
HTML_TITLE_RE = re.compile(r"<title>(.*?)</title>", re.IGNORECASE | re.UNICODE)


class UKLegislationSource(BaseSource):
    """
    UK legislation source
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
            dataset_id="ukleg",
            dataset_home="https://www.legislation.gov.uk/",
            dataset_description="UK legislation",
            dataset_license="All content is available under the Open Government Licence v3.0 except where otherwise stated © Crown copyright",
        )

        # call the super
        super().__init__(metadata)

        # set the kwargs
        self.update = kwargs.get("update", False)
        self.delay = kwargs.get("delay", 0)
        self.rate_limit = 0

        # initialize the s3 client
        self.s3_client = get_s3_client()

    def download_id(
        self, document_id: int | str, **kwargs: dict[str, Any]
    ) -> SourceDownloadStatus:
        """
        Download a document by id.

        Args:
            document_id (int | str): The document id to download.

        Returns:
            SourceDownloadStatus: The download status.
        """
        # not implemented
        raise NotImplementedError("Download by id not implemented")

    def download_date(
        self, date: datetime.date, **kwargs: dict[str, Any]
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Download documents by date.

        Args:
            date (datetime.date): The date to download.

        Yields:
            SourceProgressStatus: The progress status.
        """
        # not implemented
        raise NotImplementedError("Download by date not implemented")

    def download_date_range(
        self,
        start_date: datetime.date | datetime.datetime,
        end_date: datetime.date | datetime.datetime,
        **kwargs: dict[str, Any],
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Download documents by date range.

        Args:
            start_date (datetime.date | datetime.datetime): The start date.
            end_date (datetime.date | datetime.datetime): The end date.

        Yields:
            SourceProgressStatus: The progress status.
        """
        # not implemented
        raise NotImplementedError("Download by date range not implemented")

    def download_all(
        self, **kwargs: dict[str, Any]
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Download all documents.

        Yields:
            SourceProgressStatus: The progress status.
        """
        # init progress
        current_progress = SourceProgressStatus(
            total=0,
            description="Uploading .gov files",
        )

        # set the zip path
        with zipfile.ZipFile(DEFAULT_ZIP_PATH, "r") as zip_archive:
            zip_members = [
                zip_member
                for zip_member in zip_archive.infolist()
                if ".htm" in zip_member.filename.lower()
            ]
            current_progress.total = len(zip_members)
            for zip_member in zip_members:
                try:
                    current_progress.extra = {
                        "file": zip_member.filename,
                    }

                    # check if it already exists on s3
                    if self.check_id(zip_member.filename):
                        LOGGER.info("Document %s already exists", zip_member.filename)
                        current_progress.success += 1
                        continue

                    # read the content
                    content = zip_archive.read(zip_member.filename)
                    content_size = len(content)
                    content_hash = hashlib.blake2b(content).hexdigest()

                    # extract the title
                    title = zip_member.filename
                    title_match = HTML_TITLE_RE.search(content.decode("utf-8"))
                    if title_match:
                        title = title_match.group(1)

                    document = Document(
                        dataset_id=self.metadata.dataset_id,
                        id=zip_member.filename,
                        identifier=zip_member.filename,
                        format="text/html",
                        title=title,
                        source=self.metadata.dataset_home,
                        content=content,
                        blake2b=content_hash,
                        size=content_size,
                    )

                    # upload to s3
                    document.to_s3()

                    current_progress.success += 1
                except Exception as e:  # pylint: disable=broad-except
                    LOGGER.error(
                        "Error downloading document %s: %s", zip_member.filename, str(e)
                    )
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
