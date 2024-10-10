"""
.gov TLD domain sources
"""

# imports
import datetime
import hashlib
import json
import mimetypes
import re
from pathlib import Path
from typing import Any, Generator

# packages
import pypdfium2

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
FILE_INDEX_PATH = "/nas1/workspace-alea/kl3m-data-api/dotgov_files.jsonl"
FILE_BASE_PATH = "/nas3/data/kl3m-001/us/dotgov/input/"

INCLUDE_MIME_TYPES = {
    "application/atom+xml",
    "application/eps",
    "application/epub+zip",
    # 'application/json',
    "application/msword",
    "application/pdf",
    "application/postscript",
    "application/rss+xml",
    "application/rtf",
    "application/vnd.ms-excel",
    "application/vnd.ms-powerpoint",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "application/vnd.openxmlformats-officedocument.presentationml.template",
    "application/vnd.openxmlformats-officedocument.presentationml.slideshow",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.template",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.template",
    "application/vnd.wordperfect",
    "application/x-mobipocket-ebook",
    "application/xml",
    "application/xml-dtd",
    "application/zip",
    "message/rfc822",
    "text/csv",
    "text/html",
    "text/plain",
    "text/xml",
}

EXCLUDE_EXTENSIONS = (
    ".css",
    ".js",
    ".json",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".bmp",
    ".svg",
    ".ico",
    ".webp",
    ".webm",
    ".mp4",
    ".mp3",
    ".wav",
    ".ogg",
    ".flac",
    ".opus",
)

# basic html extraction for title or meta fields
HTML_TITLE_RE = re.compile(r"<title>(.*?)</title>", re.IGNORECASE | re.UNICODE)
HTML_META_RE = re.compile(
    r'<meta\s+name="([^"]+)"\s+content="([^"]+)"', re.IGNORECASE | re.UNICODE
)


def is_valid_file(record: dict) -> tuple[bool, str]:
    """
    Check if the file is valid to be processed.

    Args:
        record (dict): The file record.

    Returns:
        bool: True if the file is valid, False otherwise.
    """
    # check if the file is valid
    valid = False

    # get key fields for deciding
    member_file_path = Path(FILE_BASE_PATH) / record["member_file"]
    member_file_name = member_file_path.name
    member_file_type_bytes = record["mime_type"]
    try:
        member_file_type_extension = mimetypes.guess_type(member_file_name)[0]
    except Exception:  # pylint: disable=broad-except
        member_file_type_extension = None

    # get size
    member_size = record.get("size", 0) or 0

    # normalize the extension in case it's got a query string
    member_extension = member_file_path.suffix
    if "?" in member_extension:
        member_extension = member_extension.split("?")[0]

    # best extension guess
    best_guess_type = member_file_type_bytes
    if member_file_type_bytes in (None, "text/plain", "application/octet-stream"):
        if member_file_type_extension not in (
            None,
            "text/plain",
            "application/octet-stream",
        ):
            best_guess_type = member_file_type_extension

    # check if the file is valid
    if member_size > 0:
        if member_extension.lower() not in EXCLUDE_EXTENSIONS:
            if best_guess_type in INCLUDE_MIME_TYPES:
                valid = True

    return (valid, best_guess_type)


def get_metadata(content: bytes, mime_type: str) -> dict:
    """
    Get basic metadata for the content.

    Args:
        content (bytes): The content to get metadata for.
        mime_type (str): The mime type of the content.
    """
    metadata: dict[str, Any] = {"title": None}

    # handle html type
    if mime_type in ("text/html", "application/xhtml+xml"):
        # do not parse at this scale; just use regex
        title_match = HTML_TITLE_RE.search(content.decode())
        meta_matches = HTML_META_RE.findall(content.decode())

        # try to find the best title
        if title_match:
            metadata["title"] = title_match.group(1)
        else:
            for name, value in meta_matches:
                if name.lower() == "title":
                    metadata["title"] = value
                    break

        # map the rest into dict fields
        for name, value in meta_matches:
            metadata[name] = value

    # handle pdf next
    if mime_type == "application/pdf":
        # parse
        pdf_doc = pypdfium2.PdfDocument(content)

        # add metadata directly
        metadata.update(pdf_doc.get_metadata_dict())

        # add direct page count
        metadata["page_count"] = len(pdf_doc)

    return metadata


class DotGovDocSource(BaseSource):
    """
    .gov website document source
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
            dataset_id="dotgov",
            dataset_home="https://get.gov/about/data/",
            dataset_description="US Federal government website content",
            dataset_license="Public domain under 17 U.S. Code § 105 after filtering copyrighted material and excluded agencies/GSEs",
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

        with open(FILE_INDEX_PATH, "rt", encoding="utf-8") as index_file:
            for line in index_file:
                current_progress.total += 1  # type: ignore
                try:
                    record = json.loads(line)

                    valid, record_type = is_valid_file(record)
                    if not valid:
                        LOGGER.info("Skipping invalid file %s", record["member_file"])
                        continue

                    # check if it already exists
                    if self.check_id(record["member_file"]):
                        LOGGER.info("Document %s already exists", record["member_file"])
                        current_progress.success += 1
                        continue

                    # get full path combining the website and the file path
                    member_file_path = Path(FILE_BASE_PATH) / record["member_file"]

                    # get source as domain
                    source = record["member_file"].split("/")[0]

                    # get contents and hash
                    content = member_file_path.read_bytes()
                    content_hash = hashlib.blake2b(content).hexdigest()
                    content_size = len(content)

                    # try to get basic metadata
                    metadata = get_metadata(content, record_type)

                    if metadata.get("title"):
                        title = metadata["title"]
                    elif metadata.get("Title"):
                        title = metadata["Title"]
                    else:
                        title = member_file_path.name

                    if metadata.get("description"):
                        description = metadata["description"]
                    elif metadata.get("Description"):
                        description = metadata["Description"]
                    else:
                        description = None

                    document = Document(
                        dataset_id=self.metadata.dataset_id,
                        id=record["member_file"],
                        identifier=record["member_file"],
                        format=record_type,
                        title=title,
                        description=description,
                        source=source,
                        content=content,
                        blake2b=content_hash,
                        size=content_size,
                        extra=metadata,
                    )

                    # upload the document
                    document.to_s3()
                    current_progress.success += 1
                except Exception as e:  # pylint: disable=broad-except
                    LOGGER.error("Error downloading document %s: %s", document["id"], e)
                    current_progress.message = str(e)
                    current_progress.failure += 1
                    current_progress.status = False
                finally:
                    current_progress.current += 1
                    yield current_progress
                    current_progress.message = None

        # finalize progress
        current_progress.status = True
        yield current_progress
