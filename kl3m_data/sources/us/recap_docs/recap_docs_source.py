"""
RECAP/PACER doc/attachment source
"""

# imports
import datetime
import hashlib
import mimetypes
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
from kl3m_data.utils.s3_utils import get_s3_client, iter_prefix, get_object_bytes

# constants
RECAP_BUCKET = "com-courtlistener-storage"
RECAP_PREFIX_LIST = ("doc", "docx", "mp3", "pdf", "wpd")


class RECAPDocSource(BaseSource):
    """
    RECAP doc/attachment source
    """

    def __init__(self, **kwargs: dict[str, Any]):
        """
        Initialize the source.

        Args:
            update (bool): Whether to update the source.
            delay (int): Delay between requests
        """
        # set the metadata
        metadata = SourceMetadata(
            dataset_id="recap_docs",
            dataset_home="s3://com-courtlistener-storage/",
            dataset_description="Documents and filing attachments retrieved by the Free Law Project/RECAP project.",
            dataset_license="CC0/Public Domain",
        )

        # call the super
        super().__init__(metadata)

        self.update = kwargs.get("update", False)
        self.delay = kwargs.get("delay", 0)

        # initialize the s3 client
        self.s3_client = get_s3_client()

        # dedupe some objects as we go
        self.seen_hashes: set[str] = set()

    def download_id(
        self, document_id: int | str, **kwargs: dict[str, Any]
    ) -> SourceDownloadStatus:
        raise NotImplementedError

    def download_date(
        self, date: datetime.date, **kwargs: dict[str, Any]
    ) -> Generator[SourceProgressStatus, None, None]:
        raise NotImplementedError

    def download_date_range(
        self,
        start_date: datetime.date | datetime.datetime,
        end_date: datetime.date | datetime.datetime,
        **kwargs: dict[str, Any],
    ) -> Generator[SourceProgressStatus, None, None]:
        raise NotImplementedError

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
            description="Downloading RECAP objects",
        )

        # iterate through bucket and create corresponding Document objects
        for prefix in RECAP_PREFIX_LIST:
            for doc_key in iter_prefix(self.s3_client, RECAP_BUCKET, prefix):
                try:
                    # get basic field
                    doc_filename = doc_key[len(prefix) :]

                    current_progress.extra = {
                        "filename": doc_filename,
                    }

                    if self.check_id(doc_filename):
                        LOGGER.info("Skipping existing document: %s", doc_filename)
                        current_progress.success += 1
                        continue

                    # fetch the pdf object
                    doc_content = get_object_bytes(
                        self.s3_client,
                        RECAP_BUCKET,
                        doc_key,
                    )

                    # skip if missing
                    if not doc_content:
                        LOGGER.error("Error fetching object: %s", doc_key)
                        current_progress.failure += 1
                        current_progress.status = False
                        continue

                    # check if we've already seen it
                    doc_hash = hashlib.blake2b(doc_content).hexdigest()
                    if doc_hash in self.seen_hashes:
                        LOGGER.info("Skipping duplicate document: %s", doc_filename)
                        current_progress.success += 1
                        continue

                    # add to seen
                    self.seen_hashes.add(doc_hash)

                    # get mime type
                    mime_info = mimetypes.guess_type(doc_filename)
                    if mime_info:
                        doc_mime = mime_info[0]
                    else:
                        doc_mime = "application/octet-stream"

                    document = Document(
                        dataset_id=self.metadata.dataset_id,
                        id=doc_filename,
                        identifier="s3://" + RECAP_BUCKET + "/" + doc_key,
                        content=doc_content,
                        size=len(doc_content),
                        blake2b=doc_hash,
                        format=doc_mime,
                        source="RECAP",
                        publisher="Free Law Project",
                    )

                    # push to s3
                    document.to_s3()

                    # increment
                    current_progress.success += 1
                except Exception as e:  # pylint: disable=broad-except
                    LOGGER.error("Error uploading object %s: %s", doc_key, e)
                    current_progress.message = str(e)
                    current_progress.failure += 1
                    current_progress.status = False
                finally:
                    # yield progress
                    current_progress.current += 1
                    yield current_progress
                    current_progress.message = None


if __name__ == "__main__":
    source = RECAPDocSource()
    for s in source.download_all():
        continue
