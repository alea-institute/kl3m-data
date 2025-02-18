"""
Case Law Access Project (CAP) source
"""

# imports
import datetime
import gzip
import hashlib
import json
import io
import zipfile
from pathlib import Path
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


ZIP_URL_PATH = Path(__file__).parent / "zip_urls.txt.gz"


class CAPDocSource(BaseSource):
    """
    Case Law Access Project (CAP) source
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
            dataset_id="cap",
            dataset_home="https://static.case.law/",
            dataset_description="Caselaw Access Project (CAP) is a project of the Harvard Law School Library dedicated to digitizing and making available U.S. court opinions.",
            dataset_license="CC0 1.0 Universal",
        )

        # call the super
        super().__init__(metadata)

        # set the kwargs
        self.update = kwargs.get("update", False)
        self.delay = kwargs.get("delay", 0)
        self.rate_limit = 0

        # initialize the s3 client
        self.s3_client = get_s3_client()

    @staticmethod
    def embed_html_fragment(html_fragment: str) -> str:
        """
        Patch the html fragment by embedding it into a proper html doc.

        Args:
            html_fragment (str): The html fragment.

        Returns:
            str: The patched html fragment.
        """
        # patch the html fragment
        return f"""<!DOCTYPE html>\n<html>\n<body>\n{html_fragment}\n</body>\n</html>""".strip()

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
        # each ID is a url to a zip file on static.case.law
        LOGGER.info("Downloading document %s", document_id)
        zip_response = self.client.get(document_id)
        zip_response.raise_for_status()

        # get the zip file with io.BytesIO(zip_response.content)
        with zipfile.ZipFile(io.BytesIO(zip_response.read())) as zip_file:  # type: ignore
            # get the pdf file
            html_path_list = [
                name for name in zip_file.namelist() if name.endswith(".html")
            ]

            for html_path in html_path_list:
                LOGGER.info("Parsing document %s", html_path)

                # read metadata by replacing html/ with json/ and .html with .json
                metadata_path = html_path.replace("html/", "json/").replace(
                    ".html", ".json"
                )
                with zip_file.open(metadata_path) as metadata_file:
                    metadata = json.loads(metadata_file.read().decode("utf-8"))
                    metadata.pop("casebody", None)

                with zip_file.open(html_path) as html_file:
                    html_content = self.embed_html_fragment(
                        html_file.read().decode("utf-8")
                    )

                # create the document
                document = Document(
                    dataset_id=self.metadata.dataset_id,
                    id=metadata["id"],
                    title=metadata["name"],
                    format="text/html",
                    description=metadata["name"],
                    source=self.metadata.dataset_home,
                    license=self.metadata.dataset_license,
                    blake2b=hashlib.blake2b(html_content.encode("utf-8")).hexdigest(),  # type: ignore
                    content=html_content.encode("utf-8"),
                    size=len(html_content),
                    extra=metadata,
                )

                # upload the document
                document.to_s3()

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
            description="Uploading CAP documents",
        )

        # get the zip urls
        with gzip.open(ZIP_URL_PATH, "rt", encoding="utf-8") as zip_url_file:
            for url in zip_url_file:
                # download the file
                LOGGER.info("Parsing ZIP %s", url.strip())
                try:
                    current_progress.extra.update({"url": url.strip()})
                    self.download_id(url.strip())
                    current_progress.success += 1
                except Exception as e:  # pylint: disable=broad-except
                    try:
                        doc_id = url
                    except KeyError:
                        doc_id = None
                    LOGGER.error("Error downloading document %s: %s", doc_id, str(e))
                    current_progress.message = str(e)
                    current_progress.failure += 1
                    current_progress.status = False
                finally:
                    current_progress.current += 1
                    yield current_progress
                    current_progress.message = None

        # done
        current_progress.status = True
        yield current_progress


if __name__ == "__main__":
    # create the source
    source = CAPDocSource(update=True)

    # download all
    for progress in source.download_all():
        print(progress)
