"""
FDLP Electronic Collection Archive retrieval.
"""

# imports
import datetime
import hashlib
import urllib.parse
import time
import traceback
from typing import Generator, Optional, Any

# packages
import httpx

# project
from kl3m_data.logger import LOGGER
from kl3m_data.sources.base_document import Document
from kl3m_data.sources.base_source import (
    BaseSource,
    SourceMetadata,
    SourceProgressStatus,
    SourceDownloadStatus,
)
from kl3m_data.sources.us.fdlp.fdlp_types import CGPMetadata


class FDLPSource(BaseSource):
    """
    FDLP Electronic Collection Archive retrieval.
    """

    def __init__(self, **kwargs):
        """
        Initialize the source.

        Args:
            update (bool): Whether to update the source.
            min_lps_id (int): The minimum LPS ID.
            max_lps_id (int): The maximum LPS ID.
            min_gpo_id (int): The minimum GPO ID.
            max_gpo_id (int): The maximum GPO ID.
            delay (int): The delay between requests.
        """
        # set the metadata
        metadata = SourceMetadata(
            dataset_id="fdlp",
            dataset_home="https://permanent.fdlp.gov/",
            dataset_description="FDLP Electronic Collection Archive",
            dataset_license="Available for free use of the general public under 44 USC 1911.",
        )

        # call the super
        super().__init__(metadata)

        # set kwargs
        self.min_lps_id = kwargs.get("min_lps_id", 0)
        self.max_lps_id = kwargs.get("max_lps_id", 250000)
        self.min_gpo_id = kwargs.get("min_gpo_id", 0)
        self.max_gpo_id = kwargs.get("max_gpo_id", 125000)
        self.update = kwargs.get("update", False)
        self.delay = kwargs.get("delay", 1)

    # pylint: disable=too-many-branches,too-many-nested-blocks,too-many-statements
    def get_cgp_metadata(self, document_id: int | str) -> Optional[CGPMetadata]:
        """
        Get the CGP metadata by searching by the document ID with the WUR field, then
        return the table fields from the following page.

        1. Search: https://catalog.gpo.gov/F?func=find-a&find_code=WUR&request={document_id}
        2. Get the first link from the table under the Title column
        3. Follow the link: https://catalog.gpo.gov/F/VLMK1UIV2U12TKRPIRXJ8G3V4TY3FKV9X86JDVDRTQ1JBLPGBS-01104?func=full-set-set&set_number=002829&set_entry=000001&format=999
        4. Return the fields from the table

        Args:
            document_id (int | str): The document ID.

        Returns:
            Optional[CGPMetadata]: The CGP metadata.
        """
        # get the index url to try first
        search_url = (
            f"https://catalog.gpo.gov/F?func=find-a&find_code=WUR&request={document_id}"
        )
        search_result_doc = self._get_html(search_url)
        metadata_url = None
        for row in search_result_doc.xpath(".//tr"):
            row_links = row.xpath(".//a")
            for link in row_links:
                href = link.get("href")
                if href and "func=full-set-set" in href:
                    metadata_url = href
                    break

        # return empty if no metadata url
        if not metadata_url:
            return None

        # get the metadata
        metadata = CGPMetadata()
        metadata_doc = self._get_html(metadata_url)
        current_heading: Optional[str] = None
        for row_number, row in enumerate(metadata_doc.xpath(".//table//tr")):
            # skip the first row
            if row_number == 0:
                continue

            # get the key and value
            try:
                # get the columns and start with the first one
                row_columns = row.xpath(".//td")
                if len(row_columns) == 0:
                    continue

                # get the heading from first value
                key = row_columns[0].text_content().strip()

                # check if we have a second value
                if len(row_columns) > 1:
                    # check if we have a key to update the current heading
                    if len(key) > 0:
                        current_heading = key.lower()

                    # parse the value
                    current_value = CGPMetadata.parse_cgp_values(
                        row_columns[1].text_content().strip()
                    )
                    if current_value is not None and current_heading is not None:
                        # add original headings to extra
                        if current_heading in metadata.extra:
                            if isinstance(metadata.extra[current_heading], list):
                                metadata.extra[current_heading].append(current_value)
                            else:
                                metadata.extra[current_heading] += f"\n{current_value}"
                        else:
                            metadata.extra[current_heading] = current_value

                        # handle specific fields
                        if current_heading == "title":
                            if metadata.title is None:
                                metadata.title = current_value
                        elif current_heading == "description":
                            if metadata.description is None:
                                metadata.description = current_value
                            else:
                                metadata.description += f"\n{current_value}"
                        elif current_heading == "author":
                            if metadata.author is None:
                                metadata.author = current_value
                            else:
                                metadata.author += f"\n{current_value}"
                        elif current_heading == "publisher":
                            if metadata.publisher is None:
                                metadata.publisher = current_value
                            else:
                                metadata.publisher += f"\n{current_value}"
                        elif current_heading.startswith("subject"):
                            metadata.subjects.append(current_value)
            except Exception as e:  # pylint: disable=broad-except
                LOGGER.warning("Error parsing metadata: %s", traceback.format_exc())
                raise e

        return metadata

    def download_permanent_fdlp_index(
        self, document_id: str, cgp_metadata: CGPMetadata
    ) -> bool:
        """
        Download the FDLP Electronic Collection Archive for the specified document ID via permanent link.

        Args:
            document_id (str): The document ID.
            cgp_metadata (CGPMetadata): The CGP metadata for the document.

        Returns:
            bool: Whether the download was successful.
        """
        # get the index url to try first
        url = f"https://permanent.fdlp.gov/{document_id}/"

        # get the index and look for links to download
        LOGGER.info("Downloaded index at %s", url)

        # parse the index
        links = []
        index_doc = self._get_html(url)
        for index_link in index_doc.xpath(".//table//a"):
            href = index_link.get("href").strip().strip("/")
            if href.startswith("?") or href in (None, ""):
                continue
            links.append(href)

        # download the links
        status = True
        for link in links:
            try:
                # check if it's a full url by parsing it
                parsed_link = urllib.parse.urlparse(link)
                if not parsed_link.scheme:
                    link = urllib.parse.urljoin(url, link)

                # get the response and content type
                document_response = self._get_response(link)
                content_type = "application/octet-stream"
                for key, value in document_response.headers.items():
                    if key.lower() == "content-type":
                        content_type = value
                        break

                # create a Document object from this information
                link_document = Document(
                    dataset_id=self.metadata.dataset_id,
                    id=f"{document_id}/{parsed_link.path}",
                    blake2b=hashlib.blake2b(document_response.content).hexdigest(),
                    content=document_response.content,
                    size=len(document_response.content),
                    title=cgp_metadata.title,
                    description=cgp_metadata.description,
                    identifier=link,
                    format=content_type,
                    creator=cgp_metadata.author,
                    publisher=cgp_metadata.publisher,
                    subject=cgp_metadata.subjects,
                    extra=cgp_metadata.extra.copy(),
                )
                link_document.to_s3()
            except Exception as e:  # pylint: disable=broad-except
                LOGGER.warning("Error downloading %s: %s", link, e)
                status = False

        return status

    def download_purl_link(self, document_id: str, cgp_metadata: CGPMetadata) -> bool:
        """
        Download the FDLP Electronic Collection Archive for the specified document ID via PURL link,
        which is typically a redirect to a single file like a remote URL or PDF.

        Args:
            document_id (str): The document ID.
            cgp_metadata (CGPMetadata): The CGP metadata for the document.

        Returns:
            bool: Whether the download was successful.
        """
        # get the index url to try first
        url = f"https://purl.fdlp.gov/GPO/{document_id}"

        try:
            # get the full response to track redirect and content type
            document_response = self._get_response(url)

            # create the document
            content_type = "application/octet-stream"
            for key, value in document_response.headers.items():
                if key.lower() == "content-type":
                    content_type = value
                    break

            # create a Document object from this information
            link_document = Document(
                dataset_id=self.metadata.dataset_id,
                id=f"{document_id}/{document_response.url.host}{document_response.url.path}",
                blake2b=hashlib.blake2b(document_response.content).hexdigest(),
                content=document_response.content,
                size=len(document_response.content),
                title=cgp_metadata.title,
                description=cgp_metadata.description,
                identifier=str(document_response.url),
                format=content_type,
                creator=cgp_metadata.author,
                publisher=cgp_metadata.publisher,
                subject=cgp_metadata.subjects,
                extra=cgp_metadata.extra.copy(),
            )
            link_document.to_s3()
            return True
        except Exception as e:  # pylint: disable=broad-except
            LOGGER.warning("Error downloading %s: %s", url, e)
            return False

    def download_id(
        self, document_id: int | str, **kwargs: dict[str, Any]
    ) -> SourceDownloadStatus:
        """
        Download the FDLP Electronic Collection Archive for the specified document ID.

        - First, try via permanent link at: https://permanent.fdlp.gov/{document_id}
        - If that fails, try via redirect at https://purl.fdlp.gov/GPO/{document_id}

        Args:
            document_id (int | str): The document ID.
            kwargs (dict): Additional keyword arguments.

        Returns:
            bool: Whether the download was successful.
        """
        try:
            # convert int to gpo id if not provided
            if isinstance(document_id, int):
                document_id = f"gpo{document_id}"

            # check if we have the prefix
            if self.check_id(document_id) and not self.update:
                LOGGER.info("Document %s already exists.", document_id)
                return SourceDownloadStatus.EXISTED

            # try to get the metadata
            metadata = self.get_cgp_metadata(document_id)

            if metadata in (None, {}):
                LOGGER.warning("No metadata found for %s", document_id)
                return SourceDownloadStatus.NOT_FOUND

            # get the index url to try first
            try:
                self.download_permanent_fdlp_index(document_id, metadata)  # type: ignore
                return SourceDownloadStatus.SUCCESS
            except httpx.HTTPStatusError as e:
                if "404" in str(e):
                    LOGGER.warning("Unable to download index for %s", document_id)
                else:
                    LOGGER.error("Error downloading index for %s: %s", document_id, e)

            # otherwise, try the purl redirector
            try:
                self.download_purl_link(document_id, metadata)  # type: ignore
                return SourceDownloadStatus.SUCCESS
            except httpx.HTTPStatusError as e:
                if "404" in str(e):
                    LOGGER.warning("Unable to download purl for %s", document_id)
                else:
                    LOGGER.error("Error downloading purl for %s: %s", document_id, e)

            # return false if we can't download through a known method
            return SourceDownloadStatus.NOT_FOUND
        except Exception as e:  # pylint: disable=broad-except
            LOGGER.error("Error downloading %s: %s", document_id, e)
            return SourceDownloadStatus.FAILURE

    def download_date(
        self, date: datetime.date, **kwargs: dict[str, Any]
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Download the FDLP Electronic Collection Archive for the specified date.

        Args:
            date (datetime.date): The date to download.
            kwargs (dict): Additional keyword arguments.

        Returns:
            bool: Whether the download was successful.
        """
        raise NotImplementedError(
            f"download_date method is not available for dataset_id={self.metadata.dataset_id}"
        )

    def download_date_range(
        self,
        start_date: datetime.date | datetime.datetime,
        end_date: datetime.date | datetime.datetime,
        **kwargs: dict[str, Any],
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Download the FDLP Electronic Collection Archive for the specified date range.

        Args:
            start_date (datetime.date | datetime.datetime): The start date.
            end_date (datetime.date | datetime.datetime): The end date.
            kwargs (dict): Additional keyword arguments.

        Returns:
            bool: Whether the download was successful.
        """
        raise NotImplementedError(
            f"download_date_range method is not available for dataset_id={self.metadata.dataset_id}"
        )

    def download_all(
        self, **kwargs: dict[str, Any]
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Download all data from the source based on the GPO and LPS ID ranges.

        Args:
            kwargs (dict): Additional keyword arguments.

        Returns:
            Generator[SourceProgressStatus, None, None]: The progress status
        """

        # create the ID list
        id_list = []
        for gpo_id in range(self.min_gpo_id, self.max_gpo_id):
            id_list.append(f"gpo{gpo_id}")
        for lps_id in range(self.min_lps_id, self.max_lps_id):
            id_list.append(f"lps{lps_id}")

        # download the IDs and track status
        current_progress = SourceProgressStatus(
            total=len(id_list), description="Downloading FDLP resources..."
        )
        for document_id in id_list:
            current_progress.extra = {"document_id": document_id}
            try:
                # download the document
                status = self.download_id(document_id)
                if status in (
                    SourceDownloadStatus.NOT_FOUND,
                    SourceDownloadStatus.FAILURE,
                    SourceDownloadStatus.PARTIAL,
                ):
                    current_progress.failure += 1
                elif status in (
                    SourceDownloadStatus.EXISTED,
                    SourceDownloadStatus.SUCCESS,
                ):
                    current_progress.success += 1

                # sleep if new download
                if status in (
                    SourceDownloadStatus.SUCCESS,
                    SourceDownloadStatus.PARTIAL,
                ):
                    time.sleep(self.delay)
            except Exception as e:  # pylint: disable=broad-except
                LOGGER.warning("Error downloading %s: %s", document_id, e)
                current_progress.message = str(e)
                current_progress.failure += 1
                current_progress.status = False
            finally:
                # update the prog bar
                current_progress.current += 1
                yield current_progress
                current_progress.message = None

        # finalize status
        current_progress.done = True
        yield current_progress
