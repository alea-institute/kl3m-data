"""
This module contains the CurrentUSCSource class.

References:
    - Example Release Point page: https://uscode.house.gov/download/releasepoints/us/pl/118/66/usc-rp@118-66.htm
"""

# imports
import io
import datetime
import zipfile
from typing import Any, Generator

# packages
import lxml.html

# project
from kl3m_data.logger import LOGGER
from kl3m_data.sources.base_source import (
    BaseSource,
    SourceMetadata,
    SourceDownloadStatus,
    SourceProgressStatus,
)
from kl3m_data.sources.us.usc.usc_types import (
    USCReleaseFile,
    USCDocument,
    MAX_TITLES,
    BASE_DOMAIN,
)

DEFAULT_RELEASE_CONGRESS = 118
DEFAULT_RELEASE_PL_NUMBER = 66


class USCSource(BaseSource):
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
            dataset_id="usc",
            dataset_home="https://uscode.house.gov/",
            dataset_description="The most current version of the United States Code as prepared by the Office of the Law Revision Counsel of the U.S. House of Representatives (OLRC).",
            dataset_license="Not subject to copyright under 17 U.S.C. 105.",
        )

        # call the super
        super().__init__(metadata)

        # set release congress and public law number
        self.release_congress = kwargs.get("release_congress", DEFAULT_RELEASE_CONGRESS)
        self.release_pl_number = kwargs.get(
            "release_pl_number", DEFAULT_RELEASE_PL_NUMBER
        )

    @staticmethod
    def parse_doc_metadata(
        doc_element: lxml.html.HtmlElement, congress: int, public_law: int, title: int
    ) -> USCDocument:
        """
        Extract all metadata from doc comments like:

        Args:
            doc_element (lxml.html.HtmlElement): The document element.
            congress (int): The congress number.
            public_law (int): The public law number.
            title (int): The title number.

        Returns:
            dict[str, Any]: The metadata dictionary.
        """
        metadata = {}
        for comment in doc_element.xpath("//comment()"):
            comment_text = str(comment).strip()
            if comment_text.startswith("<!-- documentid:"):
                # parse all tokens that might occur in the comment
                tokens = comment_text[4:-3].split()
                for token in tokens:
                    if ":" in token:
                        key, value = token.split(":")
                        metadata[key] = value
            elif ":" in comment_text:
                key, value = comment_text[4:-3].split(":", 1)
                if key.strip() not in ("field-start", "field-end"):
                    metadata[key.strip()] = value.strip()

        try:
            document_pdf_page = int(metadata.get("documentPDFPage", -1))
        except ValueError:
            document_pdf_page = None

        return USCDocument(
            congress=congress,
            public_law=public_law,
            title=title,
            documentid=metadata.get("documentid"),  # type: ignore
            itempath=metadata.get("itempath"),  # type: ignore
            itemsortkey=metadata.get("itemsortkey"),  # type: ignore
            expcite=metadata.get("expcite"),  # type: ignore
            currentthrough=metadata.get("currentthrough"),  # type: ignore
            document_pdf_page=document_pdf_page,
            usckey=metadata.get("usckey", None),
            xhtml=lxml.html.tostring(doc_element, encoding="utf-8"),
            doc=doc_element,
        )

    @staticmethod
    def get_release_base_url(congress: int, public_law: int) -> str:
        """
        Get the release URL for the current United States Code.

        Args:
            congress (int): The congress number.
            public_law (int): The public law number.

        Returns:
            str: The release URL.
        """
        return f"{BASE_DOMAIN}download/releasepoints/us/pl/{congress}/{public_law}/"

    @staticmethod
    def get_release_title_url(congress: int, public_law: int, title: int) -> str:
        """
        Get the release URL for a title in the current United States Code.

        Args:
            congress (int): The congress number.
            public_law (int): The public law number.
            title (int): The title number.

        Returns:
            str: The release URL.
        """
        return (
            USCSource.get_release_base_url(congress, public_law)
            + f"htm_usc{title:02d}@{congress}-{public_law}.zip"
        )

    def iter_release_title(
        self, congress: int, public_law: int, title: int
    ) -> Generator[USCReleaseFile, None, None]:
        """
        Parse the release title for the current United States Code.

        Args:
            title (int): The title number.
            congress (int): The congress number.
            public_law (int): The public law number.

        Returns:
            Generator[USCReleaseFile, None, None]: The release file generator.
        """
        # get the zip url
        zip_url = self.get_release_title_url(congress, public_law, title)

        # download the zip file
        zip_response = self._get_response(zip_url)

        # yield members with metadata for release files
        with zipfile.ZipFile(io.BytesIO(zip_response.content)) as zip_file:
            for filename in zip_file.namelist():
                with zip_file.open(filename, mode="r") as zip_member_file:
                    yield USCReleaseFile(
                        title=title,
                        congress=congress,
                        public_law=public_law,
                        filename=filename,
                        contents=zip_member_file.read(),
                    )

    # pylint: disable=too-many-nested-blocks
    def download_release_title_documents(
        self,
        congress: int,
        public_law: int,
        title: int,
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Parse the release title for the current United States Code and return
        documents as xml docs.

        Args:
            title (int): The title number.
            congress (int): The congress number.
            public_law (int): The public law number.

        Returns:
            Generator[SourceProgressStatus, None, None]: The release file generator.
        """
        # set up progress
        current_progress = SourceProgressStatus(
            total=None, description="Downloading OLRC USC documents", current=0
        )

        try:
            for release_file in self.iter_release_title(congress, public_law, title):
                # track progress
                doc_start_pos = 0
                while True:
                    try:
                        # find the next document
                        doc_start_pos = release_file.contents.find(
                            b"<!-- documentid:", doc_start_pos
                        )
                        if doc_start_pos == -1:
                            break
                        doc_end_pos = release_file.contents.find(
                            b"<!-- documentid:", doc_start_pos + 1
                        )

                        # get the document
                        if doc_end_pos == -1:
                            doc_fragment = release_file.contents[doc_start_pos:].strip()
                        else:
                            doc_fragment = release_file.contents[
                                doc_start_pos:doc_end_pos
                            ].strip()

                        # update the start position
                        doc_start_pos = doc_end_pos

                        # yield the parsed document
                        try:
                            # get and check if it's uploaded
                            doc_element = lxml.html.fromstring(doc_fragment)
                            usc_doc = self.parse_doc_metadata(
                                doc_element, congress, public_law, title
                            )
                            doc = usc_doc.to_document(self.metadata.dataset_id)
                            current_progress.extra = {
                                "congress": congress,
                                "public_law": public_law,
                                "title": title,
                                "document": doc.id,
                            }
                            if self.check_id(doc.id):
                                LOGGER.info("Document already uploaded: %s", doc.id)
                            else:
                                doc.to_s3()

                            # inc and yield
                            current_progress.success += 1
                        except Exception as e:  # pylint: disable=broad-except
                            LOGGER.error("Error parsing document: %s", str(e))
                            current_progress.message = str(e)
                            current_progress.failure += 1
                            current_progress.status = False
                    except Exception as e:  # pylint: disable=broad-except
                        LOGGER.error("Error processing document: %s", str(e))
                        current_progress.message = str(e)
                        current_progress.failure += 1
                        current_progress.status = False
                    finally:
                        current_progress.current += 1
                        yield current_progress
                        current_progress.message = None

        except Exception as e:  # pylint: disable=broad-except
            LOGGER.error("Error downloading OLRC USC documents: %s", str(e))
            current_progress.message = str(e)
            current_progress.failure += 1
            current_progress.status = False

        # finalize
        current_progress.done = True
        yield current_progress

    def download_id(
        self, document_id: int | str, **kwargs: dict[str, Any]
    ) -> SourceDownloadStatus:
        raise NotImplementedError(
            "Document ID downloads are not available for OLRC USC sources."
        )

    def download_date(
        self, date: datetime.date, **kwargs: dict[str, Any]
    ) -> Generator[SourceProgressStatus, None, None]:
        raise NotImplementedError(
            "Date range downloads are not available for OLRC USC sources."
        )

    def download_date_range(
        self,
        start_date: datetime.date | datetime.datetime,
        end_date: datetime.date | datetime.datetime,
        **kwargs: dict[str, Any],
    ) -> Generator[SourceProgressStatus, None, None]:
        raise NotImplementedError(
            "Date range downloads are not available for OLRC USC sources."
        )

    def download_all(
        self, **kwargs: dict[str, Any]
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Do all titles and yield along the way.

        Args:
            kwargs (dict[str, Any]): Additional arguments.

        Returns:
            Generator[SourceProgressStatus, None, None]: The progress generator.
        """
        # get the release congress and public law number
        congress = kwargs.get("release_congress", self.release_congress)
        public_law = kwargs.get("release_pl_number", self.release_pl_number)

        # download all titles
        for title in range(1, MAX_TITLES + 1):
            yield from self.download_release_title_documents(
                congress, public_law, title
            )
