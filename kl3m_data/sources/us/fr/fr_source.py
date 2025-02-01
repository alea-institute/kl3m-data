"""
Federal Register source
"""

# imports
import datetime
import hashlib
from pathlib import Path
from typing import Any, Generator

# packages
import httpx
import lxml.etree


# project
from kl3m_data.logger import LOGGER
from kl3m_data.sources.base_document import Document
from kl3m_data.sources.base_source import (
    BaseSource,
    SourceMetadata,
    SourceDownloadStatus,
    SourceProgressStatus,
)
from kl3m_data.sources.us.fr.fr_types import FRAgency, FRDocument
from kl3m_data.utils.httpx_utils import (
    get_httpx_limits,
    get_httpx_timeout,
    get_default_headers,
)

# get XLS transform path relative to here
STYLESHEET_PATH = Path(__file__).parent / "fedregister.xsl"

# constants
FR_MIN_DATE = datetime.date(1995, 1, 1)
FR_MAX_DATE = datetime.date.today()
FR_BASE_URL = "https://www.federalregister.gov/api/v1"
FR_FIELD_SET = (
    "abstract",
    "action",
    "agencies",
    "agency_names",
    "body_html_url",
    "cfr_references",
    "citation",
    "comment_url",
    "comments_close_on",
    "correction_of",
    "corrections",
    "dates",
    "disposition_notes",
    "docket_id",
    "docket_ids",
    "dockets",
    "document_number",
    "effective_on",
    "end_page",
    "excerpts",
    "executive_order_notes",
    "executive_order_number",
    "explanation",
    "full_text_xml_url",
    "html_url",
    "images",
    "images_metadata",
    "json_url",
    "mods_url",
    "not_received_for_publication",
    "page_length",
    "page_views",
    "pdf_url",
    "president",
    "presidential_document_number",
    "proclamation_number",
    "public_inspection_pdf_url",
    "publication_date",
    "raw_text_url",
    "regulation_id_number_info",
    "regulation_id_numbers",
    "regulations_dot_gov_info",
    "regulations_dot_gov_url",
    "significant",
    "signing_date",
    "start_page",
    "subtype",
    "title",
    "toc_doc",
    "toc_subject",
    "topics",
    "type",
    "volume",
)


class FRSource(BaseSource):
    """
    Federal Register source
    """

    def __init__(self, **kwargs):
        """
        Initialize the source.

        Args:
            min_date (datetime.date): The minimum date.
            max_date (datetime.date): The maximum date.
            page_size (int): The page size.
            base_url (str): The base URL.
            update (bool): Whether to update the source.
            delay (int): Delay between requests
        """
        # set the metadata
        metadata = SourceMetadata(
            dataset_id="fr",
            dataset_home="https://www.federalregister.gov/",
            dataset_description="A web version of the Federal Register (FR) jointly administered by the National Archives and Records Administration (NARA) and the Government Publishing Office (GPO).",
            dataset_license="Not subject to copyright under 17 U.S.C. 105.",
        )

        # call the super
        super().__init__(metadata)

        # redefine the client with longer timeouts;
        # we need this to allow the API to respond for large docs like titles 12/26/42
        # TODO: decide if we want to push this to namespaced config like before
        self.client = httpx.Client(
            http1=True,
            http2=True,
            verify=False,
            follow_redirects=True,
            limits=get_httpx_limits(),
            timeout=get_httpx_timeout(read_timeout=60 * 5),
            headers=get_default_headers(),
        )

        # set the kwargs
        self.base_url = kwargs.get("base_url", FR_BASE_URL)

        # set the min and max dates
        self.min_date = kwargs.get("min_date", FR_MIN_DATE)
        self.max_date = kwargs.get("max_date", FR_MAX_DATE)
        self.page_size = kwargs.get("page_size", 1000)

        # initialize the transformer
        self.stylesheet_transformer = lxml.etree.XSLT(
            lxml.etree.fromstring(STYLESHEET_PATH.read_bytes())
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Close the source.
        """
        # clean up the transformer
        del self.stylesheet_transformer

    def get_url(self, path: str) -> str:
        """
        Get the URL for the path.

        Args:
            path (str): The path.

        Returns:
            str: The URL.
        """
        return f"{self.base_url}/{path.strip('/')}"

    def get_agencies(self) -> list[FRAgency]:
        """
        Get a list of agencies.

        Returns:
            list[dict]: The list of agencies.
        """
        return [
            FRAgency(**agency)
            for agency in self._get_json_list(self.get_url("/agencies"))
        ]

    def get_documents_by_date(
        self,
        publication_date: datetime.date,
        **kwargs: dict[str, Any],
    ) -> Generator[tuple[FRDocument, int], None, None]:
        """
        Get documents by publication date.

        https://www.federalregister.gov/api/v1/documents.json?fields[]=abstract&fields[]=action&fields[]=agencies&fields[]=agency_names&fields[]=body_html_url&fields[]=cfr_references&fields[]=citation&fields[]=comment_url&fields[]=comments_close_on&fields[]=correction_of&fields[]=corrections&fields[]=dates&fields[]=disposition_notes&fields[]=docket_id&fields[]=docket_ids&fields[]=dockets&fields[]=document_number&fields[]=effective_on&fields[]=end_page&fields[]=excerpts&fields[]=executive_order_notes&fields[]=executive_order_number&fields[]=explanation&fields[]=full_text_xml_url&fields[]=html_url&fields[]=images&fields[]=images_metadata&fields[]=json_url&fields[]=mods_url&fields[]=not_received_for_publication&fields[]=page_length&fields[]=page_views&fields[]=pdf_url&fields[]=president&fields[]=presidential_document_number&fields[]=proclamation_number&fields[]=public_inspection_pdf_url&fields[]=publication_date&fields[]=raw_text_url&fields[]=regulation_id_number_info&fields[]=regulation_id_numbers&fields[]=regulations_dot_gov_info&fields[]=regulations_dot_gov_url&fields[]=significant&fields[]=signing_date&fields[]=start_page&fields[]=subtype&fields[]=title&fields[]=toc_doc&fields[]=toc_subject&fields[]=topics&fields[]=type&fields[]=volume&per_page=10&conditions[publication_date][is]=2020-06-01

        Args:
            publication_date (datetime.date): The publication date.
            fields (list[str]): The fields to include.

        Returns:
            Generator[tuple[FRDocument, int], None, None]: The document and the total count.
        """
        # set the fields
        fields = kwargs.get("fields", FR_FIELD_SET)

        # format the field param string
        field_param_string = "&".join(f"fields[]={field}" for field in fields)

        # format the date string
        date_str = publication_date.strftime("%Y-%m-%d")
        date_param_string = f"conditions[publication_date][is]={date_str}"

        # iterate until there isn't a next page
        next_page_url = self.get_url(
            f"/documents.json?per_page={self.page_size}&{field_param_string}&{date_param_string}"
        )
        while next_page_url:
            search_data = self._get_json(next_page_url)

            for document in search_data.get("results", []):
                yield FRDocument(**document), search_data.get("count", 0)

            next_page_url = search_data.get("next_page_url", None)

    # pylint: disable=too-many-branches, too-many-statements
    def download_id(
        self, document_id: int | str, **kwargs: dict[str, Any]
    ) -> SourceDownloadStatus:
        """
        Download the document id, where the doc ID is the FR document number, e.g., 2021-11441.

        Args:
            document_id (int | str): The document ID.

        Returns:
            SourceDownloadStatus: The download status.
        """
        # check if it already exists
        if self.check_id(str(document_id) + "/"):
            LOGGER.info("Document %s already exists", document_id)
            return SourceDownloadStatus.SUCCESS

        # check for fields in kwargs
        fields = kwargs.get("fields", FR_FIELD_SET)

        # set the fields
        if fields is None:
            fields = FR_FIELD_SET

        # format the field param string
        field_param_string = "&".join(f"fields[]={field}" for field in fields)

        # get the doc url
        doc_url = self.get_url(f"/documents/{document_id}.json?{field_param_string}")

        # get the doc data
        doc_data = self._get_json(doc_url)

        # store any document formats available
        doc_versions = []

        # get xml content
        if doc_data.get("full_text_xml_url", None) is not None:
            LOGGER.info(
                "Downloading XML content from %s", doc_data["full_text_xml_url"]
            )
            try:
                xml_content = self._get(doc_data["full_text_xml_url"])
                doc_versions.append(
                    (
                        {
                            "extension": "xml",
                            "content": xml_content,
                            "size": len(xml_content),
                            "blake2b": hashlib.blake2b(xml_content).hexdigest(),
                            "format": "text/xml",
                            "identifier": doc_data["full_text_xml_url"],
                        }
                    )
                )
            except Exception as e:
                LOGGER.error(
                    "Error downloading XML content from %s: %s",
                    doc_data["full_text_xml_url"],
                    str(e),
                )
                raise e

        # get the text content
        if doc_data.get("raw_text_url", None) is not None:
            LOGGER.info("Downloading text content from %s", doc_data["raw_text_url"])
            try:
                text_content = self._get(doc_data["raw_text_url"])
                doc_versions.append(
                    (
                        {
                            "extension": "txt",
                            "content": text_content,
                            "size": len(text_content),
                            "blake2b": hashlib.blake2b(text_content).hexdigest(),
                            "format": "text/plain",
                            "identifier": doc_data["raw_text_url"],
                        }
                    )
                )
            except Exception as e:  # pylint: disable=broad-except
                LOGGER.error(
                    "Error downloading text content from %s: %s",
                    doc_data["raw_text_url"],
                    str(e),
                )

        # get the pdf
        if doc_data.get("pdf_url", None) is not None:
            LOGGER.info("Downloading PDF content from %s", doc_data["pdf_url"])
            try:
                pdf_content = self._get(doc_data["pdf_url"])
                doc_versions.append(
                    (
                        {
                            "extension": "pdf",
                            "content": pdf_content,
                            "size": len(pdf_content),
                            "blake2b": hashlib.blake2b(pdf_content).hexdigest(),
                            "format": "application/pdf",
                            "identifier": doc_data["pdf_url"],
                        }
                    )
                )
            except Exception as e:  # pylint: disable=broad-except
                LOGGER.error(
                    "Error downloading PDF content from %s: %s",
                    doc_data["pdf_url"],
                    str(e),
                )

        # get the html
        if doc_data.get("body_html_url", None) is not None:
            LOGGER.info("Downloading HTML content from %s", doc_data["body_html_url"])
            try:
                html_content = self._get(doc_data["body_html_url"])
                doc_versions.append(
                    (
                        {
                            "extension": "html",
                            "content": html_content,
                            "size": len(html_content),
                            "blake2b": hashlib.blake2b(html_content).hexdigest(),
                            "format": "text/html",
                            "identifier": doc_data["html_url"],
                        }
                    )
                )
            except Exception as e:  # pylint: disable=broad-except
                LOGGER.error(
                    "Error downloading HTML content from %s: %s",
                    doc_data["html_url"],
                    str(e),
                )

        # normalize the creator list from agency raw names
        creator_list = []
        for agency in doc_data.get("agencies", []):
            creator_list.append(agency.get("name", ""))

        # normalize subjects if we have other types
        subjects = ["Federal Register"]
        subjects.extend(doc_data.get("topics", []))
        if "type" in doc_data:
            subjects.append(doc_data["type"])
        if "subtype" in doc_data:
            subjects.append(doc_data["subtype"])

        # upload all the versions
        any_fail = False
        any_success = False
        for version in doc_versions:
            try:
                upload_doc = Document(
                    dataset_id=self.metadata.dataset_id,
                    id=f"{doc_data['document_number']}/{version['extension']}",
                    identifier=version["identifier"],
                    content=version["content"],
                    format=version["format"],
                    size=version["size"],
                    blake2b=version["blake2b"],
                    extra=doc_data.copy(),
                    title=doc_data["title"],
                    description=doc_data.get("abstract", ""),
                    date=datetime.datetime.fromisoformat(doc_data["publication_date"]),
                    source=self.metadata.dataset_home,
                    publisher="U.S. Government Publishing Office",
                    creator=creator_list,
                    language="en",
                    subject=subjects,
                    bibliographic_citation=f"{doc_data['citation']}.  {doc_data['publication_date']}",
                )
                upload_doc.to_s3()
                any_success = True
            except Exception as e:  # pylint: disable=broad-except
                LOGGER.error(
                    "Error uploading document %s: %s",
                    doc_data["document_number"],
                    str(e),
                )
                any_fail = True

        # return the status
        if any_fail and any_success:
            return SourceDownloadStatus.PARTIAL

        if any_success:
            return SourceDownloadStatus.SUCCESS

        return SourceDownloadStatus.FAILURE

    def download_date(
        self, date: datetime.date, **kwargs: dict[str, Any]
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Download the documents for the date using `get_documents_by_date` and `download_id`.

        Args:
            date (datetime.date): The date.

        Returns:
            Generator[SourceProgressStatus, None, None]: The progress status.
        """
        current_progress = SourceProgressStatus(
            total=None, description="Downloading FR documents", current=0
        )

        # get the documents
        for document, count in self.get_documents_by_date(date):
            # update count if not set yet
            if current_progress.total is None:
                current_progress.total = count

            current_progress.extra = {
                "date": date.isoformat(),
                "document_number": document.document_number,
            }

            # download each document number
            try:
                status = self.download_id(document.document_number)
                if status in (
                    SourceDownloadStatus.SUCCESS,
                    SourceDownloadStatus.PARTIAL,
                ):
                    current_progress.success += 1
                else:
                    current_progress.failure += 1
                    current_progress.status = False
            except Exception as e:  # pylint: disable=broad-except
                LOGGER.error(
                    "Error downloading document %s: %s",
                    document.document_number,
                    str(e),
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

    def download_date_range(
        self,
        start_date: datetime.date | datetime.datetime,
        end_date: datetime.date | datetime.datetime,
        **kwargs: dict[str, Any],
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Download the documents for the date range.

        Args:
            start_date (datetime.date | datetime.datetime): The start date.
            end_date (datetime.date | datetime.datetime): The end date.

        Returns:
            Generator[SourceProgressStatus, None, None]: The progress status.
        """
        # iterate over the dates
        current_date = start_date
        while current_date <= end_date:
            yield from self.download_date(current_date)
            current_date += datetime.timedelta(days=1)

    def download_all(
        self, **kwargs: dict[str, Any]
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Download all documents.

        Returns:
            Generator[SourceProgressStatus, None, None]: The progress status.
        """
        # iterate over the dates
        current_date = self.min_date
        while current_date <= self.max_date:
            try:
                yield from self.download_date(current_date)
                current_date += datetime.timedelta(days=1)
            except Exception as e:  # pylint: disable=broad-except
                LOGGER.error("Error downloading date %s: %s", current_date, str(e))
                current_date += datetime.timedelta(days=1)
                continue
