"""
EDGAR source
"""

# imports
import datetime
import hashlib
import io
import mimetypes
import re
import tarfile
from pathlib import Path
from typing import Any, Generator, Literal

# packages

from kl3m_data.logger import LOGGER
from kl3m_data.sources.base_document import Document

# project
from kl3m_data.sources.base_source import (
    BaseSource,
    SourceMetadata,
    SourceDownloadStatus,
    SourceProgressStatus,
)
from kl3m_data.utils.uu_utils import uudecode

# constants
EDGAR_BASE_URL = "https://www.sec.gov/Archives/edgar/"
EDGAR_MIN_DATE = datetime.date(1995, 1, 1)
EDGAR_MAX_DATE = datetime.date.today()

# EDGAR requires a specific user agent format
# Sample Company Name AdminContact@<sample company domain>.com
EDGAR_USER_AGENT = "ALEA Institute hello@aleainstitute.ai"


# module regex patterns
RE_SUBMISSION_TAG = re.compile("<SUBMISSION>.*?</SUBMISSION>", re.DOTALL)
RE_DOC_TAG = re.compile("<DOCUMENT>.*?</DOCUMENT>", re.DOTALL)
RE_CONTENT_TAG = re.compile("<TEXT>(.*?)</TEXT>", re.DOTALL)
RE_META_TAG = re.compile("<(DESCRIPTION|FILENAME|SEQUENCE|TYPE)>(.*?)\n", re.DOTALL)


class EDGARSource(BaseSource):
    """
    EDGAR source
    """

    def __init__(self, **kwargs: dict[str, Any]):
        """
        Initialize the source.

        Args:
            min_date (datetime.date): Minimum date for the source.
            max_date (datetime.date): Maximum date for the source.
            base_url (str): Base URL for the source.
            update (bool): Whether to update the source.
            delay (int): Delay between requests
        """
        # set the metadata
        metadata = SourceMetadata(
            dataset_id="edgar",
            dataset_home="https://www.sec.gov/",
            dataset_description="Filings submitted to the US SEC's EDGAR under the Securities Act of 1933, the Securities Exchange Act of 1934, the Trust Indenture Act of 1939, and the Investment Company Act of 1940.",
            dataset_license="Generally accepted to available for free use and distribution under Sections 19 and 20 of the Securities Act of 1933, Section 21 of the Securities Exchange Act of 1934, Section 321 of the Trust Indenture Act of 1939, Section 42 of the Investment Company Act of 1940, Section 209 of the Investment Advisers Act of 1940 and Title 17 of the Code of Federal Regulations, Section 202.5. See ISDA v. Socratek for only countervailing guidance.",
        )

        # call the super
        super().__init__(metadata)

        # patch the user agent here because EDGAR requires a specific format
        self.client.headers["User-Agent"] = EDGAR_USER_AGENT
        self.async_client.headers["User-Agent"] = EDGAR_USER_AGENT

        # set the base URL
        self.base_url = kwargs.get("base_url", EDGAR_BASE_URL)

        # set the min and max date
        self.min_date: datetime.date = EDGAR_MIN_DATE
        if "min_date" in kwargs:
            min_date_arg = kwargs["min_date"]
            if isinstance(min_date_arg, str):
                self.min_date = datetime.datetime.strptime(
                    min_date_arg, "%Y-%m-%d"
                ).date()
            elif isinstance(min_date_arg, (datetime.date, datetime.datetime)):
                self.min_date = min_date_arg
            else:
                raise ValueError("Invalid min_date format")

        self.max_date: datetime.date = EDGAR_MAX_DATE
        if "max_date" in kwargs:
            max_date_arg = kwargs["max_date"]
            if isinstance(max_date_arg, str):
                self.max_date = datetime.datetime.strptime(
                    max_date_arg, "%Y-%m-%d"
                ).date()
            elif isinstance(max_date_arg, (datetime.date, datetime.datetime)):
                self.max_date = max_date_arg
            else:
                raise ValueError("Invalid max_date format")

        self.update = kwargs.get("update", False)
        self.delay = kwargs.get("delay", 0)

    @staticmethod
    def decode_buffer(buffer: bytes) -> str:
        """
        Decode a buffer.

        Args:
            buffer (bytes): The buffer.

        Returns:
            str: The decoded buffer.
        """
        try:
            # try latin1, iso-8859-1, then utf-8
            return buffer.decode("latin1")
        except UnicodeDecodeError:
            try:
                return buffer.decode("iso-8859-1")
            except UnicodeDecodeError:
                try:
                    return buffer.decode("utf-8")
                except UnicodeDecodeError:
                    LOGGER.error("Error decoding buffer")
                    return ""

    def get_url(self, path: str) -> str:
        """
        Get a path relative to the base url.

        Args:
            path (str): The path to get.

        Returns:
            str: The full URL.
        """
        return f"{self.base_url}{path}"

    def get_year_quarter_index_url(
        self,
        year: int,
        quarter: int,
        index_type: Literal["company", "form", "master", "xbrl"] = "form",
    ) -> str:
        """
        Get the URL for a year and quarter index.

        Args:
            year (int): The year.
            quarter (int): The quarter.
            index_type (Literal["company", "form", "master", "xbrl"]): The index type.

        Returns:
            str: The URL.
        """
        # get the path
        path = f"full-index/{year}/QTR{quarter}/{index_type}.gz"
        return self.get_url(path)

    def get_feed_url(self, date: datetime.date) -> str:
        """
        Get the URL for a date feed.

        Args:
            date (datetime.date): The date.

        Returns:
            str: The URL.
        """
        # get year, quarter, and YYYYMMDD
        year = date.year
        quarter = (date.month - 1) // 3 + 1
        date_string = date.strftime("%Y%m%d")

        return self.get_url(f"Feed/{year}/QTR{quarter}/{date_string}.nc.tar.gz")

    @staticmethod
    def parse_nc_metadata(text: str) -> dict[str, Any]:
        """
        Parse NC metadata.

        Args:
            text (str): The buffer.

        Returns:
            dict[str, Any]: The metadata.
        """
        # track metadata and stack position in tags
        metadata: dict[str, Any] = {}
        tag_stack: list[str] = []

        # iterate through lines efficiently given buffer sizes
        first_doc_pos = text.find("<DOCUMENT>")
        if first_doc_pos == -1:
            return metadata

        # get header only
        header = text[:first_doc_pos]
        for line in header.splitlines():
            # update tag prefix
            tag_prefix = "/".join(tag_stack)

            # normalize and skip empty
            line = line.strip()
            if not line:
                continue

            # check tag type between </, <..., and < ... >
            if line.startswith("</") and line[-1] == ">":
                if len(tag_stack) > 0:
                    tag_stack.pop()
            elif line.startswith("<"):
                first_end_tag_pos = line.find(">")
                tag_name = tag_prefix + "/" + line[1:first_end_tag_pos]
                if line[-1] == ">":
                    # if tag closes on this line, then push to stack
                    tag = line[1:-1]
                    if tag.lower() not in ("submission",):
                        tag_stack.append(tag)
                else:
                    # otherwise, the line has a value
                    tag_value = line[first_end_tag_pos + 1 :]
                    if tag_name in metadata:
                        metadata[tag_name].append(tag_value)
                    else:
                        metadata[tag_name] = [tag_value]

        return metadata

    @staticmethod
    def parse_doc_metadata(text: str) -> dict[str, Any]:
        """
        Parse document metadata.

        Args:
            text (str): The buffer.

        Returns:
            dict[str, Any]: The metadata.
        """
        # track metadata and position
        metadata = {}
        last_tag = None

        # efficiently find the header items within the first 4k bytes
        for line in text[:4096].splitlines()[1:]:
            # normalize and skip empty
            line = line.strip()
            if not line:
                continue

            if line.startswith("<TEXT") or line.startswith("<PDF"):
                break

            if line.startswith("<TYPE>"):
                last_tag = "type"
                metadata["type"] = line[len("<TYPE>") :].strip()
            elif line.startswith("<SEQUENCE>"):
                last_tag = "sequence"
                metadata["sequence"] = line[len("<SEQUENCE>") :].strip()
            elif line.startswith("<FILENAME>"):
                last_tag = "filename"
                metadata["filename"] = line[len("<FILENAME>") :].strip()
            elif line.startswith("<DESCRIPTION>"):
                last_tag = "description"
                metadata["description"] = line[len("<DESCRIPTION>") :].strip()
            else:
                # add to last tag if set
                if last_tag and line[0] != "<":
                    metadata[last_tag] += " " + line
                else:
                    LOGGER.warning(
                        "Unmatched line in document header parsing: %s", line
                    )

        return metadata

    # pylint: disable=too-many-branches,too-many-statements
    def parse_doc_buffer(
        self,
        doc_content: bytes,
        submission_metadata: dict[str, Any],
        doc_metadata: dict[str, Any],
        filename: str,
    ) -> SourceDownloadStatus:
        """
        Parse a document buffer into a Document object.

        Returns:
            SourceDownloadStatus: The download status.
        """
        # parse key id fields
        try:
            if len(submission_metadata.get("/ACCESSION-NUMBER", [])) > 0:
                accession_number = submission_metadata["/ACCESSION-NUMBER"][0]
            else:
                LOGGER.info(
                    "Accession number is none for %s: %s", filename, submission_metadata
                )
                return SourceDownloadStatus.FAILURE

            cik = accession_number.split("-")[0]
        except IndexError:
            LOGGER.info(
                "Invalid accession number for %s: %s", filename, submission_metadata
            )
            return SourceDownloadStatus.FAILURE

        try:
            doc_sequence = int(doc_metadata["sequence"])
        except (KeyError, ValueError):
            LOGGER.info("Invalid sequence number for %s: %s", filename, doc_metadata)
            return SourceDownloadStatus.FAILURE

        try:
            form_type = doc_metadata.get("type")
        except Exception as e:  # pylint: disable=broad-except
            LOGGER.error("Error parsing form type: %s", str(e))
            return SourceDownloadStatus.FAILURE

        # handle other base fields
        if len(submission_metadata.get("/TYPE", [])) > 0:
            submission_type = submission_metadata["/TYPE"][0]
        else:
            submission_type = None

        # handle form type and description
        doc_description = doc_metadata.get("description", None)

        if "filename" in doc_metadata:
            doc_filename = doc_metadata["filename"]
            mime_type_info = mimetypes.guess_type(doc_filename)
            mime_type = mime_type_info[0]
        else:
            doc_filename = None
            mime_type = "application/octet-stream"

        # handle source name
        filer_name = submission_metadata.get(
            "FILER/COMPANY-DATA/CONFORMED-NAME", [None]
        )[0]
        issuer_name = submission_metadata.get(
            "ISSUER/COMPANY-DATA/CONFORMED-NAME", [None]
        )[0]
        subject_name = submission_metadata.get(
            "SUBJECT-COMPANY/COMPANY-DATA/CONFORMED-NAME", [None]
        )[0]
        reporting_owner_name = submission_metadata.get(
            "REPORTING-OWNER/COMPANY-DATA/CONFORMED-NAME", [None]
        )[0]

        # get the source name
        if filer_name:
            source_name = filer_name
        elif issuer_name:
            source_name = issuer_name
        elif subject_name:
            source_name = subject_name
        elif reporting_owner_name:
            source_name = reporting_owner_name
        else:
            source_name = None

        # get the identifier url by:
        # - left-stripping zeros from the cik
        # - removing hyphens from the accession number
        # - appending the file name
        # except that if there was no filename, then it's just accession number.txt
        if doc_filename is not None:
            doc_url = (
                f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip('0')}"
                f"/{accession_number.replace('-', '')}/{doc_filename}"
            )
        else:
            doc_url = (
                f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip('0')}"
                f"/{accession_number.replace('-', '')}/{accession_number}.txt"
            )

        # build valid subjects list
        subjects: list[str] = []
        if submission_type is not None:
            subjects.append(submission_type)
        if form_type is not None:
            subjects.append(form_type)
        subjects = sorted(list(set(subjects)))

        # handle dates
        try:
            # get date from YYYYMMDD input string
            submission_date = datetime.datetime.strptime(
                submission_metadata["/FILING-DATE"][0], "%Y%m%d"
            )
        except Exception as e:  # pylint: disable=broad-except
            LOGGER.warning("Error parsing submission date: %s", str(e))
            submission_date = None

        # create the doc
        document = Document(
            dataset_id=self.metadata.dataset_id,
            id=f"{cik}/{accession_number}/{doc_sequence}",
            identifier=doc_url,
            content=doc_content,
            size=len(doc_content),
            blake2b=hashlib.blake2b(doc_content).hexdigest(),
            format=mime_type,
            title=doc_description,
            source="https://www.sec.gov/",
            creator=[source_name] if source_name else None,
            publisher="U.S. Securities and Exchange Commission",
            date=submission_date,
            subject=subjects,
            bibliographic_citation=accession_number,
            extra={
                **submission_metadata,
                **doc_metadata,
            },
        )

        # upload to s3
        document.to_s3()

        return SourceDownloadStatus.SUCCESS

    def parse_nc_buffer(
        self, buffer: bytes, feed_filename: str
    ) -> Generator[SourceDownloadStatus, None, None]:
        """
        Parse an NC buffer.

        Args:
            buffer (bytes): The buffer.
            feed_filename (Optional[str]): The filename to include in the DC metadata.

        Yields:
            Document: The document.
        """
        for submission_match in RE_SUBMISSION_TAG.finditer(self.decode_buffer(buffer)):
            # get the submission buffer and header
            submission_buffer = submission_match.group(0)
            submission_metadata = self.parse_nc_metadata(submission_buffer)
            for doc_match in RE_DOC_TAG.finditer(submission_buffer):
                # get the doc buffer and header
                doc_buffer = doc_match.group(0)
                doc_metadata = self.parse_doc_metadata(doc_buffer)

                # get the content
                doc_content_start = doc_buffer.find("<TEXT>") + len("<TEXT>")
                doc_content_end = doc_buffer.rfind("</TEXT>")
                doc_content = doc_buffer[doc_content_start:doc_content_end]

                # handle uu content
                if doc_content.startswith("begin "):
                    _, doc_content_bytes = uudecode(doc_content)
                else:
                    doc_content_bytes = doc_content.encode("utf-8")

                # get the fields here
                try:
                    yield self.parse_doc_buffer(
                        doc_content_bytes,
                        submission_metadata,
                        doc_metadata,
                        feed_filename,
                    )
                except Exception as e:  # pylint: disable=broad-except
                    LOGGER.error("Error parsing document: %s", str(e))

    def download_id(
        self, document_id: int | str, **kwargs: dict[str, Any]
    ) -> SourceDownloadStatus:
        raise NotImplementedError("Download by ID is not supported for EDGAR")

    def download_date(
        self, date: datetime.date, **kwargs: dict[str, Any]
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Get the documents for a date.

        Args:
            date (datetime.date): The date.
            **kwargs: Additional parameters.

        Returns:
            SourceDownloadStatus: The download status.
        """
        current_progress = SourceProgressStatus(
            total=None,
            description="Downloading EDGAR feed",
            extra={"date": date.isoformat()},
        )

        # get the feed url
        feed_url = self.get_feed_url(date)

        # get the feed content
        try:
            feed_buffer = self._get(feed_url)
        except Exception as e:  # pylint: disable=broad-except
            # set failure and return fast
            LOGGER.error("Error downloading feed: %s", str(e))
            current_progress.failure += 1
            current_progress.status = False
            current_progress.done = True
            yield current_progress
            return

        # open in tarfile context handler
        try:
            with tarfile.open(fileobj=io.BytesIO(feed_buffer), mode="r:gz") as feed_tar:
                # iterate over members
                current_progress.total = len(feed_tar.getmembers())
                for member in feed_tar.getmembers():
                    try:
                        # get the file name
                        file_name = member.name

                        # update progress
                        current_progress.extra = {
                            "date": date.isoformat(),
                            "file_name": file_name,
                        }

                        # get the file extension
                        file_extension = Path(file_name).suffix
                        if file_extension not in (".nc",):
                            continue

                        # read the member buffer
                        member_object = feed_tar.extractfile(member)
                        if member_object is None:
                            LOGGER.error("Error extracting %s", file_name)
                            current_progress.failure += 1
                            current_progress.status = False
                            current_progress.current += 1
                            yield current_progress
                            continue

                        # read the buffer and parse it
                        member_buffer = member_object.read()
                        for doc_status in self.parse_nc_buffer(
                            member_buffer, f"{feed_url}#{file_name}"
                        ):
                            if doc_status == SourceDownloadStatus.SUCCESS:
                                current_progress.success += 1
                            else:
                                current_progress.failure += 1
                    except Exception as e:  # pylint: disable=broad-except
                        LOGGER.error(
                            "Error parsing %s in feed %s: %s",
                            file_name,
                            feed_url,
                            str(e),
                        )
                        current_progress.message = str(e)
                        current_progress.failure += 1
                        current_progress.status = False
                    finally:
                        current_progress.current += 1
                        yield current_progress
                        current_progress.message = None
        except Exception as e:  # pylint: disable=broad-except
            # set failure and return fast
            LOGGER.error("Error extracting feed: %s", str(e))
            current_progress.failure += 1
            current_progress.status = False
            current_progress.done = True
            yield current_progress
            return

    def download_date_range(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        **kwargs: dict[str, Any],
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Get the documents for a date range.

        Args:
            start_date (datetime.date): The start date.
            end_date (datetime.date): The end date.
            **kwargs: Additional parameters.

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
            yield from self.download_date(current_date)
            current_date += datetime.timedelta(days=1)


if __name__ == "__main__":
    source = EDGARSource()
    for y in source.download_date(datetime.date(1997, 6, 3)):
        print(y)
