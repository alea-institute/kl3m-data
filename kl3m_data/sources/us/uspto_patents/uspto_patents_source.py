"""
USPTO granted patent source

Parse both older text format and newer International Common Element (ICE) XML format.

XML Docs: https://www.uspto.gov/sites/default/files/products/PatentGrantXMLv42-Documentation.pdf

"""

# imports
import datetime
import hashlib
import io
import zipfile
from pathlib import Path
from typing import Any, Generator, Iterable

# packages
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

# constants
PTO_BASE_URL = "https://bulkdata.uspto.gov/data/patent/grant/redbook/fulltext/"
GRANT_URL_LIST = Path(__file__).parent / "grant_url_list.txt"


class USPTOPatentSource(BaseSource):
    """
    USPTO granted patent source.
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
            dataset_id="uspto",
            dataset_home="https://bulkdata.uspto.gov/",
            dataset_description="Granted patents from the USPTO Red Book",
            dataset_license="Public domain unless otherwise noted per 37 CFR 1.71 et seq.",
        )

        # call the super
        super().__init__(metadata)

        # check for delay and update
        self.delay = kwargs.get("delay", 0)
        self.update = kwargs.get("update", False)

    def get_grant_urls(self) -> list[str]:
        """
        Get the list of grant URLs.

        Returns:
            list[str]: The list of grant URLs.
        """
        return [
            url.strip()
            for url in GRANT_URL_LIST.read_text().splitlines()
            if len(url.strip()) > 0
        ]

    @staticmethod
    def format_patent_record(record: dict) -> str:
        """
        Format the record as Markdown like this:
        # Patent

        ## Title
        {title}

        ## Abstract
        {abstract}

        ## Background
        {background}

        ## Claims
          - {claim[0]}
          - {claim[1]}
          ...

        Args:
            record (dict): patent record

        Returns:
            str: formatted patent record
        """
        # format header and title/abstract
        markdown_output = "# Patent\n\n"

        markdown_output += f"## Title\n{record['title']}\n\n"

        markdown_output += f"## Abstract\n\n{record['abstract']}\n\n"

        markdown_output += "## Background\n"
        if isinstance(record["background"], str):
            markdown_output += f"{record['background']}\n\n"
        elif isinstance(record["background"], list):
            for background in record["background"]:
                markdown_output += f"{background}\n\n"

        markdown_output += "## Claims\n\n"
        for _, claim in enumerate(record["claims"]):
            # make the numbered list
            markdown_output += f"{claim}\n\n"

        return markdown_output

    # pylint: disable=too-many-branches
    @staticmethod
    def parse_doc_type_0(buffer: str) -> dict:
        """
        Parse the patent record from ABST to ABST.

        Args:
            buffer (str): patent record buffer

        Returns:
            dict: parsed patent record
        """
        # initialize the record
        record: dict[str, Any] = {
            "title": None,
            "abstract": None,
            "background": [],
            "claims": [],
        }

        # split the buffer by lines
        current_segment = None
        for _, line in enumerate(buffer.split("\n")):
            # parse the TTL
            if line.startswith("TTL "):
                # set the current segment
                current_segment = "title"

                # add the text after the control token
                record["title"] = line[4:].strip()
            elif line.startswith("APN ") and "application_number" not in record:
                # set the current segment
                current_segment = "application_number"

                # add the text after the control token
                record["application_number"] = line[4:].strip()
            elif line.startswith("PNO ") and "patent_number" not in record:
                # set the current segment
                current_segment = "patent_number"

                # add the text after the control token
                record["patent_number"] = line[4:].strip()
            elif line.startswith("ISD ") and "issue_date" not in record:
                # set the current segment
                current_segment = "issue_date"

                try:
                    # format from YYYYMMDD
                    record["issue_date"] = datetime.datetime.strptime(
                        line[4:].strip(), "%Y%m%d"
                    )
                except ValueError:
                    record["issue_date"] = line[4:].strip()
            elif line.startswith("NAM ") and "inventor_name" not in record:
                # set the current segment
                current_segment = "inventor_name"

                # add the text after the control token
                record["inventor_name"] = line[4:].strip()
            elif line.startswith("ABPR "):
                # set the current segment
                current_segment = "abstract"

                # add the text after the control token
                record["abstract"] = line[5:].strip()
            elif line.startswith("BSPR "):
                # set the current segment
                current_segment = "background"

                # add the text after the control token
                record["background"].append(line[5:].strip())
            elif line.startswith("CLPR "):
                # set the current segment
                current_segment = "claims"

                # get the text after the control token
                claims_text = line[5:].strip()

                # check if it starts with a number ending with a period
                first_token = claims_text.split()[0]
                if first_token[-1] == "." and first_token[:-1].isnumeric():
                    # add a new entry
                    record["claims"].append(claims_text)
                else:
                    record["claims"][-1] += " " + claims_text.strip()
            elif line[0].isspace():
                # continue current segment
                if current_segment == "title":
                    record["title"] += " " + line.strip()
                elif current_segment == "abstract":
                    record["abstract"] += " " + line.strip()
                elif current_segment == "background":
                    # append to the last entry
                    record["background"][-1] += " " + line.strip()
                elif current_segment == "claims":
                    # append to the last entry
                    record["claims"][-1] += " " + line.strip()

        return record

    # pylint: disable=too-many-branches, too-many-statements
    @staticmethod
    def parse_doc_type_1(buffer: str) -> dict:
        """
        Parse the patent record from ABST to ABST.

        Args:
            buffer (str): patent record buffer

        Returns:
            dict: parsed patent record
        """
        # initialize the record
        record: dict[str, Any] = {
            "title": None,
            "abstract": None,
            "background": [],
            "claims": [],
        }

        # split the buffer by lines
        current_segment = None
        for _, line in enumerate(buffer.split("\n")):
            # parse the TTL
            if line.startswith("TTL "):
                # set the current segment
                current_segment = "title"

                # add the text after the control token
                record["title"] = line[4:].strip()
            # parse a APN application number line
            elif line.startswith("APN ") and "application_number" not in record:
                # set the current segment
                current_segment = "application_number"

                # add the text after the control token
                record["application_number"] = line[4:].strip()
            # parse a PNO patent number line
            elif line.startswith("PNO ") and "patent_number" not in record:
                # set the current segment
                current_segment = "patent_number"

                # add the text after the control token
                record["patent_number"] = line[4:].strip()
            # parse a ISD isue date
            elif line.startswith("ISD ") and "issue_date" not in record:
                # set the current segment
                current_segment = "issue_date"

                try:
                    # format from YYYYMMDD
                    record["issue_date"] = datetime.datetime.strptime(
                        line[4:].strip(), "%Y%m%d"
                    )
                except ValueError:
                    record["issue_date"] = line[4:].strip()
            # parse the NAM inventor name
            elif line.startswith("NAM ") and "inventor_name" not in record:
                # set the current segment
                current_segment = "inventor_name"

                # add the text after the control token
                record["inventor_name"] = line[4:].strip()
            elif line.strip() == "ABST":
                # set the current segment
                current_segment = "abstract"
            elif line.strip() == "BSUM":
                # set the current segment
                current_segment = "background"
            elif line.strip() == "CLMS":
                # set the current segment
                current_segment = "claims"
            elif line.startswith("PAL ") or line.startswith("PAR "):
                # add to relevant segment
                if current_segment == "abstract":
                    record["abstract"] = line[4:].strip()
                elif current_segment == "background":
                    record["background"].append(line[5:].strip())
                elif current_segment == "claims":
                    # add to last claim
                    claims_text = line[5:].strip()

                    # check if it starts with a number ending with a period
                    first_token = claims_text.split()[0]
                    if first_token[-1] == "." and first_token[:-1].isnumeric():
                        # add a new entry
                        record["claims"].append(claims_text)
                    else:
                        record["claims"][-1] += " " + claims_text.strip()
            elif line[0].isspace():
                # continue current segment
                if current_segment == "title":
                    record["title"] += " " + line.strip()
                elif current_segment == "abstract":
                    record["abstract"] += " " + line.strip()
                elif current_segment == "background":
                    # append to the last entry
                    if len(record["background"]) == 0:
                        record["background"].append(line.strip())
                    else:
                        record["background"][-1] += " " + line.strip()
                elif current_segment == "claims":
                    # append to the last entry
                    if len(record["claims"]) == 0:
                        record["claims"].append(line.strip())
                    else:
                        record["claims"][-1] += " " + line.strip()

        return record

    @staticmethod
    def parse_patent_data(buffer: str) -> dict:
        """
        Parse the entire patent record from ABST to ABST.

        Args:
            buffer (str): patent record buffer

        Returns:
            dict: parsed patent record
        """
        # set doc type
        if "ABPR " in buffer:
            return USPTOPatentSource.parse_doc_type_0(buffer)

        if "\nPAL " in buffer:
            return USPTOPatentSource.parse_doc_type_1(buffer)

        raise ValueError("Invalid document type")

    @staticmethod
    def split_patent_segments(input_object: io.BytesIO) -> Iterable[str]:
        """
        Split the patent record into segments from this line: "*** BRS DOCUMENT BOUNDARY ***"

        Args:
            input_object (io.TextIOWrapper): input file object

        Returns:
            Iterable[str]: patent record segments
        """
        # split the buffer by the boundary
        buffer = ""
        for raw_line in input_object:
            line = raw_line.decode("iso8859-1", "ignore")
            if line.startswith("TTL "):
                # check for at least one instance of CLPR segment token as a proxy to "valid" patent record
                if "CLPR " in buffer or "CLMS\n" in buffer or "PAL " in buffer:
                    # add the line
                    buffer += line
                    yield buffer.strip()
                buffer = ""
            else:
                buffer += line

        # yield the last buffer
        if "\nCLMS\n" in buffer:
            yield buffer

    @staticmethod
    def parse_zip_text(input_buffer: bytes) -> Iterable[dict]:
        """
        Parse the patent records from the zip file.

        Args:
            input_buffer (bytes): patent ZIP buffer

        Returns:
            Iterable[dict]: patent records
        """

        # iterate through the segments
        for segment in USPTOPatentSource.split_patent_segments(
            io.BytesIO(input_buffer)
        ):  # type: ignore
            try:
                patent_record = USPTOPatentSource.parse_patent_data(segment)
                if len(patent_record["claims"]) == 0:
                    continue
            except Exception as e:  # pylint: disable=broad-except
                LOGGER.error("Error parsing patent record: %s", str(e))
                continue

            # add the record and markdown
            patent_record["markdown"] = USPTOPatentSource.format_patent_record(
                patent_record
            )

            # add the record to the list
            yield patent_record

    @staticmethod
    def parse_zip_xml(
        xml_buffer: bytes,
    ) -> Iterable[dict]:
        """
        Parse the patent records from the zip file.

        Args:
            xml_buffer (bytes): patent XML buffer

        Returns:
            Iterable[dict]: patent records
        """
        # find segments from <<us-patent-grant to </us-patent-grant>
        p0 = xml_buffer.find(b"<us-patent-grant")
        while p0 != -1:
            # find the next segment
            p1 = xml_buffer.find(b"</us-patent-grant>", p0) + len(b"</us-patent-grant>")

            # parse the segment
            try:
                # parse it
                patent_record = lxml.etree.fromstring(xml_buffer[p0:p1])
                record: dict[str, Any] = {
                    "title": None,
                    "abstract": None,
                    "background": [],
                    "claims": [],
                    "issue_date": None,
                    "patent_number": None,
                    "application_number": None,
                    "inventor_name": "",
                }

                # get the invention-title
                title = patent_record.find(".//invention-title")
                record["title"] = lxml.etree.tostring(
                    title, method="text", encoding="unicode"
                ).strip()

                # the application number is in us-bibliographic-data-grant/application-reference//doc-number
                application_number = patent_record.find(
                    ".//application-reference//doc-number"
                )
                if application_number is not None:
                    record["application_number"] = lxml.etree.tostring(
                        application_number, method="text", encoding="unicode"
                    ).strip()

                # the patent number is in publication-reference//doc-number
                patent_number = patent_record.find(
                    ".//publication-reference//doc-number"
                )
                if patent_number is not None:
                    record["patent_number"] = lxml.etree.tostring(
                        patent_number, method="text", encoding="unicode"
                    ).strip()

                # the issue date is in publication-reference//date
                issue_date = patent_record.find(".//publication-reference//date")
                if issue_date is not None:
                    try:
                        record["issue_date"] = datetime.datetime.strptime(
                            lxml.etree.tostring(
                                issue_date, method="text", encoding="unicode"
                            ).strip(),
                            "%Y%m%d",
                        )
                    except ValueError:
                        pass

                # findall inventors
                inventors = patent_record.findall(".//inventor")
                for inventor in inventors:
                    inventor_name = inventor.find(".//first-name")
                    if inventor_name is not None:
                        record["inventor_name"] += lxml.etree.tostring(
                            inventor_name, method="text", encoding="unicode"
                        ).strip()
                    inventor_name = inventor.find(".//last-name")
                    if inventor_name is not None:
                        record["inventor_name"] += (
                            " "
                            + lxml.etree.tostring(
                                inventor_name, method="text", encoding="unicode"
                            ).strip()
                        )
                    record["inventor_name"] += ";"
                    print(record["inventor_name"])

                # get the abstract
                abstract = patent_record.find(".//abstract")
                if abstract is not None:
                    record["abstract"] = lxml.etree.tostring(
                        abstract, method="text", encoding="unicode"
                    ).strip()

                # get the list of claims
                description = patent_record.find(".//description")
                if description is not None:
                    record["background"] = lxml.etree.tostring(
                        description, method="text", encoding="unicode"
                    ).strip()

                # get the claims from claims
                claims = patent_record.findall(".//claim")
                for claim in claims:
                    claim_text = lxml.etree.tostring(
                        claim, method="text", encoding="unicode"
                    )
                    record["claims"].append(claim_text.replace("\\.", ".").strip())

                # add the record to the list
                if (
                    abstract is not None
                    and description is not None
                    and len(record["claims"]) > 0
                ):
                    record["markdown"] = USPTOPatentSource.format_patent_record(record)
                    yield record
            except Exception as e:  # pylint: disable=broad-except
                LOGGER.error("Error parsing patent record: %s", str(e))

            # find the next segment
            p0 = xml_buffer.find(b"<us-patent-grant", p1)

    def download_id(
        self, document_id: int | str, **kwargs: dict[str, Any]
    ) -> SourceDownloadStatus:
        raise NotImplementedError("Download by ID is not supported for USPTO")

    def download_date(
        self, date: datetime.date, **kwargs: dict[str, Any]
    ) -> Generator[SourceProgressStatus, None, None]:
        raise NotImplementedError("Download by date is not supported for USPTO")

    def download_date_range(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        **kwargs: dict[str, Any],
    ) -> Generator[SourceProgressStatus, None, None]:
        raise NotImplementedError("Download by range is not supported for USPTO")

    def parse_zip_file(
        self, zip_buffer: bytes, filename: str
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Parse a ZIP file.

        Args:
            zip_buffer (bytes): The ZIP file buffer.
            filename (str): The ZIP file name.

        Yields:
            SourceProgressStatus: The progress status of the download.
        """
        current_progress = SourceProgressStatus(
            total=None,
            description="Parsing USPTO ZIP file",
        )

        # open the ZIP file
        with io.BytesIO(zip_buffer) as zip_file:
            with zipfile.ZipFile(zip_file) as zip_ref:
                # get the list of files
                file_list = zip_ref.namelist()
                total_files = len(file_list)
                current_progress.total = total_files

                # iterate through the files
                for file_name in file_list:
                    try:
                        # update prog status
                        current_progress.extra = {
                            "file": file_name,
                        }

                        # parse based on type
                        if file_name.lower().endswith(".txt"):
                            # parse as older text format
                            with zip_ref.open(file_name) as input_file:
                                for patent_record in self.parse_zip_text(
                                    input_file.read()
                                ):
                                    try:
                                        # create a document from it
                                        patent_doc = Document(
                                            dataset_id=self.metadata.dataset_id,
                                            id=patent_record["patent_number"],
                                            identifier=f"{filename}#{patent_record['patent_number']}",
                                            date=patent_record["issue_date"],
                                            title=patent_record["title"],
                                            content=patent_record["markdown"].encode(
                                                "utf-8"
                                            ),
                                            blake2b=hashlib.blake2b(
                                                patent_record["markdown"].encode(
                                                    "utf-8"
                                                )
                                            ).hexdigest(),
                                            size=len(patent_record["markdown"]),
                                            publisher="US Patent and Trademark Office",
                                            source="https://bulkdata.uspto.gov/",
                                            creator=[patent_record["inventor_name"]]
                                            if "inventor_name" in patent_record
                                            else [],
                                        )

                                        patent_doc.to_s3()

                                        # increment
                                        current_progress.success += 1
                                    except Exception as e:  # pylint: disable=broad-except
                                        LOGGER.error(
                                            "Error downloading feed: %s", str(e)
                                        )
                                        current_progress.failure += 1
                                        current_progress.status = False
                                        current_progress.done = True
                                    finally:
                                        current_progress.current += 1
                                        yield current_progress
                        elif file_name.lower().endswith(".xml"):
                            with zip_ref.open(file_name) as input_file:
                                for patent_record in self.parse_zip_xml(
                                    input_file.read()
                                ):
                                    try:
                                        # create a document from it
                                        patent_doc = Document(
                                            dataset_id=self.metadata.dataset_id,
                                            id=patent_record["patent_number"],
                                            identifier=f"{filename}#{patent_record['patent_number']}",
                                            date=patent_record["issue_date"],
                                            title=patent_record["title"],
                                            content=patent_record["markdown"].encode(
                                                "utf-8"
                                            ),
                                            blake2b=hashlib.blake2b(
                                                patent_record["markdown"].encode(
                                                    "utf-8"
                                                )
                                            ).hexdigest(),
                                            size=len(patent_record["markdown"]),
                                            publisher="US Patent and Trademark Office",
                                            source="https://bulkdata.uspto.gov/",
                                            creator=patent_record[
                                                "inventor_name"
                                            ].split(";")
                                            if patent_record["inventor_name"]
                                            else None,
                                        )

                                        patent_doc.to_s3()

                                        # increment
                                        current_progress.success += 1
                                    except Exception as e:  # pylint: disable=broad-except
                                        LOGGER.error(
                                            "Error downloading feed: %s", str(e)
                                        )
                                        current_progress.failure += 1
                                        current_progress.status = False
                                        current_progress.done = True
                                    finally:
                                        current_progress.current += 1
                                        yield current_progress

                    except Exception as e:  # pylint: disable=broad-except
                        LOGGER.error(
                            "An error occurred while parsing ZIP file: %s",
                            str(e),
                        )
                        current_progress.message = str(e)
                        current_progress.failure += 1
                        current_progress.status = False
                    finally:
                        current_progress.current += 1
                        yield current_progress
                        current_progress.message = None

                    yield current_progress

    def download_all(
        self, **kwargs: dict[str, Any]
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Iterate through all of the grant URL list and parse them.

        Yields:
            SourceProgressStatus: The progress status of the download.
        """
        # get the list of URLs
        for digest_url in self.get_grant_urls():
            digest_archive = self._get(digest_url)
            yield from self.parse_zip_file(digest_archive, digest_url)
