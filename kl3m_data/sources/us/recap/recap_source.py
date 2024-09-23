"""
RECAP/PACER source
"""

# imports
import datetime
import hashlib
from typing import Any, Generator

import pypdfium2

from kl3m_data.logger import LOGGER
from kl3m_data.sources.base_document import Document
# packages

# project
from kl3m_data.sources.base_source import (
    BaseSource,
    SourceMetadata,
    SourceDownloadStatus,
    SourceProgressStatus,
)
from kl3m_data.utils.s3_utils import get_s3_client, iter_prefix, get_object_bytes

# constants
RECAP_BUCKET = "com-courtlistener-storage"
RECAP_PREFIX = "recap/"

# court mapping
COURT_FULL_NAMES = {
    "ca1": "United States Court of Appeals for the First Circuit",
    "ca2": "United States Court of Appeals for the Second Circuit",
    "ca3": "United States Court of Appeals for the Third Circuit",
    "ca4": "United States Court of Appeals for the Fourth Circuit",
    "ca5": "United States Court of Appeals for the Fifth Circuit",
    "ca6": "United States Court of Appeals for the Sixth Circuit",
    "ca7": "United States Court of Appeals for the Seventh Circuit",
    "ca8": "United States Court of Appeals for the Eighth Circuit",
    "ca9": "United States Court of Appeals for the Ninth Circuit",
    "ca10": "United States Court of Appeals for the Tenth Circuit",
    "ca11": "United States Court of Appeals for the Eleventh Circuit",
    "cadc": "United States Court of Appeals for the District of Columbia Circuit",
    "cafc": "United States Court of Appeals for the Federal Circuit",
    "akb": "United States Bankruptcy Court for the District of Alaska",
    "akd": "United States District Court for the District of Alaska",
    "almb": "United States Bankruptcy Court for the Middle District of Alabama",
    "almd": "United States District Court for the Middle District of Alabama",
    "alnb": "United States Bankruptcy Court for the Northern District of Alabama",
    "alnd": "United States District Court for the Northern District of Alabama",
    "alsb": "United States Bankruptcy Court for the Southern District of Alabama",
    "alsd": "United States District Court for the Southern District of Alabama",
    "areb": "United States Bankruptcy Court for the Eastern District of Arkansas",
    "ared": "United States District Court for the Eastern District of Arkansas",
    "arwb": "United States Bankruptcy Court for the Western District of Arkansas",
    "arwd": "United States District Court for the Western District of Arkansas",
    "azb": "United States Bankruptcy Court for the District of Arizona",
    "azd": "United States District Court for the District of Arizona",
    "cacb": "United States Bankruptcy Court for the Central District of California",
    "cacd": "United States District Court for the Central District of California",
    "caeb": "United States Bankruptcy Court for the Eastern District of California",
    "caed": "United States District Court for the Eastern District of California",
    "canb": "United States Bankruptcy Court for the Northern District of California",
    "cand": "United States District Court for the Northern District of California",
    "casb": "United States Bankruptcy Court for the Southern District of California",
    "casd": "United States District Court for the Southern District of California",
    "cob": "United States Bankruptcy Court for the District of Colorado",
    "cod": "United States District Court for the District of Colorado",
    "cit": "United States Court of International Trade",
    "cofc": "United States Court of Federal Claims",
    "ctb": "United States Bankruptcy Court for the District of Connecticut",
    "ctd": "United States District Court for the District of Connecticut",
    "dcb": "United States Bankruptcy Court for the District of Columbia",
    "dcd": "United States District Court for the District of Columbia",
    "deb": "United States Bankruptcy Court for the District of Delaware",
    "ded": "United States District Court for the District of Delaware",
    "flmb": "United States Bankruptcy Court for the Middle District of Florida",
    "flmd": "United States District Court for the Middle District of Florida",
    "flnb": "United States Bankruptcy Court for the Northern District of Florida",
    "flnd": "United States District Court for the Northern District of Florida",
    "flsb": "United States Bankruptcy Court for the Southern District of Florida",
    "flsd": "United States District Court for the Southern District of Florida",
    "gamb": "United States Bankruptcy Court for the Middle District of Georgia",
    "gamd": "United States District Court for the Middle District of Georgia",
    "ganb": "United States Bankruptcy Court for the Northern District of Georgia",
    "gand": "United States District Court for the Northern District of Georgia",
    "gasb": "United States Bankruptcy Court for the Southern District of Georgia",
    "gasd": "United States District Court for the Southern District of Georgia",
    "gub": "United States Bankruptcy Court for the District of Guam",
    "gud": "United States District Court for the District of Guam",
    "hib": "United States Bankruptcy Court for the District of Hawaii",
    "hid": "United States District Court for the District of Hawaii",
    "ianb": "United States Bankruptcy Court for the Northern District of Iowa",
    "iand": "United States District Court for the Northern District of Iowa",
    "iasb": "United States Bankruptcy Court for the Southern District of Iowa",
    "iasd": "United States District Court for the Southern District of Iowa",
    "idb": "United States Bankruptcy Court for the District of Idaho",
    "idd": "United States District Court for the District of Idaho",
    "ilcb": "United States Bankruptcy Court for the Central District of Illinois",
    "ilcd": "United States District Court for the Central District of Illinois",
    "ilnb": "United States Bankruptcy Court for the Northern District of Illinois",
    "ilnd": "United States District Court for the Northern District of Illinois",
    "ilsb": "United States Bankruptcy Court for the Southern District of Illinois",
    "ilsd": "United States District Court for the Southern District of Illinois",
    "innb": "United States Bankruptcy Court for the Northern District of Indiana",
    "innd": "United States District Court for the Northern District of Indiana",
    "insb": "United States Bankruptcy Court for the Southern District of Indiana",
    "insd": "United States District Court for the Southern District of Indiana",
    "ksb": "United States Bankruptcy Court for the District of Kansas",
    "ksd": "United States District Court for the District of Kansas",
    "kyeb": "United States Bankruptcy Court for the Eastern District of Kentucky",
    "kyed": "United States District Court for the Eastern District of Kentucky",
    "kywb": "United States Bankruptcy Court for the Western District of Kentucky",
    "kywd": "United States District Court for the Western District of Kentucky",
    "laeb": "United States Bankruptcy Court for the Eastern District of Louisiana",
    "laed": "United States District Court for the Eastern District of Louisiana",
    "lamb": "United States Bankruptcy Court for the Middle District of Louisiana",
    "lamd": "United States District Court for the Middle District of Louisiana",
    "lawb": "United States Bankruptcy Court for the Western District of Louisiana",
    "lawd": "United States District Court for the Western District of Louisiana",
    "mab": "United States Bankruptcy Court for the District of Massachusetts",
    "mad": "United States District Court for the District of Massachusetts",
    "mdb": "United States Bankruptcy Court for the District of Maryland",
    "mdd": "United States District Court for the District of Maryland",
    "meb": "United States Bankruptcy Court for the District of Maine",
    "med": "United States District Court for the District of Maine",
    "mieb": "United States Bankruptcy Court for the Eastern District of Michigan",
    "mied": "United States District Court for the Eastern District of Michigan",
    "miwb": "United States Bankruptcy Court for the Western District of Michigan",
    "miwd": "United States District Court for the Western District of Michigan",
    "mnb": "United States Bankruptcy Court for the District of Minnesota",
    "mnd": "United States District Court for the District of Minnesota",
    "moeb": "United States Bankruptcy Court for the Eastern District of Missouri",
    "moed": "United States District Court for the Eastern District of Missouri",
    "mowb": "United States Bankruptcy Court for the Western District of Missouri",
    "mowd": "United States District Court for the Western District of Missouri",
    "msnb": "United States Bankruptcy Court for the Northern District of Mississippi",
    "msnd": "United States District Court for the Northern District of Mississippi",
    "mssb": "United States Bankruptcy Court for the Southern District of Mississippi",
    "mssd": "United States District Court for the Southern District of Mississippi",
    "mtb": "United States Bankruptcy Court for the District of Montana",
    "mtd": "United States District Court for the District of Montana",
    "nceb": "United States Bankruptcy Court for the Eastern District of North Carolina",
    "nced": "United States District Court for the Eastern District of North Carolina",
    "ncmb": "United States Bankruptcy Court for the Middle District of North Carolina",
    "ncmd": "United States District Court for the Middle District of North Carolina",
    "ncwb": "United States Bankruptcy Court for the Western District of North Carolina",
    "ncwd": "United States District Court for the Western District of North Carolina",
    "ndb": "United States Bankruptcy Court for the District of North Dakota",
    "ndd": "United States District Court for the District of North Dakota",
    "neb": "United States Bankruptcy Court for the District of Nebraska",
    "ned": "United States District Court for the District of Nebraska",
    "nhb": "United States Bankruptcy Court for the District of New Hampshire",
    "nhd": "United States District Court for the District of New Hampshire",
    "njb": "United States Bankruptcy Court for the District of New Jersey",
    "njd": "United States District Court for the District of New Jersey",
    "nmb": "United States Bankruptcy Court for the District of New Mexico",
    "nmd": "United States District Court for the District of New Mexico",
    "nmid": "United States District Court for the Northern Mariana Islands",
    "nvb": "United States Bankruptcy Court for the District of Nevada",
    "nvd": "United States District Court for the District of Nevada",
    "nyeb": "United States Bankruptcy Court for the Eastern District of New York",
    "nyed": "United States District Court for the Eastern District of New York",
    "nynb": "United States Bankruptcy Court for the Northern District of New York",
    "nynd": "United States District Court for the Northern District of New York",
    "nysb": "United States Bankruptcy Court for the Southern District of New York",
    "nysb-mega": "United States Bankruptcy Court for the Southern District of New York",
    "nysd": "United States District Court for the Southern District of New York",
    "nywb": "United States Bankruptcy Court for the Western District of New York",
    "nywd": "United States District Court for the Western District of New York",
    "ohnb": "United States Bankruptcy Court for the Northern District of Ohio",
    "ohnd": "United States District Court for the Northern District of Ohio",
    "ohsb": "United States Bankruptcy Court for the Southern District of Ohio",
    "ohsd": "United States District Court for the Southern District of Ohio",
    "okeb": "United States Bankruptcy Court for the Eastern District of Oklahoma",
    "oked": "United States District Court for the Eastern District of Oklahoma",
    "oknb": "United States Bankruptcy Court for the Northern District of Oklahoma",
    "oknd": "United States District Court for the Northern District of Oklahoma",
    "okwb": "United States Bankruptcy Court for the Western District of Oklahoma",
    "okwd": "United States District Court for the Western District of Oklahoma",
    "orb": "United States Bankruptcy Court for the District of Oregon",
    "ord": "United States District Court for the District of Oregon",
    "paeb": "United States Bankruptcy Court for the Eastern District of Pennsylvania",
    "paed": "United States District Court for the Eastern District of Pennsylvania",
    "pamb": "United States Bankruptcy Court for the Middle District of Pennsylvania",
    "pamd": "United States District Court for the Middle District of Pennsylvania",
    "pawb": "United States Bankruptcy Court for the Western District of Pennsylvania",
    "pawd": "United States District Court for the Western District of Pennsylvania",
    "prb": "United States Bankruptcy Court for the District of Puerto Rico",
    "prd": "United States District Court for the District of Puerto Rico",
    "rib": "United States Bankruptcy Court for the District of Rhode Island",
    "rid": "United States District Court for the District of Rhode Island",
    "scb": "United States Bankruptcy Court for the District of South Carolina",
    "scd": "United States District Court for the District of South Carolina",
    "sdb": "United States Bankruptcy Court for the District of South Dakota",
    "sdd": "United States District Court for the District of South Dakota",
    "tneb": "United States Bankruptcy Court for the Eastern District of Tennessee",
    "tned": "United States District Court for the Eastern District of Tennessee",
    "tnmb": "United States Bankruptcy Court for the Middle District of Tennessee",
    "tnmd": "United States District Court for the Middle District of Tennessee",
    "tnwb": "United States Bankruptcy Court for the Western District of Tennessee",
    "tnwd": "United States District Court for the Western District of Tennessee",
    "txeb": "United States Bankruptcy Court for the Eastern District of Texas",
    "txed": "United States District Court for the Eastern District of Texas",
    "txnb": "United States Bankruptcy Court for the Northern District of Texas",
    "txnd": "United States District Court for the Northern District of Texas",
    "txsb": "United States Bankruptcy Court for the Southern District of Texas",
    "txsd": "United States District Court for the Southern District of Texas",
    "txwb": "United States Bankruptcy Court for the Western District of Texas",
    "txwd": "United States District Court for the Western District of Texas",
    "utb": "United States Bankruptcy Court for the District of Utah",
    "utd": "United States District Court for the District of Utah",
    "vaeb": "United States Bankruptcy Court for the Eastern District of Virginia",
    "vaed": "United States District Court for the Eastern District of Virginia",
    "vawb": "United States Bankruptcy Court for the Western District of Virginia",
    "vawd": "United States District Court for the Western District of Virginia",
    "vib": "United States Bankruptcy Court for the District of the Virgin Islands",
    "vid": "United States District Court for the District of the Virgin Islands",
    "vtb": "United States Bankruptcy Court for the District of Vermont",
    "vtd": "United States District Court for the District of Vermont",
    "waeb": "United States Bankruptcy Court for the Eastern District of Washington",
    "waed": "United States District Court for the Eastern District of Washington",
    "wawb": "United States Bankruptcy Court for the Western District of Washington",
    "wawd": "United States District Court for the Western District of Washington",
    "wieb": "United States Bankruptcy Court for the Eastern District of Wisconsin",
    "wied": "United States District Court for the Eastern District of Wisconsin",
    "wiwb": "United States Bankruptcy Court for the Western District of Wisconsin",
    "wiwd": "United States District Court for the Western District of Wisconsin",
    "wvnb": "United States Bankruptcy Court for the Northern District of West Virginia",
    "wvnd": "United States District Court for the Northern District of West Virginia",
    "wvsb": "United States Bankruptcy Court for the Southern District of West Virginia",
    "wvsd": "United States District Court for the Southern District of West Virginia",
    "wyb": "United States Bankruptcy Court for the District of Wyoming",
    "wyd": "United States District Court for the District of Wyoming",
}


class RECAPSource(BaseSource):
    """
    RECAP source
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
            dataset_id="recap",
            dataset_home="s3://com-courtlistener-storage/",
            dataset_description="Documents and filings retrieved by the Free Law Project/RECAP project.",
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

    def get_pdf_metadata(self, pdf_content: bytes) -> dict[str, Any]:
        """
        Extract metadata from a PDF file.

        TODO: refactor to re-use alea-preprocess once public on pypi

        Args:
            pdf_content (bytes): The PDF content.

        Returns:
            dict[str, Any]: The extracted metadata.
        """
        # init metadata and obj
        pdf_doc = None
        metadata = {}
        try:
            # parse
            pdf_doc = pypdfium2.PdfDocument(pdf_content)

            # add metadata directly
            metadata.update(pdf_doc.get_metadata_dict())

            # add direct page count
            metadata["page_count"] = len(pdf_doc)
        except Exception as e:  # pylint: disable=broad-except
            LOGGER.error("Error extracting PDF metadata: %s", e)
        finally:
            if pdf_doc:
                pdf_doc.close()

        return metadata

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
        for doc_key in iter_prefix(self.s3_client, RECAP_BUCKET, RECAP_PREFIX):
            try:
                # get basic field
                doc_filename = doc_key[len(RECAP_PREFIX) :]
                doc_court = doc_filename.split(".")[2]
                doc_court_name = COURT_FULL_NAMES.get(doc_court, "Unknown")

                current_progress.extra = {
                    "court": doc_court,
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

                # check if xml or pdf
                if doc_filename.lower().endswith(".docket.xml"):
                    document = Document(
                        dataset_id=self.metadata.dataset_id,
                        id=doc_filename,
                        identifier="s3://" + RECAP_BUCKET + "/" + doc_key,
                        content=doc_content,
                        size=len(doc_content),
                        blake2b=doc_hash,
                        format="text/xml",
                        source="RECAP",
                        creator=doc_court_name,
                        publisher="Free Law Project",
                        subject=["Docket"],
                    )
                else:
                    # get metadata extra from pypdfium2
                    doc_metadata = self.get_pdf_metadata(doc_content)

                    document = Document(
                        dataset_id=self.metadata.dataset_id,
                        id=doc_filename,
                        identifier="s3://" + RECAP_BUCKET + "/" + doc_key,
                        content=doc_content,
                        size=len(doc_content),
                        blake2b=doc_hash,
                        format="application/pdf",
                        source="RECAP",
                        creator=doc_court_name,
                        publisher="Free Law Project",
                        extra=doc_metadata,
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
    source = RECAPSource()
    for s in source.download_all():
        continue
