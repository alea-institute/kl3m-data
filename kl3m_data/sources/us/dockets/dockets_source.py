"""
US dockets source module
"""

# imports
import bz2
import csv
import datetime
import hashlib
import sys
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
from kl3m_data.utils.s3_utils import get_object_bytes, get_s3_client

# extend csv parsing limits
csv.field_size_limit(sys.maxsize)

# constants
DOCKETS_BUCKET = "com-courtlistener-storage"
DOCKETS_KEY = "bulk-data/dockets-2024-08-31.csv.bz2"


class DocketSource(BaseSource):
    """
    Docket source class
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
            dataset_id="dockets",
            dataset_home="https://archive.org/details/usfederalcourts",
            dataset_description="US Federal docket entry data collected by CourtListener and the Internet Archive.",
            dataset_license="Not subject to copyright under 17 U.S.C. 105 and provided under CC0 by CourtListener/IA.",
        )

        # call the super
        super().__init__(metadata)

        # set s3 source info
        self.dockets_bucket = DOCKETS_BUCKET
        self.dockets_key = DOCKETS_KEY
        if "dockets_key" in kwargs:
            self.dockets_key = kwargs["dockets_key"]

    def get_docket_file(self) -> Path:
        """
        Download the docket file from the source S3 bucket and key;
        check if it already exists in this path and, if not, download it.

        Returns:
            Path: The path to the docket file
        """
        cache_path = Path(__file__).parent / "dockets.csv.bz2"
        if cache_path.exists():
            return cache_path

        # get the s3 object
        s3_client = get_s3_client()
        docket_buffer = get_object_bytes(
            s3_client, self.dockets_bucket, self.dockets_key
        )

        # save the file
        if docket_buffer:
            cache_path.write_bytes(docket_buffer)
        else:
            raise RuntimeError("Failed to download docket file")

        return cache_path

    def get_docket_records(self) -> Generator[dict, None, None]:
        """
        Download the docket data from the source S3 bucket and key, then
        parse it with the CSV dict reader.

        Returns:
            list[dict]: The list of docket data
        """
        # get the s3 object
        docket_path = self.get_docket_file()

        # stream the file records with the condition filter
        with bz2.open(docket_path, "rt") as docket_file:
            csv_reader = csv.DictReader(docket_file)
            for row in csv_reader:
                # filter the url field
                docket_entry_url = row.get("filepath_ia_json", None) or ""
                docket_entry_url = docket_entry_url.strip().strip("`")
                if docket_entry_url is None or "http" not in docket_entry_url.lower():
                    continue
                yield row

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

    def download_record(self, record: dict) -> SourceDownloadStatus:
        """
        Download a single docket record and create the corresponding Document record.

        {'id': '`6306820`', 'date_created': '`2018-02-16 09:06:28.261635+00`', 'date_modified': '`2021-11-09 08:30:50.004497+00`', 'source': '`9`', 'appeal_from_str': '``', 'assigned_to_str': '`LACEY A COLLIER`', 'referred_to_str': '`ELIZABETH M TIMOTHY`', 'panel_str': '``', 'date_last_index': '`2021-11-09 08:30:50.004508+00`', 'date_cert_granted': '', 'date_cert_denied': '', 'date_argued': '', 'date_reargued': '', 'date_reargument_denied': '', 'date_filed': '`2018-01-05`', 'date_terminated': '`2018-02-13`', 'date_last_filing': '`2018-02-26`', 'case_name_short': '`SALVADOR`', 'case_name': '`SALVADOR v. MORGAN`', 'case_name_full': '``', 'slug': '`salvador-v-morgan`', 'docket_number': '`3:18-cv-00032`', 'docket_number_core': '`1800032`', 'pacer_case_id': '`95674`', 'cause': '`42:1983 Prisoner Civil Rights`', 'nature_of_suit': '`Prison Condition`', 'jury_demand': '``', 'jurisdiction_type': '`Federal question`', 'appellate_fee_status': '``', 'appellate_case_type_information': '``', 'mdl_status': '``', 'filepath_local': '``', 'filepath_ia': '``', 'filepath_ia_json': '`https://archive.org/download/gov.uscourts.flnd.95674/gov.uscourts.flnd.95674.docket.json`', 'ia_upload_failure_count': '', 'ia_needs_upload': '`t`', 'ia_date_first_change': '`2018-09-30 07:00:00+00`', 'view_count': '`0`', 'date_blocked': '', 'blocked': '`f`', 'appeal_from_id': '', 'assigned_to_id': '`680`', 'court_id': '`flnd`', 'idb_data_id': '`25067756`', 'originating_court_information_id': '', 'referred_to_id': '`9167`', 'federal_dn_case_type': '``', 'federal_dn_office_code': '``', 'federal_dn_judge_initials_assigned': '``', 'federal_dn_judge_initials_referred': '``', 'federal_defendant_number': '', 'parent_docket_id': ''}


        Args:
            record (dict): The docket record

        Returns:
            SourceDownloadStatus: The download status
        """
        try:
            # strip all records of backticks
            record = {k: v.strip("`") for k, v in record.items()}

            # download the filepath_ia_json content
            docket_entry_url = record["filepath_ia_json"]

            id_ = docket_entry_url.split("/").pop()

            # check if it already exists
            if self.check_id(id_):
                LOGGER.info("Document already exists: %s", id_)
                return SourceDownloadStatus.EXISTED

            # download the full docket json data
            docket_json_data = self._get(docket_entry_url)

            # parse the date
            try:
                date_created = datetime.datetime.fromisoformat(record["date_created"])
            except ValueError:
                date_created = None

            # populate the document record and then upload
            doc = Document(
                dataset_id=self.metadata.dataset_id,
                id=id_,
                identifier=docket_entry_url,
                title=record.get("case_name_full", record.get("case_name", None)),  # type: ignore
                date=date_created,
                publisher="Free Law Project",
                format="application/json",
                content=docket_json_data,
                size=len(docket_json_data),
                blake2b=hashlib.blake2b(docket_json_data).hexdigest(),
                extra=record.copy(),
            )

            # upload
            doc.to_s3()

            return SourceDownloadStatus.SUCCESS
        except Exception as e:  # pylint: disable=broad-except
            LOGGER.error("Error downloading record: %s", str(e))
            return SourceDownloadStatus.FAILURE

    def download_all(
        self, **kwargs: dict[str, Any]
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Download all dockets from the source.

        Args:
            **kwargs: Additional keyword arguments
        """
        # track progress
        current_progress = SourceProgressStatus(
            total=None, description="Downloading dockets..."
        )

        # download each record
        for record in self.get_docket_records():
            try:
                # download the record
                download_status = self.download_record(record)
                if download_status in (
                    SourceDownloadStatus.SUCCESS,
                    SourceDownloadStatus.EXISTED,
                ):
                    current_progress.success += 1
                else:
                    current_progress.failure += 1
                current_progress.extra = {
                    "id": record.get("id", None),
                    "date": record.get("date_created", None),
                }
            except Exception as e:  # pylint: disable=broad-except
                LOGGER.error("Error downloading document: %s", str(e))
                current_progress.message = str(e)
                current_progress.failure += 1
                current_progress.status = False
            finally:
                current_progress.current += 1
                yield current_progress
                current_progress.message = None


if __name__ == "__main__":
    source = DocketSource()
    for p in source.download_all():
        print(p)
