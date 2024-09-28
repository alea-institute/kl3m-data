"""
EU Official Journal source
"""

# imports
import csv
import datetime
import hashlib
import urllib.parse
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

# constants
BASE_SPARQL_URL = "http://publications.europa.eu/webapi/rdf/sparql"

# default years
EU_OJ_MIN_YEAR = 2004
EU_OJ_MAX_YEAR = datetime.datetime.now().year


class EUOJSource(BaseSource):
    """
    EU Official Journal source spanning both prior and current (act by act) publications
    from SPARQL/REST endpoints.
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
            dataset_id="eu_oj",
            dataset_home="https://eur-lex.europa.eu/oj/browse-oj.html",
            dataset_description="Official Journal of the European Union",
            dataset_license="Text is available for free use under 2011/833/EU; metadata dedicated to public domain under CC0.",
        )

        # call the super
        super().__init__(metadata)

        # set base url
        self.base_url = BASE_SPARQL_URL
        if "base_url" in kwargs:
            self.base_url = kwargs["base_url"]

        # get min and max years
        self.min_year = EU_OJ_MIN_YEAR
        if "min_year" in kwargs:
            try:
                self.min_year = datetime.datetime.strptime(
                    kwargs["min_year"], "%Y"
                ).year
            except ValueError as e:
                raise ValueError(
                    "min_year must be a valid year in the format YYYY"
                ) from e

        self.max_year = EU_OJ_MAX_YEAR
        if "max_year" in kwargs:
            try:
                self.max_year = datetime.datetime.strptime(
                    kwargs["max_year"], "%Y"
                ).year
            except ValueError as e:
                raise ValueError(
                    "max_year must be a valid year in the format YYYY"
                ) from e

    @staticmethod
    def generate_sparql_list_query(year: int) -> str:
        """
        Generates a SPARQL query string for the specified year.

        Args:
            year (str): The year to filter the documents by (e.g., "2021").

        Returns:
            str: The formatted SPARQL query.
        """
        query = f"""
        PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>

        SELECT DISTINCT ?OJ ?title_ (GROUP_CONCAT(DISTINCT ?author; SEPARATOR=",") AS ?authors)
               ?date_document ?manif_fmx4 ?fmx4_to_download
        WHERE {{
            ?work a cdm:official-journal .
            ?work cdm:work_date_document ?date_document .
            FILTER(SUBSTR(STR(?date_document), 1, 4) = "{year}")
            ?work cdm:work_created_by_agent ?author .
            ?work owl:sameAs ?OJ .
            FILTER(REGEX(STR(?OJ), '/oj/'))

            OPTIONAL {{

                ?exp cdm:expression_title ?title .
                ?exp cdm:expression_uses_language ?lang .
                ?exp cdm:expression_belongs_to_work ?work .
                FILTER(?lang = <http://publications.europa.eu/resource/authority/language/ENG>)

                OPTIONAL {{
                    ?manif_fmx4 cdm:manifestation_manifests_expression ?exp .
                    ?manif_fmx4 cdm:manifestation_type ?type_fmx4 .
                    FILTER(STR(?type_fmx4) = 'fmx4')
                }}
            }}

            BIND(IF(BOUND(?title), ?title, "The Official Journal does not exist in that language"@en) AS ?title_)
            BIND(IF(BOUND(?manif_fmx4), IRI(CONCAT(STR(?manif_fmx4), "/zip")), "") AS ?fmx4_to_download)
        }}
        ORDER BY ?date_document
        """.strip()

        return query

    def get_sparql_list_url(self, year: int) -> str:
        """
        Generates the download URL for the SPARQL query based on the specified year.

        Args:
            year (str): The year to filter the documents by (e.g., "2021").

        Returns:
            str: The complete download URL with the encoded SPARQL query.
        """

        query = self.generate_sparql_list_query(year)

        # Define the query parameters
        params = {
            "default-graph-uri": "",
            "query": query,
            "format": "csv",
            "timeout": "0",
            "debug": "on",
            "run": "Run Query",
        }

        # URL-encode the parameters
        encoded_params = urllib.parse.urlencode(params)

        # Construct the full URL
        download_url = f"{self.base_url}?{encoded_params}"

        return download_url

    def get_year_list(self, year: int) -> list[dict]:
        """
        Get the list of OJ entries for the specified year.

        Args:
            year (int): the year to search

        Returns:
            list[dict]: the list of OJ entries
        """
        # get the download buffer
        list_buffer = self._get(self.get_sparql_list_url(year)).decode("utf-8")

        # parse the csv using the dict reader
        results = []
        list_reader = csv.DictReader(list_buffer.splitlines())
        for row in list_reader:
            if row.get("fmx4_to_download", None):
                results.append(row)

        return results

    def download_id(
        self, document_id: int | str, **kwargs: dict[str, Any]
    ) -> SourceDownloadStatus:
        raise NotImplementedError("Not implemented")

    def download_date(
        self, date: datetime.date, **kwargs: dict[str, Any]
    ) -> Generator[SourceProgressStatus, None, None]:
        raise NotImplementedError("Not implemented")

    def download_date_range(
        self,
        start_date: datetime.date | datetime.datetime,
        end_date: datetime.date | datetime.datetime,
        **kwargs: dict[str, Any],
    ) -> Generator[SourceProgressStatus, None, None]:
        raise NotImplementedError("Not implemented")

    def download_entry(self, entry: dict) -> SourceDownloadStatus:
        """
        Download the specified OJ entry and upload to S3.

        Args:
            entry (dict): the OJ entry to download

        Returns:
            SourceDownloadStatus: the download status
        """
        # 1. the URL to download is in the fmx4_to_download field
        # 2. the identifier URI is in http://publications.europa.eu/resource/oj/JOL_2000_239_R
        # 3. the ID is the last part of the URI
        # 4. the title is in the title_ field
        # 5. the authors are in the authors field
        # 6. the date is in the date_document field as YYYY-MM-DD

        # get the download URL
        try:
            # get the identifier
            identifier = entry["OJ"]
            id_ = identifier.split("/")[-1]

            # check if it exists
            if self.check_id(id_):
                LOGGER.info("Document %s already exists", id_)
                return SourceDownloadStatus.EXISTED

            # get the download content otherwise
            download_url = entry["fmx4_to_download"]
            download_response = self._get_response(download_url)
            download_content = download_response.content

            # set format based on response content type
            content_type = "application/zip"
            for header_name, header_value in download_response.headers.items():
                if header_name.lower() == "content-type":
                    content_type = header_value
                    break
            content_format = content_type.split(";")[0]

            # get the title
            title = entry["title_"].strip()

            # get the authors
            author = entry["authors"].strip()

            # get the date
            date = datetime.datetime.strptime(entry["date_document"], "%Y-%m-%d")

            # create the document object
            doc = Document(
                dataset_id=self.metadata.dataset_id,
                id=id_,
                identifier=identifier,
                title=title,
                creator=[author],
                publisher="Publications Office of the European Union",
                date=date,
                format=content_format,
                content=download_content,
                size=len(download_content),
                blake2b=hashlib.blake2b(download_content).hexdigest(),
                subject=[
                    "European Union",
                    "Official Journal of the European Union",
                ],
                extra={**entry},
            )

            # push to s3
            doc.to_s3()

            return SourceDownloadStatus.SUCCESS
        except Exception as e:  # pylint: disable=broad-except
            LOGGER.error("Failed to download and parse OJ entry %s: %s", entry, e)
            return SourceDownloadStatus.FAILURE

    def download_all(
        self, **kwargs: dict[str, Any]
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Download all OJ entries from the SPARQL endpoint by following the RDF <uri> links.

        Args:
            year (int): the year to search

        Returns:
            Generator[SourceProgressStatus, None, None]: a generator of progress status
        """
        # set up source prog status
        current_progress = SourceProgressStatus(
            total=None, description="Downloading EU OJ resources..."
        )

        # iterate over the years
        for year in range(self.min_year, self.max_year + 1):
            # get all the entries
            entries = self.get_year_list(year)
            current_progress.total = len(entries)
            current_progress.extra = {"year": year}

            # iterate over the entries
            for entry in entries:
                try:
                    # download the entry
                    download_status = self.download_entry(entry)
                    if download_status == (
                        SourceDownloadStatus.SUCCESS,
                        SourceDownloadStatus.EXISTED,
                    ):
                        current_progress.success += 1
                    else:
                        current_progress.failure += 1

                    current_progress.extra = {
                        "year": year,
                        "id": entry.get("OJ", None),
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
    source = EUOJSource()
    for p in source.download_all():
        print(p)
