"""
EU Official Journal source
"""

# imports
import csv
import datetime
import hashlib
import urllib.parse
from typing import Any, Generator, Optional

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

EU_OJ_LANG_LIST = [
    # Czech
    "CES",
    # Danish
    "DAN",
    # German
    "DEU",
    # Greek
    "ELL",
    # English
    "ENG",
    # Spanish
    "SPA",
    # Estonian
    "EST",
    # Finnish
    "FIN",
    # French
    "FRA",
    # Hungarian
    "HUN",
    # Italian
    "ITA",
    # Lithuanian
    "LIT",
    # Latvian
    "LAV",
    # Maltese
    "MLT",
    # Dutch
    "NLD",
    # Polish
    "POL",
    # Portuguese
    "POR",
    # Slovak
    "SLK",
    # Slovenian
    "SLV",
    # Swedish
    "SWE",
]

EU_OJ_PRIMARY_LANG_LIST = [
    # english, spanish, french, german
    "ENG",
    "SPA",
    "FRA",
    "DEU",
]


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
    def generate_sparql_query_url(
        year: int,
        language: str = "ENG",
        result_format: str = "csv",
        timeout: int = 0,
        debug: str = "on",
        run: str = "Run Query",
    ):
        """
        Generate a SPARQL query URL for the Official Journal with the given parameters.

        Args:
            year (str or int): The year to filter the documents (e.g., 2023).
            language (str): The language code to filter titles (e.g., 'en').
            result_format (str): The output format (default: "csv").
            timeout (int): The query timeout in milliseconds (default: 0 for no timeout).
            debug (str): Debug mode ("on" or "off", default: "on").
            run (str): The run parameter to trigger query execution (default: "Run Query").

        Returns:
            str: The complete URL for the SPARQL query.
        """
        base_url = "https://publications.europa.eu/webapi/rdf/sparql"

        # Build the SPARQL query with proper formatting.
        query = (
            "PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>\n"
            'SELECT distinct ?OJ ?lang ?title_ group_concat(distinct ?author; separator=",") as ?authors ?date_document ?manif_fmx4 ?fmx4_to_download\n'
            "WHERE {\n"
            "  ?work a cdm:official-journal.\n"
            "  ?work cdm:work_date_document ?date_document.\n"
            f"  FILTER(substr(str(?date_document),1,4)='{year}')\n"
            "  ?work cdm:work_created_by_agent ?author.\n"
            "  ?work owl:sameAs ?OJ.\n"
            "  FILTER(regex(str(?OJ),'/oj/'))\n"
            "  OPTIONAL {\n"
            "    ?exp cdm:expression_title ?title.\n"
            "    ?exp cdm:expression_uses_language ?lang.\n"
            "    ?exp cdm:expression_belongs_to_work ?work.\n"
            f"    FILTER(?lang = <http://publications.europa.eu/resource/authority/language/{language}>)\n"
            "    OPTIONAL {\n"
            "      ?manif_fmx4 cdm:manifestation_manifests_expression ?exp.\n"
            "      ?manif_fmx4 cdm:manifestation_type ?type_fmx4.\n"
            "      FILTER(str(?type_fmx4)='fmx4')\n"
            "    }\n"
            "  }\n"
            "  BIND(IF(BOUND(?title), ?title, 'The Official Journal does not exist in that language'@en) AS ?title_)\n"
            "  BIND(IF(BOUND(?manif_fmx4), IRI(concat(str(?manif_fmx4), '/zip')), '') AS ?fmx4_to_download)\n"
            "}\n"
            "ORDER BY ?date_document"
        )

        # Set up the query parameters for the URL.
        params = {
            "default-graph-uri": "",
            "query": query,
            "format": result_format,
            "timeout": timeout,
            "debug": debug,
            "run": run,
        }

        # Build and return the full URL.
        url = base_url + "?" + urllib.parse.urlencode(params)
        return url

    def get_year_list(
        self, year: int, language_list: Optional[list[str]] = None
    ) -> list[dict]:
        """
        Get the list of OJ entries for the specified year.

        Args:
            year (int): the year to search
            language_list (list[str]): the list of languages

        Returns:
            list[dict]: the list of OJ entries
        """
        if language_list is None:
            language_list = EU_OJ_PRIMARY_LANG_LIST

        # store results across all langs
        results = []

        # get the download buffer
        for language in language_list:
            list_buffer = self._get(
                self.generate_sparql_query_url(year=year, language=language)
            ).decode("utf-8")
            # parse the csv using the dict reader
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
            lang = entry["lang"]
            id_ = identifier.split("/").pop() + "/" + lang.split("/").pop()

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
                    if download_status in (
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
