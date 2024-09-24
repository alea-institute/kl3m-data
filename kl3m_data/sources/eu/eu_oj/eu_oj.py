"""
EU Official Journal source
"""

# imports
import datetime
import re
import urllib.parse
from typing import Any, Generator

import lxml.html

# packages

# project
from kl3m_data.sources.base_source import (
    BaseSource,
    SourceMetadata,
    SourceDownloadStatus,
    SourceProgressStatus,
)

# constants
LANGUAGE_LIST = [
    # german
    "DEU",
    # english
    "ENG",
    # spanish
    "SPA",
    # french
    "FRA",
]

# default years
EU_OJ_MIN_YEAR = 2000
EU_OJ_MAX_YEAR = datetime.datetime.now().year

# don't bother parsing rdf here given simplicity of flow
RE_URI = re.compile("<uri>(.*?)</uri>", re.IGNORECASE)
RE_CELEX = re.compile(
    'http://publications.europa.eu/resource/celex/[^/"<>]+', re.IGNORECASE
)


class EUOJRSource(BaseSource):
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

        # get min and max years
        self.min_year = EU_OJ_MIN_YEAR
        if "min_year" in kwargs:
            try:
                self.min_year = datetime.datetime.strptime(
                    kwargs["min_year"], "%Y"
                ).year
            except ValueError:
                raise ValueError("min_year must be a valid year in the format YYYY")

        self.max_year = EU_OJ_MAX_YEAR
        if "max_year" in kwargs:
            try:
                self.max_year = datetime.datetime.strptime(
                    kwargs["max_year"], "%Y"
                ).year
            except ValueError:
                raise ValueError("max_year must be a valid year in the format YYYY")

    def get_oj_sparql_query(self, year: int) -> str:
        """Generate the SPARQL 1.0 query to return all OJ
        entries for a given year and language.

        Args:
            year (int): the year to search
            language (str): the language to search

        Returns:
            str: the SPARQL query
        """
        query = f"""PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    SELECT *
    WHERE {{
        ?oj cdm:official-journal_year '{year}' ^^xsd:gYear .
    }}
    """

        return query

    def get_oj_sparql_url(
        self,
        year: int,
    ) -> str:
        """Generate the SPARQL 1.0 endpoint URL to return all OJ
        entries for a given year and language.

        Args:
            year (int): the year to search
            language (str): the language to search

        Returns:
            str: the SPARQL endpoint URL
        """
        url = f"https://publications.europa.eu/webapi/rdf/sparql?query={urllib.parse.quote(self.get_oj_sparql_query(year))}"
        return url

    def parse_celex_doc(self, doc: lxml.html.HtmlElement) -> dict[str, Any]:
        """
        Parse the CELEX document from the HTML buffer.

          - date: oj-hd-date from DD.MM.YYYY
          - language: oj-hd-lg
          - source: oj-hd-ti + oj-hd-oj
          - title: eli-main-title

        Args:
            doc (lxml.html.HtmlElement): the HTML buffer

        Returns:
            dict[str, Any]: the parsed document
        """
        metadata: dict[str, Any] = {
            "date": None,
            "language": None,
            "source": None,
            "title": None,
        }

        return metadata

    def download_id(
        self, document_id: int | str, **kwargs: dict[str, Any]
    ) -> SourceDownloadStatus:
        pass

    def download_date(
        self, date: datetime.date, **kwargs: dict[str, Any]
    ) -> Generator[SourceProgressStatus, None, None]:
        pass

    def download_date_range(
        self,
        start_date: datetime.date | datetime.datetime,
        end_date: datetime.date | datetime.datetime,
        **kwargs: dict[str, Any],
    ) -> Generator[SourceProgressStatus, None, None]:
        pass

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
        #

        for year in range(self.min_year, self.max_year + 1):
            sparql_url = self.get_oj_sparql_url(year)
            result_doc = self._get(sparql_url)
            for uri in RE_URI.findall(result_doc.decode("utf-8")):
                uri_content = self._get(uri)
                for celex_uri in RE_CELEX.findall(uri_content.decode("utf-8")):
                    celex_doc = self._get_html(celex_uri)
                    self.parse_celex_doc(celex_doc)


if __name__ == "__main__":
    for p in EUOJRSource(min_year="2005").download_all():
        print(p)
