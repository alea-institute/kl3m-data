"""
eCFR source module.
"""

# imports
import datetime
import hashlib
from typing import Any, Generator, List, Optional

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
from kl3m_data.sources.us.ecfr.ecfr_types import (
    ECFRAgency,
    ECFRTitle,
    ECFRContentVersion,
    ECFRStructureNode,
)

# constants
BASE_URL = "https://www.ecfr.gov/api"


class ECFRSource(BaseSource):
    """
    eCFR source class.
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
            dataset_id="ecfr",
            dataset_home="https://www.ecfr.gov/",
            dataset_description="A web version of the Code of Federal Regulations (CFR) that the GPO update daily to better reflect its current status",
            dataset_license="Not subject to copyright under 17 U.S.C. 105.",
        )

        # call the super
        super().__init__(metadata)

        # set the kwargs
        self.base_url = kwargs.get("base_url", BASE_URL)

        # caches
        self.title_versions_cache: dict[int, List[ECFRContentVersion]] = {}
        self.title_structure_cache: dict[int, ECFRStructureNode] = {}

    def get_url(self, path: str) -> str:
        """
        Get the URL for the path.

        Args:
            path (str): The path.

        Returns:
            str: The URL.
        """
        return f"{self.base_url}/{path.strip('/')}"

    def get_agencies(self) -> List[ECFRAgency]:
        """
        Get the agencies.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            Generator[Agency, None, None]: The agencies.
        """
        agency_data: list[dict] = self._get_json(
            url=self.get_url("/admin/v1/agencies.json"),
        ).get("agencies", [])

        return [ECFRAgency(**agency) for agency in agency_data]

    def get_titles(self) -> List[ECFRTitle]:
        """
        Get the titles.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            Generator[Title, None, None]: The titles.
        """
        title_data: list[dict] = self._get_json(
            url=self.get_url("/versioner/v1/titles.json"),
        ).get("titles", [])

        return [ECFRTitle(**title) for title in title_data]

    def get_title_versions(self, title: int) -> List[ECFRContentVersion]:
        """
        Get the versions for a title.

        Args:
            title (str): The title number.

        Returns:
            List[dict]: The title versions.
        """
        if title in self.title_versions_cache:
            return self.title_versions_cache[title]

        content_versions = self._get_json(
            url=self.get_url(f"/versioner/v1/versions/title-{title}.json"),
        ).get("content_versions", [])

        versions = [ECFRContentVersion(**version) for version in content_versions]

        self.title_versions_cache[title] = versions

        return versions

    def get_latest_title_date(self, title: int) -> datetime.date:
        """
        Get the latest version for a title.

        Args:
            title (str): The title number.

        Returns:
            dateime.date: The latest version date.
        """
        return max(v.date for v in self.get_title_versions(title))

    def get_title_structure(
        self, title: int, date: str | datetime.date
    ) -> ECFRStructureNode:
        """
        Get the structure for a title.

        Args:
            title (str): The title number.
            date (str): The date of the structure.

        Returns:
            dict: The title structure.
        """
        if isinstance(date, datetime.date):
            date = date.strftime("%Y-%m-%d")

        if title in self.title_structure_cache:
            return self.title_structure_cache[title]

        structure_data = self._get_json(
            url=self.get_url(f"/versioner/v1/structure/{date}/title-{title}.json"),
        )

        def parse_structure_node(node: dict) -> Optional[ECFRStructureNode]:
            """
            Local method to parse a structure node.

            Args:
                node (dict): The structure node data.

            Returns:
                ECFRStructureNode: The parsed structure node.
            """
            try:
                structure = ECFRStructureNode(
                    **{
                        k: v
                        if k != "children"
                        else [parse_structure_node(child) for child in v]
                        for k, v in node.items()
                    }
                )
            except Exception as e:  # pylint: disable=broad-except
                LOGGER.error("Error parsing structure node: %s", str(e))
                return None

            # remove any empty children
            structure.children = [child for child in structure.children if child]
            return structure

        # get and cache
        parsed_data = parse_structure_node(structure_data)
        if not parsed_data:
            raise ValueError("Error parsing title structure")
        self.title_structure_cache[title] = parsed_data
        return parsed_data

    def get_title_html(self, title: int, date: str | datetime.date, **kwargs) -> str:
        """
        Get the HTML content for a title or node in a title, e.g., section or chapter.

        Example URL:
          - https://www.ecfr.gov/api/renderer/v1/content/enhanced/2022-12-29/title-1?chapter=VI
          - https://www.ecfr.gov/api/renderer/v1/content/enhanced/2022-12-29/title-1?section=62.2

        Args:
            title (str): The title number.
            date (str): The date of the content.
            **kwargs: Additional hierarchy parameters.

        Returns:
            str: The HTML content.
        """
        # set up the params
        if isinstance(date, datetime.date):
            date = date.isoformat()
        params = {k: v for k, v in kwargs.items() if k not in ("title", "date")}

        # get the content
        url = f"{self.base_url}/renderer/v1/content/enhanced/{date}/title-{title}"
        return self._get(url, params=params).decode("utf-8")

    def get_title_xml(self, title: int, date: str | datetime.date, **kwargs) -> str:
        """
        Get the XML content for a title or node in a title, e.g., section or chapter.

        Example URL:
          - https://www.ecfr.gov/api/versioner/v1/full/2022-12-29/title-1.xml
          - https://www.ecfr.gov/api/versioner/v1/full/2022-12-29/title-1.xml?section=62.2

        Args:
            title (str): The title number.
            date (str): The date of the content.
            **kwargs: Additional hierarchy parameters.

        Returns:
            str: The XML content.
        """
        # set up the params
        if isinstance(date, datetime.date):
            date = date.isoformat()
        params = {k: v for k, v in kwargs.items() if k not in ("title", "date")}

        # get the content
        url = f"{self.base_url}/versioner/v1/full/{date}/title-{title}.xml"
        return self._get(url, params=params).decode("utf-8")

    def download_id(
        self, document_id: int | str, **kwargs: dict[str, Any]
    ) -> SourceDownloadStatus:
        """
        Download a document by its ID, which is a title/section string like:
        - '1/603.18'
        - '26/1.1'

        Args:
            document_id (int): The document ID.
            **kwargs: Additional keyword arguments.

        Returns:
            SourceDownloadStatus: The download status.
        """
        try:
            # document ID stands is the title/section string like this:
            if isinstance(document_id, int):
                raise ValueError("Document ID must be a string like 'title/section'")

            try:
                title, section = document_id.split("/")
                try:
                    title_number = int(title)
                except ValueError as e:
                    raise ValueError("Title must be an integer") from e
            except ValueError as e:
                raise ValueError(
                    "Document ID must be a string like 'title/section'"
                ) from e

            # check if we have a date
            date = kwargs.get("date", self.get_latest_title_date(title_number))
            if isinstance(date, datetime.date):
                date_string = date.isoformat()
            else:
                date_string = str(date)

            # get the title structure to get the related metadata
            title_structure = self.get_title_structure(title_number, date_string)

            # get the section node
            section_structure = [
                node
                for node in title_structure.get_all_nodes()
                if node.identifier == section and node.type == "section"
            ]
            if len(section_structure) == 0:
                return SourceDownloadStatus.NOT_FOUND

            # get the section node
            section_node = section_structure[0]

            # get the content
            content = self.get_title_html(
                title_number, date_string, section=section
            ).encode("utf-8")

            # create the document
            document = Document(
                dataset_id=self.metadata.dataset_id,
                id=f"{date}/{document_id}",
                identifier=f"https://www.ecfr.gov/api/renderer/v1/content/enhanced/{date_string}/title-{title}?section={section}",
                content=content,
                blake2b=hashlib.blake2b(content).hexdigest(),
                size=len(content),
                format="text/html",
                title=section_node.label,
                description=f"{title} CFR {section_node.label.rstrip('.')} as of {date_string}",
                publisher="U.S. Government Publishing Office",
                creator=[
                    "Government Publishing Office",
                    "National Archives and Records Administration",
                    "Office of the Federal Register",
                ],
                source=self.base_url,
                date=datetime.datetime.fromisoformat(date_string),
                language="en",
                subject=[
                    "Code of Federal Regulations",
                    "Electronic Code of Federal Regulations",
                ],
            )

            # send to s3
            document.to_s3()
        except Exception as e:  # pylint: disable=broad-except
            # log the error
            LOGGER.error("Error downloading document: %s", str(e))
            return SourceDownloadStatus.FAILURE

        # return the status
        return SourceDownloadStatus.SUCCESS

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

    def download_all(
        self, **kwargs: dict[str, Any]
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Download all documents from all titles by iterating through the lists.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            Generator[SourceProgressStatus, None, None]: The progress status.
        """
        # set the progress
        current_progress = SourceProgressStatus(
            total=None, description="Downloading eCFR documents", current=0
        )

        # get the titles
        titles = self.get_titles()

        # iterate through the titles
        for title in titles:
            latest_title_date = self.get_latest_title_date(title.number)
            title_structure = self.get_title_structure(title.number, latest_title_date)
            if current_progress.total is None:
                current_progress.total = len(title_structure.get_all_nodes())
            else:
                current_progress.total += len(title_structure.get_all_nodes())
            for node in title_structure.get_all_nodes():
                if node.type == "section":
                    # update the progress
                    current_progress.extra = {
                        "title": title.number,
                        "date": latest_title_date,
                        "section": node.identifier,
                    }

                    # download the document
                    try:
                        # check if it already exists
                        if self.check_id(
                            f"{latest_title_date}/{title.number}/{node.identifier}.json"
                        ):
                            LOGGER.info(
                                "Document already exists: %s",
                                f"{title.number}/{node.identifier}",
                            )
                        else:
                            self.download_id(f"{title.number}/{node.identifier}")
                        current_progress.success += 1
                    except Exception as e:  # pylint: disable=broad-except
                        LOGGER.error("Error downloading document: %s", str(e))
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


if __name__ == "__main__":
    # print(ECFRSource().get_title_html(1, "2022-12-29", section="603.18"))
    for s in ECFRSource().download_all():
        print(s)
