"""
GovInfo Source
"""

# imports
import datetime
import hashlib
import os
import time
import urllib.parse
from typing import List, Dict, Optional, Any, Generator

import httpx

# project
from kl3m_data.logger import LOGGER
from kl3m_data.sources.base_document import Document
from kl3m_data.sources.base_source import (
    BaseSource,
    SourceMetadata,
    SourceDownloadStatus,
    SourceProgressStatus,
)
from kl3m_data.sources.us.govinfo.govinfo_types import (
    SearchResponse,
    SummaryItem,
    GranuleMetadata,
    CollectionContainer,
    GranuleContainer,
    CollectionSummary,
    PackageInfo,
    SearchResult,
)

# conforming to original API names, which are not snake cased
# pylint: disable=invalid-name

# set up default sorts parameters
DEFAULT_SORTS = [
    {"field": "publishdate", "sortOrder": "ASC"},
]


# set up default min and max dates
DEFAULT_MIN_DATE = datetime.date(1995, 1, 1)
DEFAULT_MAX_DATE = datetime.date.today()

# set up defaults to avoid duplicates
DEFAULT_EXCLUDED_COLLECTIONS = (
    "CFR",
    "FR",
)

class GovInfoSource(BaseSource):
    """
    Represents a source for the GovInfo API.
    """

    def __init__(self, **kwargs):
        """
        Initialize the source.

        Args:
            update (bool): Whether to update the source.
            min_date (datetime.date): Minimum date for the source.
            max_date (datetime.date): Maximum date for the source.
            api_key (str): API key for the GovInfo API.
            delay (int): Delay between requests
        """
        # set the metadata
        metadata = SourceMetadata(
            dataset_id="govinfo",
            dataset_home="https://api.govinfo.gov",
            dataset_description="GovInfo is a service of the United States Government Publishing Office (GPO), which is a Federal agency in the legislative branch.  GovInfo provides free public access to official publications from all three branches of the Federal Government.",
            dataset_license="In general, GovInfo documents fall under 17 U.S.C. 101 and 17 U.S.C. 105 and are therefore not subject to copyright protection. Any incorporated material not covered by these provisions will be clearly marked with a notice per 17 U.S.C. 403.",
        )

        # call the super
        super().__init__(metadata)

        # set api key from kwarg or env var
        self.api_key = kwargs.get("api_key", os.getenv("GOVINFO_API_KEY", None))
        if not self.api_key:
            raise ValueError(
                "API key is required for GovInfo source; please set GOVINFO_API_KEY environment variable."
            )

        # set the min and max dates
        if "min_date" in kwargs:
            try:
                self.min_date = datetime.datetime.fromisoformat(kwargs["min_date"])
            except ValueError:
                LOGGER.warning(
                    "Invalid min_date format. Using default min_date: %s",
                    DEFAULT_MIN_DATE,
                )
                self.min_date = DEFAULT_MIN_DATE
        else:
            self.min_date = DEFAULT_MIN_DATE

        if "max_date" in kwargs:
            try:
                self.max_date = datetime.datetime.fromisoformat(kwargs["max_date"])
            except ValueError:
                LOGGER.warning(
                    "Invalid max_date format. Using default max_date: %s",
                    DEFAULT_MAX_DATE,
                )
                self.max_date = DEFAULT_MAX_DATE
        else:
            self.max_date = DEFAULT_MAX_DATE

        # add default exceptions
        self.excluded_collections = DEFAULT_EXCLUDED_COLLECTIONS
        if "excluded_collections" in kwargs:
            self.excluded_collections = kwargs["excluded_collections"]

        # set the update and delay
        self.update = kwargs.get("update", False)
        self.delay = kwargs.get("delay", 1)

        # set the base url
        self.base_url = "https://api.govinfo.gov"

        # cache collection info at startup
        self.collections = self.get_collections().collections

        # caches for repeated lookups
        self.package_cache: dict[str, Any] = {}
        self.package_summary_cache: dict[str, PackageInfo] = {}
        self.package_granule_summary_cache: dict[tuple[str, str], SearchResult] = {}
        self.granule_cache: dict[
            tuple[str, int, str, int, Optional[str]], GranuleContainer
        ] = {}

    def get_url(self, path: str) -> str:
        """
        Get the full URL for the path.

        Args:
            path (str): Path to the resource

        Returns:
            str: Full URL for the resource
        """
        return f"{self.base_url}{path}"

    def get_response_retry(
        self,
        url: str,
        headers: Optional[dict[str, str]] = None,
        params: Optional[dict[str, Any]] = None,
        max_retry: int = 3,
    ) -> httpx.Response:
        """
        Wrap GET requests with a 503 retry handled to simplify code related to the
        on-the-fly package/granule service generation.

        Args:
            url (str): The URL to get.
            headers (Optional[dict[str, str]]): The headers.
            params (Optional[dict[str, Any]]): The parameters.
            max_retry (int): The maximum number of retries.

        Returns:
            httpx.Response: The response.
        """
        # retry the request
        for _ in range(max_retry):
            try:
                # make the upstream call
                document_response = self._get_response(
                    url=url, headers=headers, params=params
                )

                # return directly if no 503
                if document_response.status_code not in (503,):
                    return document_response
            except httpx.HTTPStatusError as e:
                # NOTE: 503 really means "sleep for Retry-After" and try again
                # https://github.com/usgpo/api/blob/main/README.md#packages-service
                if e.response.status_code == 503:
                    LOGGER.info("Waiting for package service to generate...")
                    try:
                        retry_delay = int(e.response.headers.get("Retry-After", 30))
                    except ValueError:
                        retry_delay = 30

                    # sleep for the retry delay
                    time.sleep(retry_delay)
                    continue

                # raise directly if not 503
                raise e
            except Exception as e:
                # raise directly if not an httpx error
                LOGGER.error("Error downloading %s: %s", url, e)
                raise e

        # raise an error if we've exhausted retries
        raise RuntimeError(f"Exhausted retries for {url}")

    def search(
        self,
        query: str,
        page_size: int = 100,
        offset_mark: str = "*",
        result_level: str = "default",
        historical: bool = True,
        sorts: Optional[List[Dict[str, str]]] = None,
    ) -> SearchResponse:
        """
        Search the GovInfo API.

        Args:
            query (str): The search query.
            page_size (int): The number of results per page.
            offset_mark (str): The offset mark.
            result_level (str): The result level.
            historical (bool): Whether to include historical content.
            sorts (List[Dict[str, str]]): Sorts for the search.

        Returns:
            SearchResponse: The search response.
        """
        # set the path
        path = "/search"

        # set the params
        post_data = {
            "query": query,
            "pageSize": page_size,
            "offsetMark": offset_mark,
            "resultLevel": result_level,
            "historical": historical,
            "sorts": sorts or DEFAULT_SORTS,
        }

        # get the response
        LOGGER.info("Searching GovInfo with query: %s", query)
        response = self._post_json(
            url=self.get_url(path),
            headers={"X-Api-Key": self.api_key},
            json_data=post_data,
        )

        # log result count
        LOGGER.info("Search returned %s results", response.get("count", 0))

        # set the results
        results = []
        for result in response.get("results", []):
            # skip any excluded collection items
            if result.get("collectionCode") in self.excluded_collections:
                continue

            # set the result
            results.append(
                SearchResult(
                    title=result.get("title"),
                    packageId=result.get("packageId"),
                    granuleId=result.get("granuleId"),
                    collectionCode=result.get("collectionCode"),
                    resultLink=result.get("resultLink"),
                    relatedLink=result.get("relatedLink"),
                    lastModified=result.get("lastModified"),
                    dateIssued=result.get("dateIssued"),
                    dateIngested=result.get("dateIngested"),
                    governmentAuthor=result.get("governmentAuthor", []),
                    download=result.get("download", {}),
                )
            )

        # set the search response
        search_response = SearchResponse(
            count=response.get("count", 0),
            offsetMark=response.get("offsetMark", "*"),
            results=results,
        )

        # return the search response
        return search_response

    def get_package_summary(self, package_id: str) -> PackageInfo:
        """
        Get the package summary for a collection.

        Args:
            package_id (str): The package

        Returns:
            PackageInfo: The package info.
        """
        if package_id in self.package_summary_cache:
            return self.package_summary_cache[package_id]

        # return summary from package
        path = f"/packages/{package_id}/summary"

        package_json = self._get_json(
            url=self.get_url(path),
            headers={"X-Api-Key": self.api_key},
        )

        # ensure we don't store collection code with multi-tag values
        collection_code: Optional[str] = package_json.get("collectionCode", None)
        if not collection_code:
            raise ValueError(f"Collection code not found for package {package_id}")

        # now only keep the first element if there is a ;
        if ";" in collection_code:
            collection_code = collection_code[: collection_code.find(";")]

        # set default and extra fields
        pi = PackageInfo(
            packageId=package_id,
            docClass=package_json.get("docClass", None),  # type: ignore
            title=package_json.get("title", None),  # type: ignore
            congress=package_json.get("congress", None),  # type: ignore
            lastModified=package_json.get("lastModified", None),  # type: ignore
            dateIssued=package_json.get("dateIssued", None),  # type: ignore
            collectionName=package_json.get("collectionName", None),  # type: ignore
            collectionCode=collection_code,
            category=package_json.get("category", None),  # type: ignore
            session=package_json.get("session", None),  # type: ignore
            branch=package_json.get("branch", None),  # type: ignore
        )
        for key, value in package_json.items():
            setattr(pi, key, value)

        # set into cache
        self.package_summary_cache[package_id] = pi

        return pi

    def get_package_granules(
        self,
        package_id: str,
        offset: int = 0,
        offset_mark: str = "*",
        page_size: int = 10,
        granule_class: Optional[str] = None,
    ) -> GranuleContainer:
        """
        Get the granules for a package.

        Args:
            package_id (str): The package ID.
            offset (int): The offset.
            offset_mark (str): The offset mark.
            page_size (int): The page size.
            granule_class (Optional[str]): The granule class.

        Returns:
            GranuleContainer: The granule container.
        """
        if (package_id, offset, offset_mark) in self.granule_cache:
            return self.granule_cache[
                (package_id, offset, offset_mark, page_size, granule_class)
            ]

        # set path
        path = f"/packages/{package_id}/granules"

        # set up API params
        query_params = {
            "offset": offset,
            "offsetMark": offset_mark,
            "pageSize": page_size,
            "granuleClass": granule_class,
        }

        # get the response from the API and parse it
        response_json = self._get_json(
            url=self.get_url(path),
            headers={"X-Api-Key": self.api_key},
            params=query_params,
        )

        # convert the granules to GranuleMetadata objects
        for i, granule in enumerate(response_json.get("granules", [])):
            response_json["granules"][i] = GranuleMetadata(**granule)

        granule_container = GranuleContainer(**response_json)

        # set into the cache
        self.granule_cache[
            (package_id, offset, offset_mark, page_size, granule_class)
        ] = granule_container

        return granule_container

    def get_package_granule_summary(
        self,
        package_id: str,
        granule_id: str,
    ) -> SearchResult:
        """
        Get the summary for a granule.

        Args:
            package_id (str): The package ID.
            granule_id (str): The granule ID.

        Returns:
            GranuleMetadata: The granule metadata.
        """
        if (package_id, granule_id) in self.package_cache:
            return self.package_granule_summary_cache[(package_id, granule_id)]

        # return summary from package
        path = f"/packages/{package_id}/granules/{granule_id}/summary"

        granule_json = self._get_json(
            url=self.get_url(path),
            headers={"X-Api-Key": self.api_key},
        )

        sr = SearchResult(
            title=granule_json.get("title", None),  # type: ignore
            packageId=granule_json.get("packageId", None),  # type: ignore
            granuleId=granule_json.get("granuleId", None),  # type: ignore
            collectionCode=granule_json.get("collectionCode", None),  # type: ignore
            resultLink=granule_json.get("resultLink", None),  # type: ignore
            relatedLink=granule_json.get("relatedLink", None),  # type: ignore
            lastModified=granule_json.get("lastModified", None),  # type: ignore
            dateIssued=granule_json.get("dateIssued", None),  # type: ignore
            dateIngested=granule_json.get("dateIngested", None),  # type: ignore
            governmentAuthor=granule_json.get("governmentAuthor", []),  # type: ignore
            download=granule_json.get("download", {}),  # type: ignore
        )
        for key, value in granule_json.items():
            setattr(sr, key, value)

        # set into cache
        self.package_granule_summary_cache[(package_id, granule_id)] = sr

        return sr

    def get_collections(self) -> CollectionSummary:
        """
        Get the collections.

        Returns:
            CollectionSummary: The collection summary.
        """
        # set path
        path = "/collections"

        # get the response
        response_json = self._get_json(
            url=self.get_url(path),
            headers={"X-Api-Key": self.api_key},
        )

        # parse args
        summary_args = {k: v for k, v in response_json.items() if k != "collections"}
        summary_args["collections"] = [
            SummaryItem(**collection)
            for collection in response_json.get("collections", [])
        ]
        return CollectionSummary(**summary_args)

    def get_collection_updates(
        self,
        collection_code: str,
        last_modified_start: datetime.datetime,
        last_modified_end: Optional[datetime.datetime] = None,
        offset: int = 0,
        offset_mark: str = "*",
        page_size: int = 10,
    ) -> CollectionContainer:
        """
        Get the updates for a collection.

        Args:
            collection_code (str): The collection code.
            last_modified_start (datetime.datetime): The start date for last modified.
            last_modified_end (Optional[datetime.datetime]): The end date for last modified.
            offset (int): The offset.
            offset_mark (str): The offset mark.
            page_size (int): The page size.

        Returns:
            CollectionContainer: The collection container.
        """
        # encode the start date
        start_date = last_modified_start.isoformat()
        path = f"/collections/{collection_code}/{urllib.parse.quote(start_date)}Z"
        if last_modified_end:
            end_date = last_modified_end.isoformat()
            path += f"/{urllib.parse.quote(end_date)}Z"

        # set up params
        query_params = {
            "offset": offset,
            "offsetMark": offset_mark,
            "pageSize": page_size,
        }

        # get the response
        response_json = self._get_json(
            url=self.get_url(path),
            headers={"X-Api-Key": self.api_key},
            params=query_params,
        )

        collection_container = CollectionContainer(**response_json)

        # update the offset mark by parsing the nextPage if present
        if collection_container.nextPage:
            # parse the url
            next_page_parsed = urllib.parse.urlparse(collection_container.nextPage)

            # get the query
            query = urllib.parse.parse_qs(next_page_parsed.query)

            # get the offset mark
            collection_container.offsetMark = query.get("offsetMark", ["*"])[0]

        # convert all packages dicts to PackageInfo objects
        for i, package in enumerate(collection_container.packages):
            collection_container.packages[i] = PackageInfo(**package)

        return collection_container

    def populate_package_dc_document(self, package_id: str) -> Document:
        """
        Populate a Dublin Core document with metadata from a package.

        Args:
            package_id (str): The package ID.

        Returns:
            DublinCoreDocument: The Dublin Core document.
        """
        # get the package summary
        package_summary = self.get_package_summary(package_id)

        # create the dublin core doc
        dcd = Document()
        dcd.dataset_id = self.metadata.dataset_id

        # identifier
        dcd.identifier = package_summary.packageId

        # title
        dcd.title = package_summary.title

        # publisher
        dcd.publisher = package_summary.extra.get("publisher", None)

        # creators
        if "Congress" in package_summary.extra:
            dcd.creator.append(package_summary.extra["Congress"])

        for key, value in package_summary.extra.items():
            if key.lower().startswith("governmentauthor"):
                dcd.creator.append(value)

        # subject from category if present
        if package_summary.category:
            dcd.subject.append(package_summary.category)

        # part of collection
        dcd.is_part_of = package_summary.collectionName

        # handle dates
        dcd.date = dcd.date_issued = package_summary.dateIssued
        dcd.date_modified = package_summary.lastModified

        # handle held dates
        if "heldDates" in package_summary.extra:
            if isinstance(package_summary.extra["heldDates"], list):
                for date in package_summary.extra["heldDates"]:
                    try:
                        dcd.date = datetime.datetime.fromisoformat(date)
                        break
                    except ValueError:
                        pass
            else:
                try:
                    dcd.date = datetime.datetime.fromisoformat(
                        str(package_summary.extra["heldDates"])
                    )
                except ValueError:
                    pass

        # store extras
        dcd.extra = package_summary.extra.copy()

        return dcd

    @staticmethod
    def filter_download_types(download: dict[str, str]) -> dict[str, str]:
        """
        Filter the download types so that we only keep the ZIP download in cases
        where there is not at least one txt or pdf alternative.

        Args:
            download (dict[str, str]): The download types.

        Returns:
            dict[str, str]: The filtered download types.
        """
        # return copy
        return_download = download.copy()

        # check for the presence of a txt, html, xml, or pdf
        has_txt = "txtLink" in return_download
        has_pdf = "pdfLink" in return_download

        # if we have either, then pop the zipLink if it's there
        if has_txt or has_pdf:
            return_download.pop("zipLink", None)

        return return_download

    def download_link(
        self,
        download_link: str,
        download_type: str,
        collection_id: str,
        package_id: str,
        granule_id: Optional[str] = None,
    ) -> SourceDownloadStatus:
        """
        Download a link.

        Args:
            download_link (str): The download link.
            download_type (str): The download type.
            collection_id (str): The collection ID.
            package_id (str): The package ID.
            granule_id (Optional[str]): The granule ID.

        Returns:
            SourceDownloadStatus: The download status.
        """
        try:
            # populate the base doc
            document = self.populate_package_dc_document(package_id)

            # strip the "Link" string from the end
            download_type = download_type.replace("Link", "").strip().lower()

            # get the response inside 503 retry handler
            document_response = self.get_response_retry(
                url=download_link + "?api_key=" + self.api_key
            )

            # set the content type
            document.format = "application/octet-stream"
            for key, value in document_response.headers.items():
                if key.lower() == "content-type":
                    document.format = value
                    break

            # set the document id and other key fields
            if granule_id:
                document.id = (
                    f"{collection_id}/{package_id}/{granule_id}/{download_type}"
                )
            else:
                document.id = f"{collection_id}/{package_id}/{download_type}"
            document.identifier = download_link
            document.size = len(document_response.content)
            document.blake2b = hashlib.blake2b(document_response.content).hexdigest()
            document.content = document_response.content

            # save the document
            document.to_s3()

            return SourceDownloadStatus.SUCCESS
        except Exception as e:  # pylint: disable=broad-except
            LOGGER.error("Error downloading %s/%s: %s", package_id, granule_id, e)
            return SourceDownloadStatus.FAILURE

    def download_package_search_result(
        self, result: SearchResult
    ) -> SourceDownloadStatus:
        """
        Download a search result, which contains a package and granule.

        Args:
            result (SearchResult): The search result.

        Returns:
            SourceDownloadStatus: The download status.
        """
        # get base info
        try:
            # get keys to check for s3 prefix
            collection_id = result.collectionCode
            package_id = result.packageId
            if self.check_id(f"{collection_id}/{package_id}"):
                LOGGER.info("Document already exists: %s", result.packageId)
                return SourceDownloadStatus.EXISTED

            # get the package summary and populate the document
            target_summary = self.get_package_summary(package_id)
        except Exception as e:  # pylint: disable=broad-except
            LOGGER.error("Error downloading %s: %s", result.packageId, e)
            return SourceDownloadStatus.FAILURE

        # track status
        any_failed = False
        any_success = False
        download_targets = self.filter_download_types(
            target_summary.extra.get("download", {})
        )
        for download_type, download_link in download_targets.items():
            try:
                status = self.download_link(
                    download_link=download_link,
                    download_type=download_type,
                    collection_id=collection_id,
                    package_id=package_id,
                )
                if status == SourceDownloadStatus.FAILURE:
                    any_failed = True
                elif status == SourceDownloadStatus.SUCCESS:
                    any_success = True
            except Exception as e:  # pylint: disable=broad-except
                LOGGER.error("Error downloading %s: %s", result.packageId, e)
                any_failed = True
                continue

        if any_success and not any_failed:
            return SourceDownloadStatus.SUCCESS
        if any_success and any_failed:
            return SourceDownloadStatus.PARTIAL

        return SourceDownloadStatus.FAILURE

    def download_package_granule_search_result(
        self, result: SearchResult
    ) -> SourceDownloadStatus:
        """
        Download a search result, which contains a package and granule.

        Args:
            result (SearchResult): The search result.

        Returns:
            SourceDownloadStatus: The download status.
        """
        # get base info
        try:
            collection_id = result.collectionCode
            package_id = result.packageId
            granule_id = result.granuleId
            if self.check_id(f"{collection_id}/{package_id}/{granule_id}"):
                LOGGER.info(
                    "Document already exists: %s/%s", result.packageId, result.granuleId
                )
                return SourceDownloadStatus.EXISTED
            target_summary = self.get_package_granule_summary(package_id, granule_id)
        except Exception as e:  # pylint: disable=broad-except
            LOGGER.error(
                "Error downloading %s/%s: %s", result.packageId, result.granuleId, e
            )
            return SourceDownloadStatus.FAILURE

        # track status
        any_failed = False
        any_success = False
        download_targets = self.filter_download_types(target_summary.download)
        for download_type, download_link in download_targets.items():
            try:
                status = self.download_link(
                    download_link=download_link,
                    download_type=download_type,
                    collection_id=collection_id,
                    package_id=package_id,
                    granule_id=granule_id,
                )
                if status == SourceDownloadStatus.FAILURE:
                    any_failed = True
                elif status == SourceDownloadStatus.SUCCESS:
                    any_success = True
            except Exception as e:  # pylint: disable=broad-except
                LOGGER.error(
                    "Error downloading %s/%s: %s", result.packageId, result.granuleId, e
                )
                any_failed = True
                continue

        if any_success and not any_failed:
            return SourceDownloadStatus.SUCCESS

        if any_success and any_failed:
            return SourceDownloadStatus.PARTIAL

        return SourceDownloadStatus.FAILURE

    def download_search_result(self, result: SearchResult) -> SourceDownloadStatus:
        """
        Download a search result, which contains a package and granule.

        Args:
            result (SearchResult): The search result.

        Returns:
            SourceDownloadStatus: The download status.
        """
        # get base info
        granule_id = result.granuleId
        if granule_id:
            return self.download_package_granule_search_result(result)
        return self.download_package_search_result(result)

    def download_package(self, package_id: str) -> SourceDownloadStatus:
        """
        Download a package by its ID.

        Args:
            package_id (str): The package ID.

        Returns:
            SourceDownloadStatus: The download status.
        """
        # use search with packageid:{package_id} to get the download links
        search_results = self.search(f"packageid:{package_id}", page_size=1000)

        # iterate over the results and download each
        any_failed = True
        any_success = False
        for result in search_results.results:
            status = self.download_search_result(result)
            if status == SourceDownloadStatus.FAILURE:
                any_failed = True
            elif status == SourceDownloadStatus.PARTIAL:
                any_failed = True
                any_success = True
            elif status == SourceDownloadStatus.SUCCESS:
                any_success = True

        if any_success and not any_failed:
            return SourceDownloadStatus.SUCCESS

        if any_success and any_failed:
            return SourceDownloadStatus.PARTIAL

        return SourceDownloadStatus.FAILURE

    def download_id(
        self, document_id: int | str, **kwargs: dict[str, Any]
    ) -> SourceDownloadStatus:
        """
        Download a document by its ID, where document_id is a package ID.

        Args:
            document_id (int | str): The document ID.
            kwargs (dict[str, Any]): Additional keyword arguments.

        Returns:
            SourceDownloadStatus: The download status.
        """
        # download the package
        return self.download_package(str(document_id))

    def download_date(
        self, date: datetime.date, **kwargs: dict[str, Any]
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Download all documents for a given date.

        Args:
            date (datetime.date): The date to download.
            kwargs (dict[str, Any]): Additional keyword arguments.

        Yields:
            SourceProgressStatus: The progress status.
        """
        # set up source prog status
        current_progress = SourceProgressStatus(
            total=None, description="Downloading GovInfo resources..."
        )

        # use the search api with publishdate: and ingestdate: to get the documents
        query = f"(publishdate:{date.isoformat()} OR ingestdate:{date.isoformat()})"

        # add collection filter if present
        if isinstance(kwargs.get("collection", None), str):
            query += f" AND collection:{kwargs['collection']}"

        search_results = self.search(
            query=query,
            page_size=1000,
        )

        while True:
            if not search_results.results or len(search_results.results) == 0:
                break

            current_progress.total = search_results.count
            for result in search_results.results:
                try:
                    status = self.download_search_result(result)
                    if status in (
                        SourceDownloadStatus.FAILURE,
                        SourceDownloadStatus.PARTIAL,
                    ):
                        current_progress.failure += 1
                    elif status in (SourceDownloadStatus.SUCCESS,):
                        current_progress.success += 1
                    current_progress.extra = {
                        "date": date.isoformat(),
                        "package_id": result.packageId,
                        "granule_id": result.granuleId,
                        "collection_code": result.collectionCode,
                        "rate-limit-remaining": self.rate_limit_remaining,
                    }
                except Exception as e:  # pylint: disable=broad-except
                    LOGGER.warning("Error downloading %s: %s", result, e)
                    current_progress.message = str(e)
                    current_progress.failure += 1
                    current_progress.status = False
                finally:
                    # update the prog bar
                    current_progress.current += 1
                    yield current_progress
                    current_progress.message = None

            # get the next page
            search_results = self.search(
                query=query,
                page_size=1000,
                offset_mark=search_results.offsetMark,
            )

        # yield the final status
        current_progress.done = True
        yield current_progress

    def download_date_range(
        self,
        start_date: datetime.date | datetime.datetime,
        end_date: datetime.date | datetime.datetime,
        **kwargs: dict[str, Any],
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Download all documents for a given date range.

        Args:
            start_date (datetime.date | datetime.datetime): The start date.
            end_date (datetime.date | datetime.datetime): The end date.
            kwargs (dict[str, Any]): Additional keyword arguments.

        Yields:
            SourceProgressStatus: The progress status.
        """
        # track current date and progress
        current_date = start_date
        while current_date <= end_date:
            yield from self.download_date(current_date, **kwargs)
            current_date += datetime.timedelta(days=1)

    def download_all(
        self, **kwargs: dict[str, Any]
    ) -> Generator[SourceProgressStatus, None, None]:
        """
        Download all documents.

        Args:
            kwargs (dict[str, Any]): Additional keyword arguments

        Yields:
            SourceProgressStatus: The progress status.
        """
        # track current date and progress
        yield from self.download_date_range(self.min_date, self.max_date, **kwargs)
