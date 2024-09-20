"""
GovInfo data types
"""

# conform to upstream API naming, which is not snake case
# pylint: disable=invalid-name

# future
from __future__ import annotations

# imports
import datetime
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class SearchResult:
    """
    Represents a single search result from the GovInfo API.
    """

    title: str
    packageId: str
    granuleId: str
    collectionCode: str
    resultLink: str
    relatedLink: str
    lastModified: datetime.date | datetime.datetime
    dateIssued: datetime.date | datetime.datetime
    dateIngested: datetime.date | datetime.datetime
    governmentAuthor: List[str] = field(default_factory=list)
    download: Dict[str, str] = field(default_factory=dict)

    # All other fields are stored in this dictionary
    extra: Dict[str, Any] = field(default_factory=dict)

    def __getattr__(self, name):
        """
        Allows accessing items in the extra dict as if they were attributes of the class.
        """
        if name in self.extra:
            return self.extra[name]
        raise AttributeError(f"'SearchResult' object has no attribute '{name}'")

    def __setattr__(self, name, value):
        """
        Allows setting items in the extra dict as if they were attributes of the class.
        """
        if name in self.__annotations__ or name in self.__dict__:
            super().__setattr__(name, value)
        else:
            self.extra[name] = value


@dataclass
class SearchResponse:
    """
    Represents a response from the GovInfo API search endpoint.
    """

    count: int
    offsetMark: str = "*"
    results: List[SearchResult] = field(default_factory=list)


@dataclass
class PackageInfo:
    """
    Represents a package of documents from the GovInfo API.
    Core fields are explicitly defined, all other fields are stored in the extra dictionary.
    """

    packageId: str
    docClass: str
    title: str
    congress: str
    lastModified: datetime.date | datetime.datetime
    dateIssued: datetime.date | datetime.datetime
    collectionName: Optional[str] = None
    collectionCode: Optional[str] = None
    category: Optional[str] = None
    packageLink: Optional[str] = None

    # Optional fields that are common but not always present
    session: Optional[str] = None
    branch: Optional[str] = None

    # All other fields are stored in this dictionary
    extra: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        # Convert string dates to datetime objects if they're not already
        if isinstance(self.lastModified, str):
            self.lastModified = datetime.datetime.fromisoformat(
                self.lastModified.rstrip("Z")
            )
        if isinstance(self.dateIssued, str):
            self.dateIssued = datetime.datetime.fromisoformat(self.dateIssued)

    def __getattr__(self, name):
        """
        Allows accessing items in the extra dict as if they were attributes of the class.
        """
        if name in self.extra:
            return self.extra[name]
        raise AttributeError(f"'PackageInfo' object has no attribute '{name}'")

    def __setattr__(self, name, value):
        """
        Allows setting items in the extra dict as if they were attributes of the class.
        """
        if name in self.__annotations__ or name in self.__dict__:
            super().__setattr__(name, value)
        else:
            self.extra[name] = value


@dataclass
class CollectionContainer:
    """
    Represents a collection of packages from the GovInfo API.
    """

    count: int
    message: str
    nextPage: str
    previousPage: str
    offsetMark: Optional[str] = "*"
    packages: List[PackageInfo] = field(default_factory=list)


@dataclass
class GranuleMetadata:
    """
    Represents metadata for a granule from the GovInfo API.
    """

    title: str
    granuleId: str
    granuleLink: str
    granuleClass: str
    md5: Optional[str] = None


@dataclass
class GranuleContainer:
    """
    Represents a container of granules from the GovInfo API.
    """

    count: int
    offset: int
    pageSize: int
    nextPage: str
    previousPage: str
    message: Optional[str] = None
    granules: List[GranuleMetadata] = field(default_factory=list)


@dataclass
class SummaryItem:
    """
    Represents a summary item for a collection from the GovInfo API.
    """

    collectionCode: str
    collectionName: str
    packageCount: int
    granuleCount: int


@dataclass
class CollectionSummary:
    """
    Represents a summary of collections from the GovInfo API.
    """

    collections: List[SummaryItem] = field(default_factory=list)
