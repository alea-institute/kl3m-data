"""
eCFR data types
"""

# imports
from __future__ import annotations

# imports
import datetime
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional


# packages

# projects


@dataclass
class ECFRAgency:
    """
    Represents an agency in the eCFR.

    Attributes:
        name (str): Full name of the agency.
        short_name (str): Abbreviated name of the agency.
        display_name (str): Name of the agency for display purposes.
        sortable_name (str): Name of the agency used for sorting.
        slug (str): URL-friendly version of the agency name.
        children (List['Agency']): List of child agencies.
        cfr_references (List[Dict[str, Any]]): List of CFR references related to the agency.
    """

    name: str
    short_name: str
    display_name: str
    sortable_name: str
    slug: str
    children: List[ECFRAgency]
    cfr_references: List[Dict[str, Any]]


@dataclass
class ECFRTitle:
    """
    Represents a title in the eCFR.

    Attributes:
        title_number (int): Title number.
        title_name (str): Title name.
        title_description (str): Title description.
        title_url (str): Title URL.
        title_parts (List[Dict[str, Any]]): List of title parts.
    """

    number: int
    name: str
    latest_amended_on: Optional[datetime.date]
    latest_issue_date: Optional[datetime.date]
    up_to_date_as_of: Optional[datetime.date]
    reserved: Optional[bool]


@dataclass
class ECFRContentVersion:
    """
    Represents a content version in the eCFR.

    Attributes:
        title (int): Title number.
        version (int): Version number.
        date (str): Date of the version.
        title_url (str): Title URL.
        version_url (str): Version URL.
        content_url (str): Content URL.
    """

    identifier: str
    name: str
    type: str
    date: datetime.date
    amendment_date: Optional[datetime.date] = None
    issue_date: Optional[datetime.date] = None
    substantive: Optional[bool] = None
    removed: Optional[bool] = None
    title: Optional[int] = None
    part: Optional[str] = None
    subpart: Optional[str] = None


@dataclass
class ECFRStructureNode:
    """
    Represents a structure node in the eCFR.

    Attributes:
        identifier (str): Node identifier.
        type (str): Node type.
        label (str): Node label.
        title (str): Node title.
        children (List['StructureNode']): List of child nodes.
    """

    identifier: str
    type: str
    label: str
    label_level: str
    label_description: str
    reserved: bool
    children: List[ECFRStructureNode] = field(default_factory=list)
    title: Optional[int] = None
    volumes: Optional[List[str]] = None
    date: Optional[datetime.date] = None
    received_on: Optional[datetime.date] = None
    descendant_range: Optional[str] = None
    generated_id: Optional[str] = None

    def get_all_nodes(self) -> List[ECFRStructureNode]:
        """
        Get all nodes in the structure as a flat list.

        Returns:
            List[StructureNode]: List of all nodes.
        """
        nodes = [self]
        for child in self.children:
            nodes.extend(child.get_all_nodes())
        return nodes
