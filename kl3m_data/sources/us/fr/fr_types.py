"""
FR data types
"""

# imports
from __future__ import annotations

# imports
import datetime
from dataclasses import dataclass
from typing import List, Optional


# packages

# projects


@dataclass
class FRAgency:
    """
    Represents an agency in the FR API.
    """

    agency_url: str
    child_ids: List[int]
    child_slugs: List[str]
    description: str
    id: int
    logo: str
    name: str
    parent_id: int
    recent_articles_url: str
    short_name: str
    slug: str
    url: str
    json_url: str


# pylint: disable=too-many-instance-attributes
@dataclass
class FRDocument:
    """
    Represents a document in the FR API.
    """

    abstract: Optional[str]
    action: Optional[str]
    agencies: list
    agency_names: list
    body_html_url: str
    cfr_references: list
    citation: str
    comment_url: Optional[str]
    comments_close_on: Optional[str]
    correction_of: Optional[str]
    corrections: list
    dates: Optional[str]
    disposition_notes: Optional[str]
    docket_id: Optional[str]
    docket_ids: list
    dockets: list
    document_number: str
    effective_on: Optional[str]
    end_page: int
    excerpts: Optional[str]
    executive_order_notes: Optional[str]
    executive_order_number: Optional[str]
    explanation: Optional[str]
    full_text_xml_url: str
    html_url: str
    images: dict
    images_metadata: dict
    json_url: str
    mods_url: str
    not_received_for_publication: Optional[str]
    page_length: int
    page_views: dict
    pdf_url: str
    president: dict
    presidential_document_number: Optional[str]
    proclamation_number: Optional[str]
    public_inspection_pdf_url: str
    publication_date: str
    raw_text_url: str
    regulation_id_number_info: dict
    regulation_id_numbers: list
    regulations_dot_gov_info: dict
    regulations_dot_gov_url: Optional[str]
    significant: Optional[str]
    signing_date: Optional[datetime.date]
    start_page: int
    subtype: Optional[str]
    title: str
    toc_doc: str
    toc_subject: Optional[str]
    topics: list
    type: str
    volume: int
