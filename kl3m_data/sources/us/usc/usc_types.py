"""
USC types module
"""

# imports
import hashlib
from dataclasses import dataclass
from typing import Optional

# packages
import lxml.html

# project
from kl3m_data.logger import LOGGER
from kl3m_data.sources.base_document import Document

# constants for USC release point/versions
MAX_TITLES = 54
BASE_DOMAIN = "https://uscode.house.gov/"


@dataclass
class USCReleaseFile:
    """
    Release file data class
    """

    title: int
    congress: int
    public_law: int
    filename: str
    contents: bytes


@dataclass
class USCDocument:
    """
    Represents a parsed document from USC XHTML content.
    """

    congress: int
    public_law: int
    title: int
    documentid: str
    itempath: str
    itemsortkey: str
    expcite: str
    currentthrough: str
    document_pdf_page: Optional[int] = None
    usckey: Optional[str] = None
    xhtml: Optional[bytes] = None
    doc: Optional[lxml.html.HtmlElement] = None

    def get_expcite_tokens(self) -> list[str]:
        """
        Get the tokens of the expcite string.

        Returns:
            list[str]: The tokens of the expcite string.
        """
        return self.expcite.split("!@!")

    def get_title_heading(self) -> str:
        """
        Get the title heading.

        Returns:
            str: The title heading.
        """
        return self.get_expcite_tokens()[0]

    def get_chapter_heading(self) -> Optional[str]:
        """
        Get the chapter heading.

        Returns:
            str: The chapter heading.
        """
        for token in self.get_expcite_tokens():
            if token.startswith("CHAPTER"):
                return token
        return None

    def get_section(self) -> Optional[str]:
        """
        Get the section.

        Returns:
            str: The section.
        """
        for token in self.get_expcite_tokens():
            if token.startswith("Sec."):
                return token
        return None

    def get_section_heading(self) -> Optional[str]:
        """
        Get the section heading.

        Returns:
            str: The section heading.
        """
        if not self.xhtml:
            return None

        p0 = self.xhtml.find(b'<h3 class="section-head">')
        p1 = self.xhtml.find(b"</h3>", p0)

        # parse fragment and get text
        try:
            fragment_doc = lxml.html.fromstring(self.xhtml[p0 : p1 + len("</h3>")])
            section_heading = lxml.html.tostring(
                fragment_doc, encoding="utf-8", method="text"
            ).strip()
            return section_heading
        except Exception as e:  # pylint: disable=broad-except
            LOGGER.error("Error getting section heading: %s", str(e))
            return None

    def get_citation(self) -> str:
        """
        Get the citation for the document.

        Returns:
            str: The citation for the
        """
        return f"{self.get_title_heading()} USC {self.get_section()}"

    def get_temporal_id(self) -> str:
        """
        Get the temporal ID for the document.

        Returns:
            str: The temporal ID.
        """
        return f"{self.congress}/{self.public_law}/{self.documentid}"

    def get_heading(self) -> Optional[str]:
        """
        Get the most appropriate heading for the document.

        Returns:
            str: The heading.
        """
        if not self.xhtml:
            return None

        # find text of section-head
        p0 = self.xhtml.find(b'<h3 class="section-head">')
        p1 = self.xhtml.find(b"</h3>", p0)

        # parse fragment and get text
        try:
            fragment_doc = lxml.html.fromstring(self.xhtml[p0 : p1 + len("</h3>")])
            section_heading = lxml.html.tostring(
                fragment_doc, encoding="latin1", method="text"
            ).strip()
            return f"{self.title} USC {section_heading.decode('utf-8')}"
        except Exception as e:  # pylint: disable=broad-except
            LOGGER.error("Error getting heading for %s: %s", self.documentid, str(e))
            return None

    def get_release_point_url(self) -> str:
        """
        Get link to the release point for the document.

        Returns:
            str: The release point URL.
        """
        return f"{BASE_DOMAIN}download/releasepoints/us/pl/{self.congress}/{self.public_law}/htm_usc{self.title:02d}@{self.congress}-{self.public_law}.zip#{self.documentid}"

    def to_document(self, dataset_id: str) -> Document:
        """
        Convert the USCDocument to a Document object.

        Returns:
            Document: The Document object.
        """
        return Document(
            dataset_id=dataset_id,
            id=self.get_temporal_id(),
            blake2b=hashlib.blake2b(self.xhtml).hexdigest() if self.xhtml else None,
            content=self.xhtml,
            size=len(self.xhtml) if self.xhtml else None,
            title=self.get_heading(),
            identifier=self.get_release_point_url(),
            format="application/xhtml+xml",
            creator=[
                "Office of the Law Revision Counsel",
                "U.S. House of Representatives",
            ],
            publisher="Office of the Law Revision Counsel",
            description=f"{self.get_citation()} as of {self.currentthrough}",
            subject=["United States Code"],
            extra={
                "congress": self.congress,
                "public_law": self.public_law,
                "title": self.title,
                "documentid": self.documentid,
                "itempath": self.itempath,
                "itemsortkey": self.itemsortkey,
                "expcite": self.expcite,
                "currentthrough": self.currentthrough,
                "documentPDFPage": self.document_pdf_page,
                "usckey": self.usckey,
            },
        )
