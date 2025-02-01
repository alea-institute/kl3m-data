"""
mdtransformer.html_types - HTML element classes for Markdown to HTML conversion

This module defines HTML element classes to support translating Markdown
abstract syntax trees to HTML using lxml.html.

The module provides a set of classes representing various HTML elements,
as well as utility functions for creating and manipulating HTML documents.
"""

# imports
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional

# packages
from lxml import etree, html

from kl3m_data.logger import LOGGER


@dataclass
class HTMLElement:
    """
    Base class for HTML elements.

    Attributes:
        tag (str): The HTML tag name.
        attributes (dict): A dictionary of HTML attributes.
        children (List[HTMLElement]): A list of child HTML elements.
        text (Optional[str]): The text content of the element.
    """

    tag: str
    attributes: dict = field(default_factory=dict)
    children: List[HTMLElement] = field(default_factory=list)
    text: Optional[str] = None

    def to_lxml(self) -> etree._Element:
        """
        Convert the HTMLElement to an lxml element.

        Returns:
            etree._Element: The lxml representation of the HTML element.

        Raises:
            Exception: If there's an error during conversion.
        """
        try:
            element = etree.Element(self.tag, attrib=self.attributes)
            if self.text:
                element.text = self.text
            for child in self.children:
                element.append(child.to_lxml())
            return element
        except Exception as e:
            LOGGER.error(f"Error converting HTMLElement to lxml: {e}")
            raise


@dataclass
class Paragraph(HTMLElement):
    """Represents an HTML paragraph element."""

    tag: str = "p"


@dataclass
class Heading(HTMLElement):
    """
    Represents an HTML heading element.

    Attributes:
        level (int): The heading level (1-6).
    """

    level: int = 1
    tag: str = field(init=False)

    def __post_init__(self):
        self.tag = f"h{self.level}"


@dataclass
class Emphasis(HTMLElement):
    """Represents an HTML emphasis element."""

    tag: str = "em"


@dataclass
class Strong(HTMLElement):
    """Represents an HTML strong element."""

    tag: str = "strong"


@dataclass
class Anchor(HTMLElement):
    """Represents an HTML anchor element."""

    tag: str = "a"


@dataclass
class Image(HTMLElement):
    """Represents an HTML image element."""

    tag: str = "img"


@dataclass
class Code(HTMLElement):
    """Represents an HTML code element."""

    tag: str = "code"


@dataclass
class Pre(HTMLElement):
    """Represents an HTML preformatted text element."""

    tag: str = "pre"


@dataclass
class ListItem(HTMLElement):
    """Represents an HTML list item element."""

    tag: str = "li"


@dataclass
class UnorderedList(HTMLElement):
    """Represents an HTML unordered list element."""

    tag: str = "ul"


@dataclass
class OrderedList(HTMLElement):
    """Represents an HTML ordered list element."""

    tag: str = "ol"


@dataclass
class BlockQuote(HTMLElement):
    """Represents an HTML blockquote element."""

    tag: str = "blockquote"


@dataclass
class HorizontalRule(HTMLElement):
    """Represents an HTML horizontal rule element."""

    tag: str = "hr"


@dataclass
class Table(HTMLElement):
    """Represents an HTML table element."""

    tag: str = "table"


@dataclass
class TableRow(HTMLElement):
    """Represents an HTML table row element."""

    tag: str = "tr"


@dataclass
class TableHeader(HTMLElement):
    """Represents an HTML table header cell element."""

    tag: str = "th"


@dataclass
class TableCell(HTMLElement):
    """Represents an HTML table cell element."""

    tag: str = "td"


@dataclass
class Article(HTMLElement):
    """Represents an HTML5 article element."""

    tag: str = "article"


@dataclass
class Section(HTMLElement):
    """Represents an HTML5 section element."""

    tag: str = "section"


@dataclass
class Nav(HTMLElement):
    """Represents an HTML5 nav element."""

    tag: str = "nav"


@dataclass
class Aside(HTMLElement):
    """Represents an HTML5 aside element."""

    tag: str = "aside"


@dataclass
class Header(HTMLElement):
    """Represents an HTML5 header element."""

    tag: str = "header"


@dataclass
class Footer(HTMLElement):
    """Represents an HTML5 footer element."""

    tag: str = "footer"


@dataclass
class Main(HTMLElement):
    """Represents an HTML5 main element."""

    tag: str = "main"


@dataclass
class Figure(HTMLElement):
    """Represents an HTML5 figure element."""

    tag: str = "figure"


@dataclass
class FigCaption(HTMLElement):
    """Represents an HTML5 figcaption element."""

    tag: str = "figcaption"


@dataclass
class HTMLDocument:
    """
    Represents a complete HTML document.

    Attributes:
        head (HTMLElement): The head element of the HTML document.
        body (HTMLElement): The body element of the HTML document.
    """

    head: HTMLElement = field(default_factory=lambda: HTMLElement("head"))
    body: HTMLElement = field(default_factory=lambda: HTMLElement("body"))

    def to_lxml(self) -> etree._Element:
        """
        Convert the HTMLDocument to an lxml HTML document.

        Returns:
            etree._Element: The lxml representation of the HTML document.

        Raises:
            Exception: If there's an error during conversion.
        """
        try:
            html_element = etree.Element("html")
            html_element.append(self.head.to_lxml())
            html_element.append(self.body.to_lxml())
            return html.document_fromstring(etree.tostring(html_element))
        except Exception as e:
            LOGGER.error(f"Error converting HTMLDocument to lxml: {e}")
            raise

    def to_string(self) -> str:
        """
        Convert the HTMLDocument to a string.

        Returns:
            str: The string representation of the HTML document.

        Raises:
            Exception: If there's an error during conversion.
        """
        try:
            return html.tostring(self.to_lxml(), pretty_print=True, encoding="unicode")
        except Exception as e:
            LOGGER.error(f"Error converting HTMLDocument to string: {e}")
            raise


def create_html_element(
    tag: str, text: Optional[str] = None, **attributes
) -> HTMLElement:
    """
    Create an HTMLElement with the given tag, text, and attributes.

    Args:
        tag (str): The HTML tag name.
        text (Optional[str]): The text content of the element.
        **attributes: Keyword arguments for HTML attributes.

    Returns:
        HTMLElement: The created HTML element.
    """
    return HTMLElement(tag, attributes, text=text)
