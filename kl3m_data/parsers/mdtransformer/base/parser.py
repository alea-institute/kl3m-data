"""
mdtransformer.parser - HTML to Markdown parser classes

This module defines an abstract base class for HTML to Markdown parsers,
specifying the interface for converting various HTML elements to their
Markdown equivalents.
"""

# imports
from abc import ABC, abstractmethod
from typing import List, TypeVar

# packages

# project
from kl3m_data.parsers.mdtransformer.base.parser_config import ParserConfig

HTMLElement = TypeVar("HTMLElement")


class HTMLToMarkdownParser(ABC):
    """Abstract base class for HTML to Markdown parsers."""

    def __init__(self, config: ParserConfig = None) -> None:
        """Initialize the HTML to Markdown parser.

        Args:
            config (ParserConfig): The parser configuration.
        """
        self.config = config or ParserConfig()

    @abstractmethod
    def parse(self, html_str: str, **kwargs) -> str:
        """Parse HTML and return Markdown.

        Args:
            html_str (str): The HTML string to parse.
            **kwargs: Additional keyword arguments for the parser.

        Returns:
            str: The converted Markdown string.

        Raises:
            ValueError: If the input HTML is invalid or cannot be parsed.
        """
        pass

    @abstractmethod
    def _parse_block_elements(self, element: HTMLElement) -> List[str]:
        """Parse block-level HTML elements.

        Args:
            element (HTMLElement): The HTML element to parse.

        Returns:
            List[str]: List of Markdown strings for block elements.

        Raises:
            ValueError: If the element is not a valid block-level element.
        """
        pass

    @abstractmethod
    def _parse_inline_elements(self, element: HTMLElement) -> str:
        """Parse inline HTML elements.

        Args:
            element (HTMLElement): The HTML element to parse.

        Returns:
            str: The converted Markdown string for inline elements.

        Raises:
            ValueError: If the element is not a valid inline element.
        """
        pass

    @abstractmethod
    def _convert_headings(self, element: HTMLElement) -> str:
        """Convert HTML headings to Markdown.

        Args:
            element (HTMLElement): The HTML heading element.

        Returns:
            str: The Markdown heading string.

        Raises:
            ValueError: If the element is not a valid heading element.
        """
        pass

    @abstractmethod
    def _convert_paragraphs(self, element: HTMLElement) -> str:
        """Convert HTML paragraphs to Markdown.

        Args:
            element (HTMLElement): The HTML paragraph element.

        Returns:
            str: The Markdown paragraph string.

        Raises:
            ValueError: If the element is not a valid paragraph element.
        """
        pass

    @abstractmethod
    def _convert_lists(self, element: HTMLElement) -> str:
        """Convert HTML lists to Markdown.

        Args:
            element (HTMLElement): The HTML list element.

        Returns:
            str: The Markdown list string.

        Raises:
            ValueError: If the element is not a valid list element.
        """
        pass

    @abstractmethod
    def _convert_links(self, element: HTMLElement) -> str:
        """Convert HTML links to Markdown.

        Args:
            element (HTMLElement): The HTML link element.

        Returns:
            str: The Markdown link string.

        Raises:
            ValueError: If the element is not a valid link element.
        """
        pass

    @abstractmethod
    def _convert_images(self, element: HTMLElement) -> str:
        """Convert HTML images to Markdown.

        Args:
            element (HTMLElement): The HTML image element.

        Returns:
            str: The Markdown image string.

        Raises:
            ValueError: If the element is not a valid image element.
        """
        pass

    @abstractmethod
    def _convert_emphasis(self, element: HTMLElement) -> str:
        """Convert HTML emphasis to Markdown.

        Args:
            element (HTMLElement): The HTML emphasis element.

        Returns:
            str: The Markdown emphasis string.

        Raises:
            ValueError: If the element is not a valid emphasis element.
        """
        pass

    @abstractmethod
    def _convert_strong(self, element: HTMLElement) -> str:
        """Convert HTML strong to Markdown.

        Args:
            element (HTMLElement): The HTML strong element.

        Returns:
            str: The Markdown strong string.

        Raises:
            ValueError: If the element is not a valid strong element.
        """
        pass

    @abstractmethod
    def _convert_code(self, element: HTMLElement) -> str:
        """Convert HTML code to Markdown.

        Args:
            element (HTMLElement): The HTML code element.

        Returns:
            str: The Markdown code string.

        Raises:
            ValueError: If the element is not a valid code element.
        """
        pass

    @abstractmethod
    def _convert_blockquote(self, element: HTMLElement) -> str:
        """Convert HTML blockquote to Markdown.

        Args:
            element (HTMLElement): The HTML blockquote element.

        Returns:
            str: The Markdown blockquote string.

        Raises:
            ValueError: If the element is not a valid blockquote element.
        """
        pass

    @abstractmethod
    def _convert_horizontal_rule(self, element: HTMLElement) -> str:
        """Convert HTML horizontal rule to Markdown.

        Args:
            element (HTMLElement): The HTML horizontal rule element.

        Returns:
            str: The Markdown horizontal rule string.

        Raises:
            ValueError: If the element is not a valid horizontal rule element.
        """
        pass

    @abstractmethod
    def _convert_table(self, element: HTMLElement) -> str:
        """Convert HTML table to Markdown.

        Args:
            element (HTMLElement): The HTML table element.

        Returns:
            str: The Markdown table string.

        Raises:
            ValueError: If the element is not a valid table element.
        """
        pass
