"""
mdtransformer.parser_config - Configuration options for the Markdown parser

This module defines the configuration options for the Markdown parser.

It includes enums for parser types and Markdown styles, as well as a
ParserConfig dataclass that encapsulates all configuration options.
Utility functions for creating default and specific configurations are
also provided.
"""

# imports
from enum import Enum
from dataclasses import dataclass, field
from typing import List, Optional

from kl3m_data.logger import LOGGER


class ParserType(Enum):
    """Enum representing the types of parsers available."""

    LXML = "lxml"
    BEAUTIFULSOUP = "beautifulsoup"
    REGEX = "regex"


class MarkdownStyle(Enum):
    """Enum representing the supported Markdown styles."""

    COMMONMARK = "commonmark"
    GITHUB = "github"
    CUSTOM = "custom"


# simple tag set for basic HTML elements parsing
SIMPLE_TAG_SET = [
    "p",
    "strong",
    "em",
    "b",
    "i",
    "a",
    "ul",
    "ol",
    "li",
    "div",
    "span",
    "pre",
    "code",
    "blockquote",
    "figure",
    "figcaption",
    "imgh1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "table",
    "thead",
    "tbody",
    "tr",
    "th",
    "td",
    "br",
    "hr",
    "body",
    "article",
    "section",
]


@dataclass
class ParserConfig:
    """Configuration class for the Markdown parser."""

    parser_type: ParserType = ParserType.LXML
    markdown_style: MarkdownStyle = MarkdownStyle.GITHUB
    output_links: bool = True
    output_images: bool = True
    simple_mode: bool = False
    include_tags: List[str] = field(default_factory=list)
    exclude_tags: List[str] = field(
        default_factory=lambda: [
            "script",
            "style",
            "head",
            "meta",
            "link",
            "title",
            "noscript",
        ]
    )
    custom_inline_tags: List[str] = field(default_factory=list)
    custom_block_tags: List[str] = field(default_factory=list)
    preserve_comments: bool = False
    sanitize_html: bool = True
    table_of_contents: bool = False
    code_language_class: Optional[str] = "language-"
    emoji_support: bool = True
    task_list_support: bool = True
    strikethrough_support: bool = True
    table_support: bool = True
    footnotes_support: bool = False
    latex_math_support: bool = False

    def __post_init__(self) -> None:
        """Initialize custom configuration options for CUSTOM style."""
        if self.markdown_style == MarkdownStyle.CUSTOM:
            self.custom_heading_style: str = "#"
            self.custom_emphasis_style: str = "*"
            self.custom_strong_style: str = "**"
            self.custom_list_marker: str = "-"
            self.custom_code_block_style: str = "```"

    def update(self, **kwargs) -> None:
        """Update configuration with provided keyword arguments.

        Args:
            **kwargs: Keyword arguments to update the configuration.

        Raises:
            AttributeError: If an invalid attribute is provided.
        """
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                LOGGER.error(f"Invalid configuration attribute: {key}")
                raise AttributeError(
                    f"'{self.__class__.__name__}' object has no attribute '{key}'"
                )

    @classmethod
    def create_custom(cls, **kwargs) -> "ParserConfig":
        """Create a custom configuration with provided settings.

        Args:
            **kwargs: Keyword arguments for custom configuration.

        Returns:
            ParserConfig: A custom parser configuration.
        """
        config = cls(markdown_style=MarkdownStyle.CUSTOM)
        try:
            config.update(**kwargs)
        except AttributeError as e:
            LOGGER.error(f"Error creating custom configuration: {str(e)}")
            raise
        return config


def get_default_config() -> ParserConfig:
    """Return the default parser configuration.

    Returns:
        ParserConfig: The default parser configuration.
    """
    return ParserConfig()


def create_github_flavored_config() -> ParserConfig:
    """Create a configuration for GitHub Flavored Markdown.

    Returns:
        ParserConfig: A configuration for GitHub Flavored Markdown.
    """
    return ParserConfig(
        markdown_style=MarkdownStyle.GITHUB,
        task_list_support=True,
        strikethrough_support=True,
        table_support=True,
        emoji_support=True,
    )


def create_commonmark_config() -> ParserConfig:
    """Create a configuration for CommonMark specification.

    Returns:
        ParserConfig: A configuration for CommonMark specification.
    """
    return ParserConfig(
        markdown_style=MarkdownStyle.COMMONMARK,
        task_list_support=False,
        strikethrough_support=False,
        table_support=False,
        emoji_support=False,
    )
