"""
Generic HTML parsing
"""

# imports
from typing import List, Optional

# packages

# project
from kl3m_data.logger import LOGGER
from alea_markdown.normalizer import MarkdownNormalizer
from alea_markdown.auto_parser import AutoParser
from alea_markdown.regex_parser import RegexHTMLParser
from alea_markdown.lxml_parser import LXMLHTMLParser
from alea_markdown.base.parser_config import ParserConfig
from kl3m_data.parsers.parser_types import (
    ParsedDocument,
    ParsedDocumentRepresentation,
)


def parse(
    content: bytes,
    source: Optional[str] = None,
    identifier: Optional[str] = None,
) -> List[ParsedDocument]:
    """
    Parse the document data.

    Args:
        content (bytes): Document content.
        source (str): Document source.
        identifier (str): Document identifier.

    Returns:
        List[ParsedDocument]: Parsed document
    """
    LOGGER.info("Parsing HTML document: %s", identifier)

    # init return list
    documents = []

    # extract markdown
    try:
        # decode
        content = content.decode("utf-8")

        # shared config
        parser_config = ParserConfig(
            output_links=False,
            output_images=False,
        )
        # get all three parser types: regex, lxml, markdownify
        try:
            regex_parser = RegexHTMLParser(parser_config)
            regex_text = regex_parser.parse(content)
        except Exception as e:  # pylint: disable=broad-except
            LOGGER.error("Error parsing with regex: %s", e)
            regex_text = None

        try:
            lxml_parser = LXMLHTMLParser(parser_config)
            lxml_text = lxml_parser.parse(content)
        except Exception as e:  # pylint: disable=broad-except
            LOGGER.error("Error parsing with lxml: %s", e)
            lxml_text = None

        try:
            auto_parser = AutoParser(parser_config)
            auto_text = auto_parser.parse(content)
        except Exception as e:  # pylint: disable=broad-except
            LOGGER.error("Error parsing with auto: %s", e)
            auto_text = None

        # get the longest text
        regex_length = len(regex_text.split()) if regex_text else 0
        lxml_length = len(lxml_text.split()) if lxml_text else 0
        auto_length = len(auto_text.split()) if auto_text else 0
        max_length = max(regex_length, lxml_length, auto_length)
        if max_length == 0:
            text = None
        elif max_length == regex_length:
            text = regex_text
        elif max_length == lxml_length:
            text = lxml_text
        else:
            text = auto_text

        if text:
            # normalize
            normalizer = MarkdownNormalizer()
            text = normalizer.normalize(text)

            # create the parsed document
            documents.append(
                ParsedDocument(
                    source=source,
                    identifier=identifier,
                    representations={
                        "text/markdown": ParsedDocumentRepresentation(
                            content=text,
                            mime_type="text/markdown",
                        )
                    },
                    success=True,
                )
            )
        else:
            LOGGER.warning("Unable to extract any text for %s", identifier)
    except Exception as e:  # pylint: disable=broad-except
        LOGGER.error("Error extracting markdown: %s", e)

    return documents
