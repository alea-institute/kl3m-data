"""
Generic HTML parsing
"""

# imports
import html
from typing import List, Optional

# packages
import alea_preprocess

# project
from kl3m_data.logger import LOGGER
from kl3m_data.parsers.mdtransformer.auto_parser import AutoParser
from kl3m_data.parsers.mdtransformer.base.parser_config import ParserConfig
from kl3m_data.parsers.parser_types import (
    ParsedDocument,
    ParsedDocumentRepresentation,
)


HTML_TO_TEXT_RATIO = 0.1


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
        try:
            tl_text = alea_preprocess.parsers.html.conversion.extract_buffer_markdown(
                content.decode(), output_links=False, output_images=False
            )

            # if it's empty, try again after wrapping with <html> tags
            if tl_text is None or len(tl_text.strip()) == 0:
                wrapped_content = f"<html>{content.decode()}</html>"
                tl_text = (
                    alea_preprocess.parsers.html.conversion.extract_buffer_markdown(
                        wrapped_content, output_links=False, output_images=False
                    )
                )
        except Exception as e:
            LOGGER.error("Error extracting markdown via tl: %s", e)
            tl_text = ""

        try:
            mdt_text = AutoParser(
                ParserConfig(output_images=False, output_links=False)
            ).parse(
                content.decode(),
            )
        except Exception as e:
            LOGGER.error("Error extracting markdown via mdt: %s", e)
            mdt_text = ""

        # get the longer one
        if len(tl_text) > len(mdt_text) or mdt_text is None:
            text = tl_text
            LOGGER.info("Using text from tl")
        elif len(mdt_text) > len(tl_text) or tl_text is None:
            text = mdt_text
            LOGGER.info("Using text from mdt")
        elif tl_text is not None:
            text = tl_text
            LOGGER.info("Using text from tl (equal length)")
        elif mdt_text is not None:
            text = mdt_text
            LOGGER.info("Using text from mdt (equal length)")
        else:
            text = None

        # if we have a text return, make sure it's unescaped
        if text is not None and len(text.strip()) > 0:
            text = html.unescape(text)

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
