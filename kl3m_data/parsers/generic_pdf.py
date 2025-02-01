"""
Generic PDF parsing
"""

# imports
from typing import List, Optional

# packages
import alea_preprocess

# project
from kl3m_data.logger import LOGGER
from kl3m_data.parsers.generic_tika import parse_tika
from kl3m_data.parsers.parser_types import (
    ParsedDocument,
    ParsedDocumentRepresentation,
)


def parse_digital_pdf(
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
        ParsedDocument: Parsed document
    """
    # extract text and markdown
    try:
        text = alea_preprocess.parsers.pdf.conversion.extract_buffer_text(content)
    except Exception as e:
        LOGGER.error("Error extracting markdown: %s", e)
        text = None

    try:
        markdown = alea_preprocess.parsers.pdf.conversion.extract_buffer_markdown(
            content
        )
    except Exception as e:
        LOGGER.error("Error extracting markdown: %s", e)
        markdown = None

    if text is None and markdown is None:
        return []

    # create the parsed document
    parsed_document = ParsedDocument(
        source=source,
        identifier=identifier,
        success=True,
    )

    # add representations
    if text is not None:
        parsed_document.representations["text/plain"] = ParsedDocumentRepresentation(
            content=text,
            mime_type="text/plain",
        )

    if markdown is not None:
        parsed_document.representations["text/markdown"] = ParsedDocumentRepresentation(
            content=markdown,
            mime_type="text/markdown",
        )

    return [parsed_document]


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
    LOGGER.info("Parsing PDF document: %s", identifier)

    documents = []

    # get the pdf type
    pdf_type = alea_preprocess.parsers.pdf.detection.detect_buffer_type(content)

    if pdf_type in (
        alea_preprocess.parsers.pdf.detection.PyDocumentType.Text,
        alea_preprocess.parsers.pdf.detection.PyDocumentType.Mixed,
        alea_preprocess.parsers.pdf.detection.PyDocumentType.ImagePostOCR,
    ):
        # switch on type
        documents.extend(
            parse_digital_pdf(
                content=content,
                source=source,
                identifier=identifier,
            )
        )
    else:
        # use tika otherwise
        documents.extend(
            parse_tika(
                object_content=content,
                object_source=source,
                object_url=identifier,
            )
        )

    return documents
