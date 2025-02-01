"""
Generic XML document parsing
"""

from typing import List, Optional

# imports
import lxml.etree

# project
from kl3m_data.logger import LOGGER
from kl3m_data.parsers.converters import etree_to_json, etree_to_yaml
from kl3m_data.parsers.parser_types import (
    ParsedDocument,
    ParsedDocumentRepresentation,
)

# packages


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
    LOGGER.info("Parsing XML document: %s", identifier)

    representations = {}

    # check for EDGAR <XML> and </XML> tags at start and end
    if content.strip().startswith(b"<XML>") and content.strip().endswith(b"</XML>"):
        # get positions and remove it
        start = content.find(b"<XML>")
        end = content.rfind(b"</XML>")
        content = content[start + len(b"<XML>") : end].strip()

    # try to parse and nicely format the xml
    try:
        xml_doc = lxml.etree.fromstring(content)
        representations["application/xml"] = ParsedDocumentRepresentation(
            content=lxml.etree.tostring(
                xml_doc, method="xml", pretty_print=True
            ).decode(),
            mime_type="application/xml",
        )
    except Exception as e:
        LOGGER.error("Error parsing XML: %s", e)
        return []

    # convert to a nested json object next
    try:
        representations["application/json"] = ParsedDocumentRepresentation(
            content=etree_to_json(xml_doc),
            mime_type="application/json",
        )
    except Exception as e:
        LOGGER.error("Error converting XML to JSON: %s", e)

    # convert to yaml
    try:
        representations["application/yaml"] = ParsedDocumentRepresentation(
            content=etree_to_yaml(xml_doc),
            mime_type="application/yaml",
        )
    except Exception as e:
        LOGGER.error("Error converting XML to YAML: %s", e)

    # create the parsed document
    parsed_document = ParsedDocument(
        source=source,
        identifier=identifier,
        representations=representations,
        success=True,
    )

    return [parsed_document]
