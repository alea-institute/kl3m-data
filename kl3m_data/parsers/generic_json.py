"""
Generic JSON document parsing
"""

# imports
import json
from typing import List, Optional

# project
from kl3m_data.logger import LOGGER
from kl3m_data.parsers.converters import json_to_xml, json_to_yaml
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
        ParsedDocument: Parsed document
    """
    LOGGER.info("Parsing JSON document: %s", identifier)

    documents = []

    # try to parse the json
    try:
        document_data = json.loads(content.decode(), strict=False)

        # initialize the representations
        representations = {
            "application/json": ParsedDocumentRepresentation(
                content=json.dumps(document_data, indent=2, default=str),
                mime_type="application/json",
            )
        }
    except Exception as e:  # pylint: disable=broad-except
        LOGGER.error("Error parsing JSON: %s", e)
        return documents

    # try to add the yaml version
    try:
        representations["application/yaml"] = ParsedDocumentRepresentation(
            content=json_to_yaml(document_data),
            mime_type="application/yaml",
        )
    except Exception as e:  # pylint: disable=broad-except
        LOGGER.error("Error converting JSON to YAML: %s", e)

    # try to add the xml version
    try:
        representations["application/xml"] = ParsedDocumentRepresentation(
            content=json_to_xml(document_data),
            mime_type="application/xml",
        )
    except Exception as e:  # pylint: disable=broad-except
        LOGGER.error("Error converting JSON to XML: %s", e)

    # create the parsed document
    documents.append(
        ParsedDocument(
            source=source,
            identifier=identifier,
            representations=representations,
            success=True,
        )
    )

    return documents
