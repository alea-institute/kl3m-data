"""
Parse EU OJ XML documents by applying the XSL transformation and then
returning the text and Markdown representations of the HTML output.

TODO: Still need to finish redone EU OJ source and review this after
"""

# imports
from pathlib import Path
from typing import List, Optional

# packages
import lxml.etree

# project
from kl3m_data.logger import LOGGER
from kl3m_data.parsers import generic_html
from kl3m_data.parsers.parser_types import ParsedDocument

# load the XSL transformer
EU_OJ_XSL = lxml.etree.XSLT(
    lxml.etree.parse(Path(__file__).parent / "xsl" / "eu_oj.xsl")
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
    LOGGER.info("Parsing EU OJ XML document: %s", identifier)

    # try to parse and nicely format the xml
    try:
        xml_doc = lxml.etree.fromstring(content)
        html_doc = EU_OJ_XSL(xml_doc)

        import uuid

        x = uuid.uuid4()
        with open(f"/tmp/euoj_{x}.xml", "wb") as output_file:
            output_file.write(content)

        with open(f"/tmp/euoj_{x}.html", "wt") as output_file:
            output_file.write(lxml.etree.tostring(html_doc).decode())

        return generic_html.parse(
            lxml.etree.tostring(html_doc),
            source=source,
            identifier=identifier,
        )
    except Exception as e:  # pylint: disable=broad-except
        LOGGER.error("Error parsing EU OJ XML: %s", e)
        return []
