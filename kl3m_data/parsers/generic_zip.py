"""
Parse generic ZIP files by iterating through members and parsing them.
"""

# imports
import io
import zipfile
from typing import List, Optional

# packages
import alea_preprocess

# project
from kl3m_data.logger import LOGGER
from kl3m_data.parsers import (
    eu_oj_xml,
    generic_html,
    generic_json,
    generic_pdf,
    generic_xml,
)
from kl3m_data.parsers.parser_types import (
    ParsedDocument,
    ParsedDocumentRepresentation,
)


def parse_zip_member(
    object_content: bytes,
    object_source: Optional[str] = None,
    object_format: Optional[str] = None,
    object_url: Optional[str] = None,
) -> List[ParsedDocument]:
    """
    Parse the content of a ZIP member, which is similar to
    how parse_content works, but more limited and does not
    recurse into nested ZIP files.

    Returns:
        List[ParsedDocument]: Parsed documents.
    """
    # parse the document
    documents = []
    if object_format in ("application/pdf",):
        documents.extend(generic_pdf.parse(object_content))
    elif object_format in (
        "text/html",
        "application/xhtml+xml",
    ):
        documents.extend(generic_html.parse(object_content))
    elif object_format in (
        "text/xml",
        "application/xml",
    ):
        if object_source == "https://publications.europa.eu/":
            documents.extend(
                eu_oj_xml.parse(
                    object_content,
                    source=object_source,
                    identifier=object_url,
                )
            )
        else:
            documents.extend(generic_xml.parse(object_content))
    elif object_format in ("application/json",):
        documents.extend(generic_json.parse(object_content))
    elif object_format in ("text/markdown",):
        documents.append(
            ParsedDocument(
                source=object_source,
                identifier=object_url,
                representations={
                    "text/markdown": ParsedDocumentRepresentation(
                        content=object_content.decode(),
                        mime_type="text/markdown",
                    ),
                },
                success=True,
            )
        )
    elif object_format in ("text/plain",):
        documents.append(
            ParsedDocument(
                source=object_source,
                identifier=object_url,
                representations={
                    "text/plain": ParsedDocumentRepresentation(
                        content=object_content.decode(),
                        mime_type="text/plain",
                    ),
                },
                success=True,
            )
        )
    else:
        LOGGER.info(
            "No parser found for object format=%s, key=%s", object_format, object_url
        )

    return documents


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
    LOGGER.info("Parsing ZIP document: %s", identifier)

    documents = []

    # load the zip file from the buffer
    with zipfile.ZipFile(io.BytesIO(content)) as zip_file:
        # iterate through the members
        for member in zip_file.infolist():
            # read the member content
            member_content = zip_file.read(member.filename)

            # we need to determine the object format ourselves
            content_info = alea_preprocess.io.fs.file_info.get_file_info_from_buffer(
                member_content
            )
            object_format = content_info.media_type

            # extend by parsing directly
            documents.extend(
                parse_zip_member(
                    member_content,
                    object_source=source,
                    object_format=object_format,
                    object_url=identifier + "/" + member.filename,
                )
            )

    return documents
