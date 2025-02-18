"""
Generic object parser.
"""

import mimetypes

# imports
from typing import List, Optional

# project
from kl3m_data.logger import LOGGER
from kl3m_data.parsers import (
    eu_oj_xml,
    generic_html,
    generic_json,
    generic_pdf,
    generic_xml,
    generic_zip,
)
from kl3m_data.parsers.generic_tika import parse_tika
from kl3m_data.parsers.parser_types import (
    ParsedDocument,
    ParsedDocumentRepresentation,
)
from kl3m_data.utils.uu_utils import uudecode

# packages


# file names to avoid
IGNORE_FILENAMES = (
    "Thumbs.db",
    ".DS_Store",
    "desktop.ini",
)

# extensions to avoid
IGNORE_EXTENSIONS = (
    ".ini",
    ".db",
    ".DS_Store",
)

# mime types to ignore
IGNORE_MIME_TYPES = ()


def patch_source_metadata(object_key: str, object_data: dict) -> dict:
    """
    Patch metadata, e.g., normalize source or fix double slashes (//) in S3 identifiers.

    Args:
        object_key (str): Object key.
        object_data (dict): Object data.

    Returns:
        dict: Patched object data.
    """

    # check if we are missing the source from key
    if "source" not in object_data or object_data["source"] is None:
        if object_key.startswith("documents/dockets/"):
            # set source
            object_data["source"] = "https://archive.org/download/federal-court-dockets"

        if object_key.startswith("documents/fdlp/"):
            # set markdown format
            object_data["source"] = "https://permanent.fdlp.gov/"

        if object_key.startswith("documents/eu_oj/"):
            # set source
            object_data["source"] = "https://publications.europa.eu/"

    if object_key.startswith("documents/uspto/"):
        # set markdown format
        object_data["format"] = "text/markdown"

    return object_data


def parse_content(
    object_content: bytes,
    object_source: Optional[str] = None,
    object_format: Optional[str] = None,
    object_url: Optional[str] = None,
    tika_url: Optional[str] = None,
) -> List[ParsedDocument]:
    """
    Parse the content of an object from S3 or a nested object
    like a member from within a ZIP file.

    Args:
        object_content (bytes): Object content.
        object_source (str): Object source.
        object_format (str): Object format.
        object_url (str): Object URL.
        tika_url (str): Tika URL.

    Returns:
        List[ParsedDocument]: Parsed documents.
    """
    # strip initial <PDF> tag if present
    if object_content.startswith(b"<PDF>"):
        object_content = object_content[5:].lstrip()
        # remove from end
        if object_content.endswith(b"</PDF>"):
            object_content = object_content[:-6].rstrip()

    # check if the content is uuencoded here
    if object_format == "application/uuencode":
        # decode the content
        try:
            object_name, object_content = uudecode(object_content)
            mime_type, _ = mimetypes.guess_type(object_name)
            if mime_type:
                object_format = mime_type
        except Exception as e:
            LOGGER.error("Failed to decode uuencoded content: %s", e)
            return []

    # check for begin ### next
    if object_content.startswith(b"begin"):
        # decode the content
        try:
            if object_content[6:9].decode().isnumeric():
                object_name, object_content = uudecode(object_content)
                mime_type, _ = mimetypes.guess_type(object_name)
                if mime_type:
                    object_format = mime_type
        except Exception as e:
            LOGGER.error("Failed to decode uuencoded content: %s", e)
            return []

    # parse the document
    documents = []
    if object_format in ("application/zip",):
        documents.extend(
            generic_zip.parse(
                content=object_content,
                source=object_source,
                identifier=object_url,
            )
        )
    elif object_format in ("application/pdf",):
        documents.extend(
            generic_pdf.parse(
                content=object_content,
                source=object_source,
                identifier=object_url,
            )
        )
    elif object_format in (
        "text/html",
        "application/xhtml+xml",
    ):
        documents.extend(
            generic_html.parse(
                content=object_content,
                source=object_source,
                identifier=object_url,
            )
        )
    elif object_format in (
        "text/xml",
        "application/xml",
    ):
        if object_source == "https://publications.europa.eu/":
            documents.extend(
                eu_oj_xml.parse(
                    content=object_content,
                    source=object_source,
                    identifier=object_url,
                )
            )
        else:
            documents.extend(
                generic_xml.parse(
                    content=object_content,
                    source=object_source,
                    identifier=object_url,
                )
            )
    elif object_format in ("application/json",):
        documents.extend(
            generic_json.parse(
                content=object_content,
                source=object_source,
                identifier=object_url,
            )
        )
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
        # double-check for <html or <!doctype tags here
        if b"<html" in object_content.lower() or b"<!doctype" in object_content.lower():
            documents.extend(
                generic_html.parse(
                    content=object_content,
                    source=object_source,
                    identifier=object_url,
                )
            )
        else:
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
        if object_format in IGNORE_MIME_TYPES:
            LOGGER.info(
                "Ignoring object with format=%s, key=%s", object_format, object_url
            )
            return []

        # try to use tika as a final fallback
        try:
            documents.extend(
                parse_tika(
                    object_content=object_content,
                    object_source=object_source,
                    object_url=object_url,
                    tika_url=tika_url,
                )
            )
        except Exception as e:
            LOGGER.error("Failed to parse content with Tika: %s", e)

    return documents
