"""
Generic Tika parser.
"""

# imports
from typing import List, Optional

# packages
import alea_preprocess
from alea_markdown.normalizer import MarkdownNormalizer

# project
from kl3m_data.logger import LOGGER
from kl3m_data.parsers.parser_types import (
    ParsedDocument,
    ParsedDocumentRepresentation,
)
from kl3m_data.utils.tika_utils import get_html_contents

# default max size to pass to tika (in bytes)
DEFAULT_MAX_SIZE = 1024 * 1024 * 4  # 4 MB

# default tika url
DEFAULT_TIKA_URL = "http://localhost:9998/"


def parse_tika(
    object_content: bytes,
    object_source: Optional[str] = None,
    object_url: Optional[str] = None,
    tika_url: Optional[str] = None,
) -> List[ParsedDocument]:
    """
    Parse the content of an object using Tika.

    Args:
        object_content (bytes): Object content.
        object_source (str): Object source.
        object_url (str): Object URL.
        tika_url (str): Tika URL.

    Returns:
        List[ParsedDocument]: Parsed documents.
    """
    documents = []

    if 0 < len(object_content) < DEFAULT_MAX_SIZE:
        if tika_url is None:
            tika_url = DEFAULT_TIKA_URL

        # get the content
        try:
            tika_docs = get_html_contents(tika_url, object_content)

            # add the documents
            for doc in tika_docs:
                doc_markdown = (
                    alea_preprocess.parsers.html.conversion.extract_buffer_markdown(
                        doc, output_links=False, output_images=False
                    )
                )
                doc_markdown = MarkdownNormalizer().normalize(doc_markdown)
                documents.append(
                    ParsedDocument(
                        source=object_source,
                        identifier=object_url,
                        representations={
                            "text/markdown": ParsedDocumentRepresentation(
                                content=doc_markdown,
                                mime_type="text/markdown",
                            ),
                        },
                        success=True,
                    )
                )
        except Exception as e:
            LOGGER.error("Error parsing content with Tika: %s", e)

    return documents
