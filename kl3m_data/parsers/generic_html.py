"""
Generic HTML parsing
"""

# imports
from typing import Dict, List, Optional

# packages
from alea_markdown.auto_parser import AutoParser
from alea_markdown.base.parser_config import ParserConfig
from alea_markdown.lxml_parser import LXMLHTMLParser
from alea_markdown.normalizer import MarkdownNormalizer
from alea_markdown.regex_parser import RegexHTMLParser

# project
from kl3m_data.logger import LOGGER
from kl3m_data.parsers.parser_types import (
    ParsedDocument,
    ParsedDocumentRepresentation,
)

# Candidate parsers in order of preference.  When several candidates recover (nearly) the
# same content, the earliest one wins.
#
# The lxml parser is last on purpose: alea-markdown 0.1.x's LXMLHTMLParser joins the
# fragments of inline elements (<span>, <em>, <a>, ...) and the children of <div> elements
# with " ", and ends each <div> with a single "\n".  On markup such as the eCFR renderer's
# <span class="paren">(</span>a<span class="paren">)</span> this produces "( a )", and
# paragraphs wrapped in <div>s lose their blank-line separation.  AutoParser routes
# 128KB-2MB documents to the lxml parser, so it is ranked after the regex parser as well.
PARSER_PREFERENCE = ("regex", "auto", "lxml")

# A preferred candidate is kept unless another candidate recovers more than this fraction
# of additional content.
CONTENT_TOLERANCE = 0.01


def strip_link_destinations(text: str) -> str:
    """
    Remove Markdown inline link/image destinations, i.e. the "(...)" in "[text](...)",
    keeping the link text.  Parentheses inside the destination are balanced, so
    eCFR-style URLs such as "/section-1.1#p-1.1(a)(1)" are removed completely.

    Args:
        text (str): Markdown text.

    Returns:
        str: Markdown text without link destinations.
    """
    output = []
    position = 0
    length = len(text)
    while position < length:
        start = text.find("](", position)
        if start < 0:
            output.append(text[position:])
            break

        # keep everything up to and including the closing bracket
        output.append(text[position : start + 1])

        # skip the balanced (...) destination
        cursor = start + 2
        depth = 1
        while cursor < length and depth > 0:
            character = text[cursor]
            if character == "(":
                depth += 1
            elif character == ")":
                depth -= 1
            elif character == "\n":
                break
            cursor += 1

        if depth > 0:
            # not a well-formed destination; keep it as text
            output.append(text[start + 1 : cursor])
        position = cursor

    return "".join(output)


def content_length(text: Optional[str]) -> int:
    """
    Measure how much document content a candidate conversion recovered.

    The measure counts alphanumeric characters outside of link destinations, so it is
    invariant to whitespace, Markdown punctuation, and link URLs.  A whitespace-sensitive
    measure (e.g., ``len(text.split())``) rewards converters that insert spurious spaces:
    "( a )" is three words while "(a)" is one.

    Args:
        text (Optional[str]): Candidate text.

    Returns:
        int: Number of content characters.
    """
    if not text:
        return 0
    return sum(1 for character in strip_link_destinations(text) if character.isalnum())


def select_candidate(
    candidates: Dict[str, Optional[str]],
    preference: tuple = PARSER_PREFERENCE,
    tolerance: float = CONTENT_TOLERANCE,
) -> Optional[str]:
    """
    Select the conversion to keep from several candidate conversions.

    Returns the most-preferred candidate whose content length is within ``tolerance`` of
    the largest content length, or None if no candidate recovered any content.

    Args:
        candidates (Dict[str, Optional[str]]): Candidate texts keyed by parser name.
        preference (tuple): Parser names in order of preference.
        tolerance (float): Fraction of content a preferred candidate may lack.

    Returns:
        Optional[str]: The selected parser name.
    """
    lengths = {name: content_length(text) for name, text in candidates.items()}
    max_length = max(lengths.values(), default=0)
    if max_length == 0:
        return None

    ordered = [name for name in preference if name in lengths] + [
        name for name in lengths if name not in preference
    ]
    for name in ordered:
        if lengths[name] > 0 and lengths[name] >= (1.0 - tolerance) * max_length:
            return name

    # unreachable: the candidate with max_length always qualifies
    return None


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
        html_content = content.decode("utf-8")

        # shared config
        parser_config = ParserConfig(
            output_links=False,
            output_images=False,
        )

        # get all three candidate conversions: regex, auto (markdownify/lxml/regex), lxml
        parser_classes = {
            "regex": RegexHTMLParser,
            "auto": AutoParser,
            "lxml": LXMLHTMLParser,
        }
        candidates: Dict[str, Optional[str]] = {}
        for name, parser_class in parser_classes.items():
            try:
                candidates[name] = parser_class(parser_config).parse(html_content)
            except Exception as e:  # pylint: disable=broad-except
                LOGGER.error("Error parsing with %s: %s", name, e)
                candidates[name] = None

        # select the conversion to keep
        selected = select_candidate(candidates)
        text = candidates[selected] if selected else None
        if selected:
            LOGGER.info("Using text from %s parser for %s", selected, identifier)

        if text:
            # normalize
            normalizer = MarkdownNormalizer()
            markdown = normalizer.normalize(text)

            # create the parsed document
            documents.append(
                ParsedDocument(
                    source=source,
                    identifier=identifier,
                    representations={
                        "text/markdown": ParsedDocumentRepresentation(
                            content=markdown,
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
