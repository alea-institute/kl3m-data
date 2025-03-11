"""
Generic PDF parsing
"""

# imports
import subprocess
from typing import List, Optional

# packages
import alea_preprocess
from alea_markdown.normalizer import MarkdownNormalizer

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
        markdown = MarkdownNormalizer().normalize(markdown)
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

    # filter empty documents
    documents = [
        d
        for d in documents
        if any(
            r.content is not None and len(r.content.strip()) > 0
            for r in d.representations.values()
        )
    ]

    # try again with tesseract if we failed
    if len(documents) == 0:
        documents.extend(
            parse_poppler_tesseract(
                content=content,
                source=source,
                identifier=identifier,
            )
        )

    return documents


def parse_poppler_tesseract(
    content: bytes,
    source: Optional[str] = None,
    identifier: Optional[str] = None,
) -> List[ParsedDocument]:
    """
    Parse the document data using pdftocairo and tesseract,
    piping output to stdout and using stdin wherever possible to minimize file I/O.

    Args:
        content (bytes): Document content.
        source (str): Document source.
        identifier (str): Document identifier.

    Returns:
        List[ParsedDocument]: Parsed document
    """
    # Get the number of pages in the PDF using pdfinfo through pipes
    pdfinfo_process = subprocess.Popen(
        ["pdfinfo", "-"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    pdfinfo_output, pdfinfo_error = pdfinfo_process.communicate(input=content)

    if pdfinfo_process.returncode != 0:
        LOGGER.error("Error getting page count: %s", pdfinfo_error.decode())
        return []

    # Parse page count
    page_count = 0
    for line in pdfinfo_output.decode().split("\n"):
        if line.startswith("Pages:"):
            page_count = int(line.split(":")[1].strip())
            break

    if page_count == 0:
        LOGGER.error("Could not determine page count")
        return []

    LOGGER.info("Processing PDF with %d pages: %s", page_count, identifier)

    all_text = ""
    # Process each page individually
    for page_num in range(1, page_count + 1):
        LOGGER.info(
            "Processing page %d/%d (%d tokens total)",
            page_num,
            page_count,
            len(all_text.split()),
        )

        try:
            # Step 1: Use pdftocairo to convert PDF page to PNG, output to stdout
            pdftocairo_cmd = [
                "pdftocairo",
                "-png",  # Output format: PNG
                "-gray",  # Grayscale output
                "-antialias",
                "gray",
                "-f",
                str(page_num),  # First page
                "-l",
                str(page_num),  # Last page
                "-singlefile",  # Don't append page numbers to filename
                "-",  # Read from stdin
                "-",  # Write to stdout
            ]

            # Start pdftocairo process
            pdftocairo_process = subprocess.Popen(
                pdftocairo_cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            # Feed PDF content and get PNG data
            png_data, pdftocairo_error = pdftocairo_process.communicate(input=content)

            if pdftocairo_process.returncode != 0:
                LOGGER.error(
                    "Error converting page %d to PNG: %s",
                    page_num,
                    pdftocairo_error.decode(),
                )
                continue

            # Step 2: Pass PNG data to tesseract via stdin
            tesseract_cmd = [
                "tesseract",
                "stdin",  # Read from stdin
                "stdout",  # Write to stdout
                "-l",
                "eng",  # Language
                "--psm",
                "1",  # Automatic page segmentation with OSD
            ]

            tesseract_process = subprocess.Popen(
                tesseract_cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            # Feed PNG data to tesseract
            page_text_bytes, tesseract_error = tesseract_process.communicate(
                input=png_data
            )

            if tesseract_process.returncode != 0:
                LOGGER.error(
                    "Error OCRing page %d: %s", page_num, tesseract_error.decode()
                )
                continue

            # Add the text output
            page_text = page_text_bytes.decode()
            all_text += page_text + "\n\n"

        except Exception as e:
            LOGGER.error("Unexpected error processing page %d: %s", page_num, e)
            continue

    # Return the parsed document
    return [
        ParsedDocument(
            source=source,
            identifier=identifier,
            success=True,
            representations={
                "text/plain": ParsedDocumentRepresentation(
                    content=all_text,
                    mime_type="text/plain",
                ),
            },
        )
    ]
