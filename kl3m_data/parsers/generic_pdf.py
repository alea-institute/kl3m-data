"""
Generic PDF parsing
"""

# imports
import subprocess
import tempfile
from pathlib import Path
from typing import List, Optional

# packages
import alea_preprocess
import pytesseract
from pypdfium2 import PdfDocument
from PIL import Image

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


def convert_pdf_to_tiff(content: bytes) -> Path:
    """
    Run pdftocairo to convert a pdf to a tiff
    in a temp folder.

    Args:
        content (bytes): PDF content.

    Returns:
        Path: Path to the tiff files
    """
    # create a temp dir
    with tempfile.TemporaryDirectory(delete=False) as output_dir:
        # write the pdf to a file
        pdf_path = Path(output_dir) / "file.pdf"
        with open(pdf_path, "wb") as output_file:
            output_file.write(content)

        # run pdftocairo to convert the pdf to a png
        command = [
            "pdftocairo",
            "-tiff",
            "-tiffcompression",
            "lzw",
            "-gray",
            "-antialias",
            "gray",
            pdf_path,
        ]
        LOGGER.info("Converting PDF to TIFF(s): %s", pdf_path)
        subprocess.run(command, cwd=output_dir)

        # return the path to the tiffs
        return Path(output_dir)


def parse_poppler_tesseract(
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
    # convert the pdf to tiff
    tiff_dir = convert_pdf_to_tiff(content)

    # get the tiff files
    tiff_files = list(tiff_dir.glob("*.tif"))

    # get the text from each tiff file
    text = ""
    for page_number, tiff_file in enumerate(tiff_files):
        LOGGER.info("Running tesseract on page %d: %s", page_number, tiff_file)
        image = Image.open(tiff_file)
        text += pytesseract.image_to_string(image, timeout=600) + "\n\n"

    # delete the folder
    for path_file in tiff_dir.rglob("*"):
        try:
            path_file.unlink()
        except Exception as e:
            LOGGER.error("Error deleting file %s: %s", path_file, e)
            pass
    try:
        tiff_dir.rmdir()
    except Exception as e:
        LOGGER.error("Error deleting folder %s: %s", tiff_dir, e)
        pass

    # return the parsed document
    return [
        ParsedDocument(
            source=source,
            identifier=identifier,
            success=True,
            representations={
                "text/plain": ParsedDocumentRepresentation(
                    content=text,
                    mime_type="text/plain",
                ),
            },
        )
    ]


def parse_pdfium_tesseract(
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
    # get a pdfium object from the bytes
    pdf_doc: Optional[PdfDocument] = None
    try:
        # load doc and metadata
        pdf_doc = PdfDocument(content, autoclose=True)
        pdf_metadata = pdf_doc.get_metadata_dict()

        # collect text
        pdf_text = ""
        for pdf_page in pdf_doc:
            rendered_page = pdf_page.render(grayscale=True, optimize_mode="print")
            page_image = rendered_page.to_pil()
            page_text = pytesseract.image_to_string(page_image, timeout=600)

            if isinstance(page_text, str) and len(page_text.strip()) > 0:
                pdf_text += page_text.strip() + "\n\n"

        # strip
        pdf_text = pdf_text.strip()

        # return the parsed document
        return [
            ParsedDocument(
                source=source,
                identifier=identifier,
                success=True,
                representations={
                    "text/plain": ParsedDocumentRepresentation(
                        content=pdf_text,
                        mime_type="text/plain",
                    ),
                },
                metadata=pdf_metadata,
            )
        ]
    except Exception as e:
        LOGGER.error("Error creating pdfium document: %s", e)
        return []
    finally:
        # make sure to clean up this stuff
        try:
            if pdf_doc:
                pdf_doc.close()
        except Exception:
            pass
