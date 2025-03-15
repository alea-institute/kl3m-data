"""
Centralized parsing logic for all formats and sources.
"""

# imports
import base64
import json
import zlib
from typing import List, Optional

# packages
import alea_preprocess
import boto3

# project
from kl3m_data.logger import LOGGER
from kl3m_data.parsers.filters import verdate
from kl3m_data.parsers.generic_object import parse_content, patch_source_metadata
from kl3m_data.parsers.parser_types import ParsedDocument
from kl3m_data.utils.s3_utils import get_object_bytes

# default filters
DEFAULT_FILTERS = [
    verdate,
]

# default tokenizers
DEFAULT_TOKENIZERS = [
    "alea-institute/kl3m-004-128k-cased",
]


def get_object_data(
    client: boto3.client,
    bucket: str,
    key: str,
) -> Optional[dict]:
    """
    Get the object data from an S3 object and normalize it
    prior to parsing.

    Args:
        client (boto3.client): S3 client.
        bucket (str): Bucket name.
        key (str): Prefix.

    Returns:
        dict: Object data.
    """
    # get the object bytes
    object_bytes = get_object_bytes(client, bucket, key)

    # decode the object bytes
    object_data = json.loads(object_bytes)

    # decompress contents
    try:
        object_data["content"] = zlib.decompress(
            base64.b64decode(object_data["content"])
        )
    except Exception as e:
        raise Exception(f"Error decompressing contents: {e}")

    # check that we have >0 content length
    if len(object_data["content"].strip()) == 0:
        return None

    # get key filters for switching
    object_source = object_data["source"].lower() if "source" in object_data else None

    # get raw object format
    object_format = (
        object_data.get("format", "application/octet-stream")
        or "application/octet-stream"
    )
    object_format = object_format.split(";")[0].lower().strip()

    # get the content type from the content directly for some sources
    # if object format is just binary, then try to guess it
    if object_format == "application/octet-stream":
        content_info = alea_preprocess.io.fs.file_info.get_file_info_from_buffer(
            object_data["content"]
        )
        object_format = content_info.media_type

    # set updated source and format prior to parsing
    object_data["source"] = object_source
    object_data["format"] = object_format

    # patch metadata
    object_data = patch_source_metadata(key, object_data)

    return object_data


def postprocess_document(
    document: ParsedDocument,
    object_url: str,
) -> ParsedDocument:
    """
    Postprocess a document using a list of filters.

    Args:
        document (ParsedDocument): Parsed document.
        object_url (str): Object URL.

    Returns:
        ParsedDocument: Postprocessed document.
    """
    if document:
        # add the original uri
        document.original_uri = object_url

        # final representations
        final_representations = {}

        # tokenize the content
        for (
            representation_type,
            representation_data,
        ) in document.representations.items():
            # apply filters
            for filter_func in DEFAULT_FILTERS:
                representation_data.content = filter_func.filter_buffer(
                    representation_data.content
                )

            # check if empty
            if len(representation_data.content.strip()) == 0:
                continue

            for tokenizer in DEFAULT_TOKENIZERS:
                # encode and set
                representation_data.tokens[tokenizer] = (
                    alea_preprocess.algos.tokenizers.encode_str(
                        tokenizer,
                        representation_data.content,
                    )
                )

            # add to final representations
            final_representations[representation_type] = representation_data

        # set the final representations
        document.representations = final_representations

    # return the finalized document
    return document


def get_output_key(input_key: str) -> str:
    """
    Get the output location key from the input location key.

    Args:
        input_key (str): The input location key.

    Returns:
        str: The output location key.
    """
    # Import here to avoid circular imports
    from kl3m_data.utils.s3_utils import get_representation_key

    # Use the utility function to convert to representation key
    return get_representation_key(input_key)


def parse_object(
    client: boto3.client,
    bucket: str,
    key: str,
    max_size: Optional[int] = None,
) -> List[ParsedDocument]:
    """
    Parse an object from S3.

    Args:
        client (boto3.client): S3 client.
        bucket (str): Bucket name.
        key (str): Prefix.
        max_size (Optional[int]): Maximum file size in bytes to process.

    Returns:
        List[ParsedDocument]: Parsed documents.
    """
    # get the full s3 uri
    object_uri = "s3://" + bucket + "/" + key

    # get the object data
    try:
        object_data = get_object_data(client, bucket, key)
    except Exception as e:
        LOGGER.error("Error getting object data for %s: %s", object_uri, e)
        return []

    # check that we have object data
    if object_data is None:
        return []

    # check that we have content
    if "content" not in object_data:
        LOGGER.error("No content found for %s", object_uri)
        return []

    # check that the length of the content is >0
    if len(object_data["content"].strip()) == 0:
        LOGGER.error("Empty content found for %s", object_uri)
        return []

    # check size
    if max_size is not None and len(object_data["content"]) > max_size:
        LOGGER.error(
            "Content is larger than current max size setting for %s (%d > %d)",
            object_uri,
            len(object_data["content"]),
            max_size,
        )
        return []

    # parse the content
    documents = parse_content(
        object_content=object_data["content"],
        object_source=object_data.get("source"),
        object_format=object_data.get("format"),
        object_url=object_uri,
    )

    # postprocess the document
    final_documents = []
    for document in documents:
        try:
            document = postprocess_document(document, object_uri)

            # skip docs without any representations
            if len(document.representations) == 0:
                continue

            # add the document to the final list if it passed
            final_documents.append(document)
        except Exception as e:
            LOGGER.error("Error postprocessing document %s: %s", object_uri, e)

    return final_documents
