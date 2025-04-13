"""
Identify raw files of a certain MIME type or extension from a source
datasets under documents/ and then put the raw file to the corresponding
folder under s3://data.kl3m.ai/raw/{dataset_id}
"""

# imports
import argparse
import base64
import json
import logging
import mimetypes
import zlib
from pathlib import Path

# package

# project
from kl3m_data.utils.s3_utils import iter_prefix, get_object_bytes, put_object_bytes, get_s3_client

ALLOWED_MIME_TYPES = ("application/pdf", "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
              "application/vnd.openxmlformats-officedocument.presentationml.presentation",
              "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
              )

ALLOWED_EXTENSIONS = (".pdf", ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx")


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Extract raw files from datasets")
    parser.add_argument(
        "dataset",
        type=str,
        help="The dataset to extract raw files from",
    )
    args = parser.parse_args()

    # Get S3 client
    s3_client = get_s3_client()

    # Iterate over all objects in the dataset
    for document_key in iter_prefix(s3_client, "data.kl3m.ai", f"documents/{args.dataset}"):
        # Get the object bytes
        document = json.loads(get_object_bytes(s3_client, "data.kl3m.ai", document_key))
        mime_type = document.get("format", "application/octet-stream")
        extension = Path(document.get("identifier", "unknown.bin")).suffix

        # check for pdf, doc, docx, ppt, pptx, xls, xlsx via mime type and extension
        if mime_type.lower() in ALLOWED_MIME_TYPES or extension.lower() in ALLOWED_EXTENSIONS:
            # zlib/base64 decode the content
            content = zlib.decompress(base64.b64decode(document.get("content")))  # noqa

            # get proper extension with mime type
            target_extension = extension or mimetypes.guess_extension(mime_type, strict=True)
            if target_extension is None:
                target_extension = ".bin"

            # target path is s3://data.kl3m.ai/raw/{dataset_id}/{document.get("id")}
            target_file_name = f"{document.get('id')}"
            if not target_file_name.lower().endswith(target_extension):
                target_file_name += target_extension
            target_key = f"raw/{args.dataset}/{target_file_name}"

            # now put it into place
            put_object_bytes(s3_client, "data.kl3m.ai", target_key, content)


