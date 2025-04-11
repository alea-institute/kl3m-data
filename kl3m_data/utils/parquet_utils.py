# import
import base64
import html
import zlib

# packages
import pyarrow
import pyarrow.parquet
from tokenizers import Tokenizer

# load default tokenizer
DEFAULT_TOKENIZER_NAME = "alea-institute/kl3m-004-128k-cased"
DEFAULT_TOKENIZER = Tokenizer.from_pretrained(DEFAULT_TOKENIZER_NAME)

# load default token type
DEFAULT_TOKEN_TYPE = pyarrow.uint32()


def get_document_schema() -> pyarrow.Schema:
    """
    Get the schema for the document table.

    Returns:
        pyarrow.Schema: The schema for the document table.
    """
    schema = pyarrow.schema(
        [
            # source
            pyarrow.field("identifier", pyarrow.string()),
            pyarrow.field(
                "representations",
                pyarrow.map_(pyarrow.string(), pyarrow.list_(DEFAULT_TOKEN_TYPE)),
            ),
        ]
    )
    return schema


def serialize_document(document: dict, schema=get_document_schema()) -> bytes | None:
    """
    Serialize a document to a pyarrow.Table.

    Args:
        document (dict): The document to serialize.
        schema (pyarrow.Schema): The schema for the document table.

    Returns:
        bytes: The serialized document as parquet bytes.
    """
    # get content token map
    token_map = []
    for content_type, record in document["representations"].items():
        try:
            # decode content
            content = zlib.decompress(base64.b64decode(record.get("content"))).decode(  # type: ignore
                "utf-8"
            )

            # if the content type is text/markdown or text/plain, unescape html
            # to get the actual content
            if content_type in ["text/markdown", "text/plain"]:
                if "&nbsp;" in content or "&#160;" in content:
                    try:
                        content = html.unescape(content)
                    except Exception as e:
                        print(f"Error while unescaping HTML: {e}")
                        continue

            # encode content
            token_map.append((content_type, DEFAULT_TOKENIZER.encode(content).ids))
        except Exception as e:
            print(f"Error while encoding content: {e}")

    # create the table
    if len(token_map) < 1:
        return None

    table = pyarrow.table(
        {
            "identifier": [document["identifier"]],
            "representations": [token_map],  # Wrap token_map in a list
        },
        schema=schema,
    )

    # write parquet bytes to buffer
    output_stream = pyarrow.BufferOutputStream()
    pyarrow.parquet.write_table(
        table, output_stream, write_statistics=False, store_schema=False
    )

    # return buffer as bytes
    return zlib.compress(output_stream.getvalue().to_pybytes())


def deserialize_document_bytes(document_bytes: bytes) -> dict:
    """
    Deserialize a document from bytes.

    Args:
        document_bytes (bytes): The document bytes to deserialize.

    Returns:
        dict: The deserialized document.
    """
    # read table from bytes
    table = pyarrow.parquet.read_table(
        pyarrow.BufferReader(zlib.decompress(document_bytes)),
        schema=get_document_schema(),
    )

    # get the document as a dictionary
    return {
        "identifier": table["identifier"][0].as_py(),
        "representations": dict(table["representations"][0].as_py()),
    }
