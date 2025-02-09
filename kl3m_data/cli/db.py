"""
Build and manage the sqlite db for the document store.
"""

# imports
import argparse
import json

# packages
import tqdm

# project
from kl3m_data.db.documents import DocumentStore
from kl3m_data.logger import LOGGER
from kl3m_data.utils.s3_utils import (
    get_s3_client,
    iter_prefix,
    get_object_bytes,
    check_object_exists,
)


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Insert documents from a JSON file (local or S3) into the SQLite document store."
    )
    parser.add_argument(
        "input_path",
        help="Local file path or S3 URL (s3://bucket/prefix) to a JSON file containing documents.",
    )
    parser.add_argument(
        "--db",
        default="documents.db",
        help="Path to the SQLite database file (default: documents.db).",
    )
    return parser.parse_args()


def insert_local(store: DocumentStore, input_path: str) -> None:
    """
    Insert a document from a local JSON file into the document store.

    Args:
        store (DocumentStore): Instance of DocumentStore.
        input_path (str): Path to the JSON file.
    """
    try:
        with open(input_path, "rt", encoding="utf-8") as input_file:
            data = json.load(input_file)
        # Use the file path as the kl3m_id for local documents.
        if store.insert_document(input_path, data):
            store.logger.info("Successfully inserted document from '%s'.", input_path)
        else:
            store.logger.error("Failed to insert document from '%s'.", input_path)
    except Exception as e:
        store.logger.exception("Error reading local file '%s': %s", input_path, e)


def insert_s3(store: DocumentStore, input_path: str) -> None:
    """
    Insert documents from S3 JSON files into the document store.

    Args:
        store (DocumentStore): Instance of DocumentStore.
        input_path (str): S3 URL in the format s3://bucket/prefix.
    """
    try:
        s3_client = get_s3_client()

        # Split the S3 URL (s3://bucket/prefix).
        s3_url = input_path[len("s3://") :]
        bucket, prefix = s3_url.split("/", 1)
        document_paths = iter_prefix(s3_client, bucket, prefix)
        progress_bar = tqdm.tqdm(document_paths, desc="Processing S3 documents")

        num_docs = 0
        for document_path in progress_bar:
            try:
                # get id and check if present
                kl3m_id = f"s3://{bucket}/{document_path}"
                if store.get_document_by_id(kl3m_id):
                    LOGGER.info(
                        "Document '%s' already exists in the database. Skipping.",
                        kl3m_id,
                    )
                    continue

                # get raw doc and parse
                document_bytes = get_object_bytes(s3_client, bucket, document_path)
                data = json.loads(document_bytes)

                # check if representations/ version exists
                representation_path = "representations/" + "/".join(
                    document_path.split("/")[1:]
                )
                data["processed"] = int(
                    check_object_exists(s3_client, bucket, representation_path)
                )
                if data["processed"] > 0:
                    # download it
                    data["num_representations"] = 0
                    data["num_tokens"] = 0

                    try:
                        representation_bytes = get_object_bytes(
                            s3_client, bucket, representation_path
                        )
                        for rep_doc in json.loads(representation_bytes).get(
                            "documents", []
                        ):
                            for rep_mime_type, rep_rep in rep_doc.get(
                                "representations", {}
                            ).items():
                                data["num_representations"] += 1
                                for rep_tokens in rep_rep["tokens"].values():
                                    data["num_tokens"] += len(rep_tokens)
                    except Exception as ex:
                        store.logger.exception(
                            "Failed to load representations for '%s': %s",
                            representation_path,
                            ex,
                        )
                        pass

                if store.insert_document(kl3m_id, data):
                    num_docs += 1
                else:
                    store.logger.error(
                        "Failed to insert document from '%s'.", document_path
                    )
            except Exception as ex:
                store.logger.exception(
                    "Error processing document '%s': %s", document_path, ex
                )
                continue

        store.logger.info("Inserted %d documents from S3.", num_docs)
    except Exception as e:
        store.logger.exception(
            "Error initializing S3 client or processing S3 path '%s': %s", input_path, e
        )


def main() -> None:
    """
    Main entry point for the DocumentStore CLI.
    """
    args = parse_arguments()
    db_path = args.db
    input_path = args.input_path

    store = DocumentStore(db_path=db_path)

    try:
        if input_path.startswith("s3://"):
            insert_s3(store, input_path)
        else:
            insert_local(store, input_path)
    except Exception as e:
        store.logger.exception("Error during document insertion: %s", e)
        print(f"Error: {e}")
    finally:
        store.close()


if __name__ == "__main__":
    main()
