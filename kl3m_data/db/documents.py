"""
DocumentStore Module

This module provides a simple database/Data Access Layer (DAL) interface
for storing and retrieving documents with normalized subject fields.
It supports both local and S3-based JSON document sources.

Usage:
    python document_store.py <input_path> [--db <db_path>]

For more details, run:
    python document_store.py --help
"""

# imports
import base64
import sqlite3
import json
import sys
import traceback
import zlib
from typing import List, Dict, Any

# packages

# project
from kl3m_data.logger import get_logger


class DocumentStore:
    """
    DocumentStore provides a simple interface to a SQLite database for storing documents,
    their metadata, and normalized subjects.
    """

    def __init__(self, db_path: str = "documents.db") -> None:
        """
        Initialize the DocumentStore and set up the database.

        Args:
            db_path (str): Path to the SQLite database file.
        """
        self.logger = get_logger()
        self.logger.info("Initializing DocumentStore with database path: %s", db_path)

        try:
            self.conn = sqlite3.connect(db_path)
            self.conn.row_factory = sqlite3.Row  # rows as dict-like objects
            self._set_pragmas()
            self._create_tables()
            self.logger.info("DocumentStore initialized successfully.")
        except sqlite3.Error as e:
            self.logger.exception("Error initializing database: %s", e)
            traceback.print_exc()
            sys.exit(1)

    def _set_pragmas(self) -> None:
        """
        Set SQLite pragmas for improved write performance on large-scale data.
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL;")
            self.logger.debug("Set journal_mode to WAL.")
            cursor.execute("PRAGMA synchronous=NORMAL;")
            self.logger.debug("Set synchronous to NORMAL.")
            self.conn.commit()
        except sqlite3.Error as e:
            self.logger.exception("Error setting SQLite pragmas: %s", e)
            raise

    def _create_tables(self) -> bool:
        """
        Create the documents, metadata, and subjects tables if they do not already exist.

        Returns:
            bool: True if tables were created or already exist, False otherwise.
        """
        try:
            cursor = self.conn.cursor()

            # The documents table no longer stores the subject field.
            create_documents_sql = """
            CREATE TABLE IF NOT EXISTS documents (
                kl3m_id TEXT PRIMARY KEY,
                id TEXT,
                title TEXT,
                publisher TEXT,
                creator TEXT,  -- JSON-encoded list
                date TEXT,
                format TEXT,
                identifier TEXT,
                source TEXT,
                citation TEXT,
                blake2b TEXT,
                first8 BLOB,
                size INTEGER,
                dataset_id TEXT,
                processed INTEGER DEFAULT 0,
                num_representations INTEGER DEFAULT 0,
                num_tokens INTEGER DEFAULT 0
                
            );
            """
            cursor.execute(create_documents_sql)
            self.logger.info("Ensured 'documents' table exists.")

            create_metadata_sql = """
            CREATE TABLE IF NOT EXISTS metadata (
                kl3m_id TEXT,
                meta_key TEXT,
                meta_value TEXT,
                PRIMARY KEY (kl3m_id, meta_key),
                FOREIGN KEY (kl3m_id) REFERENCES documents(kl3m_id) ON DELETE CASCADE
            );
            """
            cursor.execute(create_metadata_sql)
            self.logger.info("Ensured 'metadata' table exists.")

            create_subjects_sql = """
            CREATE TABLE IF NOT EXISTS subjects (
                kl3m_id TEXT,
                subject TEXT,
                PRIMARY KEY (kl3m_id, subject),
                FOREIGN KEY (kl3m_id) REFERENCES documents(kl3m_id) ON DELETE CASCADE
            );
            """
            cursor.execute(create_subjects_sql)
            self.logger.info("Ensured 'subjects' table exists.")

            # Create necessary indexes.
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_documents_kl3m_id ON documents(kl3m_id);"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_documents_id ON documents(id);"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_documents_date ON documents(date);"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_documents_dataset ON documents(dataset_id);"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_documents_processed ON documents(processed);"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_documents_blake2b ON documents(blake2b);"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_metadata_kl3m_id ON metadata(kl3m_id);"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_metadata_key ON metadata(meta_key);"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_subjects_kl3m_id ON subjects(kl3m_id);"
            )
            self.logger.info("Created necessary indexes on tables.")
            self.conn.commit()

            return True
        except sqlite3.Error as e:
            self.logger.exception("Error creating tables or indexes: %s", e)
            return False

    def insert_document(self, kl3m_id: str, doc: Dict[str, Any]) -> bool:
        """
        Insert or update a single document in the database along with its metadata and subjects.

        The document dictionary is expected to include the following keys:
            - id, title, publisher, creator (list), date, format, identifier, source,
              citation, blake2b, size, dataset_id, processed, subject (list), extra (dict)
            - content (base64-encoded and zlib-compressed bytes)

        Args:
            kl3m_id (str): Unique identifier for the document.
            doc (Dict[str, Any]): Document data.

        Returns:
            bool: True if the document was successfully inserted/updated, False otherwise.
        """
        try:
            # Decompress the document content and extract the first 8 bytes.
            try:
                decoded_content = base64.b64decode(doc["content"])
                contents = zlib.decompress(decoded_content)  # type: ignore
            except Exception as ex:
                self.logger.exception(
                    "Failed to decompress content for kl3m_id '%s': %s", kl3m_id, ex
                )
                return False

            document_values = (
                kl3m_id,
                doc.get("id"),
                doc.get("title"),
                doc.get("publisher"),
                json.dumps(doc.get("creator", []), default=str),
                doc.get("date"),
                doc.get("format"),
                doc.get("identifier"),
                doc.get("source"),
                doc.get("citation") or doc.get("bibliographic_citation"),
                doc.get("blake2b"),
                contents[0:8],
                doc.get("size"),
                doc.get("dataset_id"),
                doc.get("processed", 0),
                doc.get("num_representations", 0),
                doc.get("num_tokens", 0),
            )

            doc_sql = """
            INSERT OR REPLACE INTO documents
            (kl3m_id, id, title, publisher, creator, date, format, identifier, source,
             citation, blake2b, first8, size, dataset_id, processed, num_representations, num_tokens)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """
            cursor = self.conn.cursor()
            cursor.execute(doc_sql, document_values)
            self.logger.info("Inserted/updated document with kl3m_id: %s", kl3m_id)

            # Remove any existing metadata for this document.
            cursor.execute("DELETE FROM metadata WHERE kl3m_id = ?;", (kl3m_id,))
            self.logger.debug("Deleted existing metadata for kl3m_id: %s", kl3m_id)

            # Insert metadata entries from the 'extra' field.
            extra = doc.get("extra", {})
            meta_rows = []
            for key, value in extra.items():
                if isinstance(value, (list, dict)):
                    meta_value = json.dumps(value)
                else:
                    meta_value = str(value)
                meta_rows.append((kl3m_id, key, meta_value))
            if meta_rows:
                meta_sql = """
                INSERT OR REPLACE INTO metadata (kl3m_id, meta_key, meta_value)
                VALUES (?, ?, ?);
                """
                cursor.executemany(meta_sql, meta_rows)
                self.logger.debug("Inserted metadata for kl3m_id: %s", kl3m_id)

            # Remove existing subjects and insert new ones.
            cursor.execute("DELETE FROM subjects WHERE kl3m_id = ?;", (kl3m_id,))
            self.logger.debug("Deleted existing subjects for kl3m_id: %s", kl3m_id)
            subject_list = doc.get("subject", [])
            subject_rows = [(kl3m_id, subject) for subject in subject_list]
            if subject_rows:
                subject_sql = """
                INSERT OR REPLACE INTO subjects (kl3m_id, subject)
                VALUES (?, ?);
                """
                cursor.executemany(subject_sql, subject_rows)
                self.logger.debug("Inserted subjects for kl3m_id: %s", kl3m_id)

            self.conn.commit()
            self.logger.info("Transaction committed for kl3m_id: %s", kl3m_id)
            return True

        except sqlite3.Error as e:
            self.logger.exception(
                "Error inserting document with kl3m_id '%s': %s", kl3m_id, e
            )
            self.conn.rollback()
            return False

    def insert_documents(self, kl3m_ids: List[str], docs: List[Dict[str, Any]]) -> None:
        """
        Batch insert or update multiple documents along with their metadata and subjects.
        All operations are wrapped in a single transaction.

        Args:
            kl3m_ids (List[str]): List of unique document identifiers.
            docs (List[Dict[str, Any]]): List of document dictionaries.

        Returns:
            None
        """
        try:
            doc_sql = """
            INSERT OR REPLACE INTO documents
            (kl3m_id, id, title, publisher, creator, date, format, identifier, source,
             citation, blake2b, first8, size, dataset_id, processed, num_representations, num_tokens)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """
            meta_sql = """
            INSERT OR REPLACE INTO metadata (kl3m_id, meta_key, meta_value)
            VALUES (?, ?, ?);
            """
            subject_sql = """
            INSERT OR REPLACE INTO subjects (kl3m_id, subject)
            VALUES (?, ?);
            """

            doc_rows = []
            meta_rows = []
            subject_rows = []

            for i, doc in enumerate(docs):
                kl3m_id = kl3m_ids[i]
                try:
                    decoded_content = base64.b64decode(doc["content"])
                    contents = zlib.decompress(decoded_content)
                except Exception as ex:
                    self.logger.exception(
                        "Failed to decompress content for kl3m_id '%s': %s", kl3m_id, ex
                    )
                    continue

                doc_rows.append(
                    (
                        kl3m_id,
                        doc.get("id"),
                        doc.get("title"),
                        doc.get("publisher"),
                        json.dumps(doc.get("creator", [])),
                        doc.get("date"),
                        doc.get("format"),
                        doc.get("identifier"),
                        doc.get("source"),
                        doc.get("citation") or doc.get("bibliographic_citation"),
                        doc.get("blake2b"),
                        contents[0:8],
                        doc.get("size"),
                        doc.get("dataset_id"),
                        doc.get("processed", 0),
                        doc.get("num_representations", 0),
                        doc.get("num_tokens", 0),
                    )
                )

                extra = doc.get("extra", {})
                for key, value in extra.items():
                    if isinstance(value, (list, dict)):
                        meta_value = json.dumps(value)
                    else:
                        meta_value = str(value)
                    meta_rows.append((kl3m_id, key, meta_value))

                for subject in doc.get("subject", []):
                    subject_rows.append((kl3m_id, subject))

            cursor = self.conn.cursor()
            cursor.executemany(doc_sql, doc_rows)
            self.logger.info("Batch inserted %d documents.", len(doc_rows))

            # Delete and insert metadata for these documents.
            cursor.executemany(
                "DELETE FROM metadata WHERE kl3m_id = ?;",
                [(kl3m_id,) for kl3m_id in kl3m_ids],
            )
            self.logger.debug("Deleted metadata for %d documents.", len(kl3m_ids))
            if meta_rows:
                cursor.executemany(meta_sql, meta_rows)
                self.logger.debug("Inserted %d metadata rows.", len(meta_rows))

            # Delete and insert subjects for these documents.
            cursor.executemany(
                "DELETE FROM subjects WHERE kl3m_id = ?;",
                [(kl3m_id,) for kl3m_id in kl3m_ids],
            )
            self.logger.debug("Deleted subjects for %d documents.", len(kl3m_ids))
            if subject_rows:
                cursor.executemany(subject_sql, subject_rows)
                self.logger.debug("Inserted %d subject rows.", len(subject_rows))

            self.conn.commit()
            self.logger.info("Batch transaction committed.")
        except sqlite3.Error as e:
            self.logger.exception("Error during batch insertion: %s", e)
            self.conn.rollback()

    def get_document_by_id(self, kl3m_id: str) -> Dict[str, Any]:
        """
        Retrieve a document by its kl3m_id.

        The returned dictionary includes:
            - All main document fields.
            - A 'subject' key containing a list of subjects.
            - An 'extra' key containing a dictionary of metadata key/value pairs.
              Attempts to parse metadata values from JSON where possible.

        Args:
            kl3m_id (str): Unique document identifier.

        Returns:
            Dict[str, Any]: Document data or an empty dict if not found.
        """
        try:
            cursor = self.conn.cursor()
            self.logger.info("Retrieving document with kl3m_id: %s", kl3m_id)
            cursor.execute("SELECT * FROM documents WHERE kl3m_id = ?;", (kl3m_id,))
            row = cursor.fetchone()
            if not row:
                self.logger.info("No document found with kl3m_id: %s", kl3m_id)
                return {}

            document = dict(row)
            try:
                document["creator"] = json.loads(document.get("creator", "[]"))
            except json.JSONDecodeError:
                document["creator"] = []

            # Retrieve normalized subjects.
            cursor.execute(
                "SELECT subject FROM subjects WHERE kl3m_id = ?;", (kl3m_id,)
            )
            subjects = [sub_row["subject"] for sub_row in cursor.fetchall()]
            document["subject"] = subjects

            # Retrieve metadata.
            cursor.execute(
                "SELECT meta_key, meta_value FROM metadata WHERE kl3m_id = ?;",
                (kl3m_id,),
            )
            extra = {}
            for meta in cursor.fetchall():
                key = meta["meta_key"]
                value = meta["meta_value"]
                try:
                    extra[key] = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    extra[key] = value
            document["extra"] = extra

            self.logger.info("Document retrieved successfully for kl3m_id: %s", kl3m_id)
            return document

        except sqlite3.Error as e:
            self.logger.exception(
                "Error retrieving document with kl3m_id '%s': %s", kl3m_id, e
            )
            return {}

    def close(self) -> None:
        """
        Close the SQLite database connection.
        """
        try:
            self.conn.close()
            self.logger.info("Database connection closed.")
        except sqlite3.Error as e:
            self.logger.exception("Error closing database connection: %s", e)
