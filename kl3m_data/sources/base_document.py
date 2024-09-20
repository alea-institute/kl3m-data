"""
Base document to standardize retrieval/update interface for all resources, focused
on Dublin Core metadata.

# References
https://www.dublincore.org/specifications/dublin-core/dcmi-terms/
"""

# future imports
from __future__ import annotations

# imports
import json
import uuid
from dataclasses import dataclass, field
from typing import Optional

# packages
from alea_dublincore.document import DublinCoreDocument

# project
from kl3m_data.config import KL3MDataConfig
from kl3m_data.utils.s3_utils import get_object_bytes, get_s3_client, put_object_bytes


@dataclass
class Document(DublinCoreDocument):
    """
    Document metadata
    """

    dataset_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    # get the default path
    def get_s3_key(self) -> str:
        """
        Get the S3 key for the document based on its

        Returns:
            str: The default path.
        """
        return f"documents/{self.dataset_id}/{self.id}.json"

    def to_s3(self) -> bool:
        """
        Save the document to S3.

        Returns:
            bool: Whether the save was successful.
        """
        # get the S3 client
        s3_client = get_s3_client()

        # get the S3 key
        s3_key = self.get_s3_key()

        # save the document to S3
        return put_object_bytes(
            s3_client,
            KL3MDataConfig.default_s3_bucket,
            s3_key,
            self.to_json().encode("utf-8"),
        )

    @classmethod
    def from_s3(cls, dataset_id: str, document_id: str) -> Optional[Document]:
        """
        Load a document from S3.

        Args:
            dataset_id (str): The dataset ID.
            document_id (str): The document ID.

        Returns:
            Optional[Document]: The loaded document.
        """
        # get the S3 client
        s3_client = get_s3_client()

        # get the S3 key
        s3_key = f"{dataset_id}/{document_id}.json"

        # load the document from S3
        object_data = get_object_bytes(
            s3_client, KL3MDataConfig.default_s3_bucket, s3_key
        )

        # return None if no data
        if object_data is None:
            return None

        # load from the document otherwise
        dc_document = cls.from_dict(json.loads(object_data.decode("utf-8")))
        return Document(**dc_document.to_dict())
