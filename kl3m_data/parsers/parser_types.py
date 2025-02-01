"""
Output types for parsers.
"""

# imports
import base64
import json
import zlib
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional

# packages

# project


@dataclass
class ParsedDocumentRepresentation:
    """
    Parser output representation with text, tokens, and metadata.
    """

    # format or mime type, e.g., text/plain, text/markdown, text/html
    content: str
    tokens: Dict[str, List[int]] = field(default_factory=dict)
    mime_type: str = "text/plain"

    def __init__(
        self,
        content: str,
        tokens: Dict[str, List[int]] = None,
        mime_type: str = "text/plain",
    ):
        """
        Initialize the parsed document representation.

        Args:
            content (str): Content of the representation.
            tokens (dict): Tokens of the representation.
            mime_type (str): Mime type of the representation.

        Returns:
            None
        """
        self.content = content
        self.tokens = tokens or {}
        self.mime_type = mime_type


@dataclass
class ParsedDocument:
    """
    Parser output with key fields and representations.
    """

    # primary key fields
    source: str
    identifier: str
    original_uri: Optional[str] = None

    # representations
    representations: Dict[str, ParsedDocumentRepresentation] = field(
        default_factory=dict
    )

    # metadata and status fields
    metadata: dict = field(default_factory=dict)
    success: bool = False
    error: Optional[str] = None

    def __init__(
        self,
        source: str,
        identifier: str,
        original_uri: Optional[str] = None,
        representations: Optional[Dict[str, ParsedDocumentRepresentation]] = None,
        metadata: dict = None,
        success: bool = False,
        error: Optional[str] = None,
    ):
        """
        Initialize the parsed document.

        Args:
            source (str): Source of the document.
            identifier (str): Identifier of the document.
            original_uri (str): Original URI of the document.
            representations (dict): Representations of the document.
            metadata (dict): Metadata of the document.
            success (bool): Whether the parsing was successful.
            error (str): Error message if parsing failed.

        Returns:
            None
        """
        self.source = source
        self.identifier = identifier
        self.original_uri = original_uri
        self.representations = representations or {}
        self.metadata = metadata or {}
        self.success = success
        self.error = error

    def __repr__(self):
        return f"<ParsedDocument source={self.source} identifier={self.identifier} representations={len(self.representations)}>"

    def __str__(self):
        return f"<ParsedDocument source={self.source} identifier={self.identifier} representations={len(self.representations)}>"

    def to_dict(self) -> dict:
        """
        Convert the parsed document to a dictionary.

        Returns:
            dict: The parsed document as a dictionary.
        """
        return asdict(self)

    def to_json_dict(self) -> dict:
        """
        Convert the parsed document to a dictionary.

        Returns:
            dict: The parsed document as a dictionary.
        """
        # get the dictionary
        doc_dict = self.to_dict()

        # compress all the representations
        for rep_key, rep_val in doc_dict["representations"].items():
            rep_val["content"] = base64.b64encode(
                zlib.compress(rep_val["content"].encode())
            ).decode()

        # return the json string
        return doc_dict

    def to_json(self) -> str:
        """
        Convert the parsed document to a JSON string.

        Returns:
            str: The parsed document as a JSON string.
        """
        # get the json dictionary as a string
        return json.dumps(self.to_json_dict(), default=str)
