"""
FDLP data types
"""

# future
from __future__ import annotations

# imports
import re
import html
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class CGPMetadata:
    """
    Represents metadata from the Catalog of U.S. Government Publications (CGP).
    """

    title: Optional[str] = None
    description: Optional[str] = None
    author: Optional[str] = None
    publisher: Optional[str] = None
    subjects: List[str] = field(default_factory=list)

    # extras
    extra: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def parse_cgp_values(code: str) -> Optional[str]:
        """
        Parses the JavaScript code to extract the value assigned to recordVal.

        If recordVal is not found, tries to extract the text between document.write(cell); and document.write('</span>');

        Args:
            code (str): The JavaScript code as a string.

        Returns:
            str or None: The cleaned value assigned to recordVal, or extracted from the code, or None if not found.
        """
        # Try to extract the value assigned to recordVal using a regular expression
        match = re.search(r"var\s+recordVal\s*=\s*`(.*?)`;", code, re.DOTALL)
        if match:
            value = match.group(1)
        else:
            # If recordVal is not found, try to extract the text between document.write(cell); and document.write('</span>');
            match = re.search(
                r"document\.write\(cell\);\s*(.*?)\s*document\.write\('</span>'\);",
                code,
                re.DOTALL,
            )
            if match:
                value = match.group(1)
            else:
                # Return None if no value is found
                return None

        # Unescape HTML entities to convert them to their corresponding characters
        value = html.unescape(value)
        # Remove any HTML tags present in the value
        value = re.sub(r"<[^<]+?>", "", value)
        # Replace multiple whitespace characters with a single space
        value = re.sub(r"\s+", " ", value)
        # Trim leading and trailing whitespace
        value = value.strip()

        return value
