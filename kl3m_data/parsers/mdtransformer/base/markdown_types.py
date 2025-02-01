"""
mdtransformer.markdown_types - Data structures for representing Markdown elements

This module defines the data structures used to represent Markdown elements
in the mdtransformer package. It includes classes for inline elements,
block elements, and the overall Markdown document structure.
"""

# imports
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional, Union, TypeVar

# Type aliases for improved readability
InlineContent = Union["InlineElement", str]
BlockContent = Union["BlockElement", "InlineElement", str]
MarkdownVisitor = TypeVar("MarkdownVisitor")


@dataclass
class MarkdownElement:
    """Base class for all Markdown elements."""

    def accept(self, visitor: MarkdownVisitor) -> None:
        """Accept a visitor to process the element.

        Args:
            visitor (MarkdownVisitor): The visitor object that will process the element.

        Raises:
            NotImplementedError: If the subclass does not implement this method.
        """
        raise NotImplementedError("Subclasses must implement accept method.")


@dataclass
class InlineElement(MarkdownElement):
    """Base class for inline Markdown elements."""

    text: str


@dataclass
class EmphasisElement(InlineElement):
    """Represents emphasized text (italic or bold)."""

    level: int  # 1 for italics, 2 for bold

    def accept(self, visitor: "MarkdownVisitor") -> None:
        """Accept a visitor to process the emphasis element.

        Args:
            visitor (MarkdownVisitor): The visitor object that will process the element.
        """
        return visitor.visit_emphasis(self)


@dataclass
class LinkElement(InlineElement):
    """Represents a hyperlink."""

    url: str
    title: Optional[str] = None

    def accept(self, visitor: "MarkdownVisitor") -> None:
        """Accept a visitor to process the link element.

        Args:
            visitor (MarkdownVisitor): The visitor object that will process the element.
        """
        return visitor.visit_link(self)


@dataclass
class ImageElement(InlineElement):
    """Represents an image."""

    url: str
    alt_text: str
    title: Optional[str] = None

    def accept(self, visitor: "MarkdownVisitor") -> None:
        """Accept a visitor to process the image element.

        Args:
            visitor (MarkdownVisitor): The visitor object that will process the element.
        """
        return visitor.visit_image(self)


@dataclass
class CodeSpanElement(InlineElement):
    """Represents inline code."""

    def accept(self, visitor: "MarkdownVisitor") -> None:
        """Accept a visitor to process the code span element.

        Args:
            visitor (MarkdownVisitor): The visitor object that will process the element.
        """
        return visitor.visit_code_span(self)


@dataclass
class BlockElement(MarkdownElement):
    """Base class for block-level Markdown elements."""

    pass


@dataclass
class ParagraphElement(BlockElement):
    """Represents a paragraph."""

    content: List[InlineContent] = field(default_factory=list)

    def accept(self, visitor: "MarkdownVisitor") -> None:
        """Accept a visitor to process the paragraph element.

        Args:
            visitor (MarkdownVisitor): The visitor object that will process the element.
        """
        return visitor.visit_paragraph(self)


@dataclass
class HeadingElement(BlockElement):
    """Represents a heading."""

    level: int
    content: List[InlineContent] = field(default_factory=list)

    def accept(self, visitor: "MarkdownVisitor") -> None:
        """Accept a visitor to process the heading element.

        Args:
            visitor (MarkdownVisitor): The visitor object that will process the element.
        """
        return visitor.visit_heading(self)


@dataclass
class ListItemElement(BlockElement):
    """Represents a list item."""

    content: List[BlockContent] = field(default_factory=list)

    def accept(self, visitor: "MarkdownVisitor") -> None:
        """Accept a visitor to process the list item element.

        Args:
            visitor (MarkdownVisitor): The visitor object that will process the element.
        """
        return visitor.visit_list_item(self)


@dataclass
class ListElement(BlockElement):
    """Represents an ordered or unordered list."""

    items: List[ListItemElement] = field(default_factory=list)
    ordered: bool = False

    def accept(self, visitor: "MarkdownVisitor") -> None:
        """Accept a visitor to process the list element.

        Args:
            visitor (MarkdownVisitor): The visitor object that will process the element.
        """
        return visitor.visit_list(self)


@dataclass
class BlockQuoteElement(BlockElement):
    """Represents a block quote."""

    content: List[BlockContent] = field(default_factory=list)

    def accept(self, visitor: "MarkdownVisitor") -> None:
        """Accept a visitor to process the block quote element.

        Args:
            visitor (MarkdownVisitor): The visitor object that will process the element.
        """
        return visitor.visit_block_quote(self)


@dataclass
class CodeBlockElement(BlockElement):
    """Represents a code block."""

    content: str
    language: Optional[str] = None

    def accept(self, visitor: "MarkdownVisitor") -> None:
        """Accept a visitor to process the code block element.

        Args:
            visitor (MarkdownVisitor): The visitor object that will process the element.
        """
        return visitor.visit_code_block(self)


@dataclass
class HorizontalRuleElement(BlockElement):
    """Represents a horizontal rule."""

    def accept(self, visitor: "MarkdownVisitor") -> None:
        """Accept a visitor to process the horizontal rule element.

        Args:
            visitor (MarkdownVisitor): The visitor object that will process the element.
        """
        return visitor.visit_horizontal_rule(self)


@dataclass
class TableCellElement(MarkdownElement):
    """Represents a table cell."""

    content: List[InlineContent] = field(default_factory=list)
    align: Optional[str] = None  # 'left', 'center', 'right'

    def accept(self, visitor: "MarkdownVisitor") -> None:
        """Accept a visitor to process the table cell element.

        Args:
            visitor (MarkdownVisitor): The visitor object that will process the element.
        """
        return visitor.visit_table_cell(self)


@dataclass
class TableRowElement(MarkdownElement):
    """Represents a table row."""

    cells: List[TableCellElement] = field(default_factory=list)

    def accept(self, visitor: "MarkdownVisitor") -> None:
        """Accept a visitor to process the table row element.

        Args:
            visitor (MarkdownVisitor): The visitor object that will process the element.
        """
        return visitor.visit_table_row(self)


@dataclass
class TableElement(BlockElement):
    """Represents a table."""

    header: TableRowElement
    rows: List[TableRowElement] = field(default_factory=list)

    def accept(self, visitor: "MarkdownVisitor") -> None:
        """Accept a visitor to process the table element.

        Args:
            visitor (MarkdownVisitor): The visitor object that will process the element.
        """
        return visitor.visit_table(self)


@dataclass
class MarkdownDocument:
    """Represents a complete Markdown document."""

    elements: List[BlockElement] = field(default_factory=list)

    def accept(self, visitor: MarkdownVisitor) -> None:
        """Accept a visitor to process the document.

        Args:
            visitor (MarkdownVisitor): The visitor object that will process the document.
        """
        return visitor.visit_document(self)

    def add_element(self, element: BlockElement) -> None:
        """Add a block element to the document.

        Args:
            element (BlockElement): The block element to add to the document.
        """
        self.elements.append(element)


def create_paragraph(*content: InlineContent) -> ParagraphElement:
    """Create a paragraph element.

    Args:
        *content (InlineContent): The content of the paragraph.

    Returns:
        ParagraphElement: The created paragraph element.
    """
    return ParagraphElement(list(content))


def create_heading(level: int, *content: InlineContent) -> HeadingElement:
    """Create a heading element.

    Args:
        level (int): The heading level (1-6).
        *content (InlineContent): The content of the heading.

    Returns:
        HeadingElement: The created heading element.

    Raises:
        ValueError: If the heading level is not between 1 and 6.
    """
    if not 1 <= level <= 6:
        raise ValueError("Heading level must be between 1 and 6")
    return HeadingElement(level, list(content))


def create_list(ordered: bool, *items: Union[str, ListItemElement]) -> ListElement:
    """Create a list element.

    Args:
        ordered (bool): Whether the list is ordered or unordered.
        *items (Union[str, ListItemElement]): The items of the list.

    Returns:
        ListElement: The created list element.
    """
    return ListElement(
        [ListItemElement([item]) if isinstance(item, str) else item for item in items],
        ordered,
    )


def create_blockquote(*content: BlockContent) -> BlockQuoteElement:
    """Create a block quote element.

    Args:
        *content (BlockContent): The content of the block quote.

    Returns:
        BlockQuoteElement: The created block quote element.
    """
    return BlockQuoteElement(list(content))


def create_table(
    header: List[str], rows: List[List[str]], alignments: Optional[List[str]] = None
) -> TableElement:
    """Create a table element.

    Args:
        header (List[str]): The header row of the table.
        rows (List[List[str]]): The data rows of the table.
        alignments (Optional[List[str]]): The alignments for each column ('left', 'center', 'right').

    Returns:
        TableElement: The created table element.

    Raises:
        ValueError: If the number of columns in any row doesn't match the header.
    """
    if alignments is None:
        alignments = ["left"] * len(header)

    if any(len(row) != len(header) for row in rows):
        raise ValueError("All rows must have the same number of columns as the header")

    header_row = TableRowElement(
        [TableCellElement([cell], align) for cell, align in zip(header, alignments)]
    )
    data_rows = [
        TableRowElement(
            [TableCellElement([cell], align) for cell, align in zip(row, alignments)]
        )
        for row in rows
    ]

    return TableElement(header_row, data_rows)
