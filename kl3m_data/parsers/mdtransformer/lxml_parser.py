"""
mdtransformer.lxml_parser - HTML to Markdown parser using lxml.html

This module contains the LXMLToMarkdownParser class, which implements
HTML to Markdown parsing using the lxml library.

The parser handles various HTML elements and converts them to their Markdown
equivalents, including headings, paragraphs, lists, links, images, emphasis,
strong text, code blocks, blockquotes, horizontal rules, and tables.
"""

# imports
import traceback
from typing import List

# packages
from lxml import html
from lxml.etree import _Element  # pylint: disable=no-name-in-module

from kl3m_data.logger import LOGGER

# project
from kl3m_data.parsers.mdtransformer.base.parser import HTMLToMarkdownParser
from kl3m_data.parsers.mdtransformer.base.parser_config import (
    ParserConfig,
    SIMPLE_TAG_SET,
)


class LXMLHTMLParser(HTMLToMarkdownParser):
    """HTML to Markdown parser using lxml.html

    This class extends the HTMLToMarkdownParser base class and implements
    the parsing logic using the lxml library for HTML parsing.
    """

    def __init__(self, config: ParserConfig = None) -> None:
        """Initialize the LXMLToMarkdownParser.

        Args:
            config (ParserConfig, optional): The parser configuration. Defaults to None.
        """
        super().__init__(config)
        self.config = config or ParserConfig()

        # Validate include_tags and exclude_tags
        if self.config.include_tags and self.config.exclude_tags:
            raise ValueError(
                "Only one of include_tags or exclude_tags can be set, not both."
            )

    def parse(self, html_str: str, **kwargs) -> str:
        """Parse HTML string and convert it to Markdown.

        Args:
            html_str (str): The input HTML string to be parsed.
            kwargs: Additional keyword arguments for the parser.

        Returns:
            str: The converted Markdown string.

        Raises:
            ValueError: If the input HTML is invalid or cannot be parsed.
        """
        parser = html.HTMLParser(
            recover=True,
            remove_blank_text=not self.config.preserve_comments,
            remove_comments=not self.config.preserve_comments,
            remove_pis=not self.config.preserve_comments,
            strip_cdata=True,
            no_network=True,
        )

        try:
            # parse root
            root = html.fromstring(html_str, parser=parser)

            # remove exclude tag from the tree
            if self.config.exclude_tags:
                for tag in self.config.exclude_tags:
                    for element in root.xpath(f"//{tag}"):
                        element.getparent().remove(element)

            # implement include_tags parsing
            if self.config.include_tags:
                for element in root.iter():
                    if element.tag not in self.config.include_tags:
                        element.getparent().remove(element)

            # implement simple_mode parsing
            if self.config.simple_mode:
                for element in root.iter():
                    if element.tag not in SIMPLE_TAG_SET:
                        if element.getparent() is not None:
                            element.getparent().remove(element)

            return "\n\n".join(self._parse_block_elements(root))
        except Exception as e:
            traceback_string = traceback.format_exc()
            LOGGER.error(f"Error parsing HTML: {e}\n{traceback_string}")
            raise ValueError(f"Invalid HTML input: {e}")

    def _parse_block_elements(self, element: _Element) -> List[str]:
        """Parse block elements from the HTML tree.

        Args:
            element (_Element): The HTML element to parse.

        Returns:
            List[str]: A list of parsed block elements as strings.
        """
        blocks = []
        for child in element.iterchildren():
            if self.config.exclude_tags and child.tag in self.config.exclude_tags:
                continue
            if self.config.include_tags and child.tag not in self.config.include_tags:
                continue
            if self.config.simple_mode and child.tag not in SIMPLE_TAG_SET:
                continue
            if child.tag in ("p", "h1", "h2", "h3", "h4", "h5", "h6"):
                blocks.append(self._convert_block_element(child))
            elif child.tag in ("ul", "ol"):
                blocks.append(self._convert_lists(child))
            elif child.tag == "blockquote":
                blocks.append(self._convert_blockquote(child))
            elif child.tag == "hr":
                blocks.append(self._convert_horizontal_rule(child))
            elif child.tag == "pre":
                blocks.append(self._convert_code(child))
            elif child.tag == "table" and self.config.table_support:
                blocks.append(self._convert_table(child))
            elif child.tag == "div":
                blocks.append(self._convert_div(child))
            else:
                blocks.extend(self._parse_block_elements(child))
        return blocks

    def _convert_div(self, element: _Element) -> str:
        """Convert div elements, focusing on extracting important nested content.

        Args:
            element (_Element): The div element to convert.

        Returns:
            str: The converted content as a string.
        """
        content = []
        for child in element.iterchildren():
            if self.config.exclude_tags and child.tag in self.config.exclude_tags:
                continue
            if self.config.include_tags and child.tag not in self.config.include_tags:
                continue
            if self.config.simple_mode and child.tag not in SIMPLE_TAG_SET:
                continue
            if child.tag == "a":
                content.append(self._convert_links(child))
            elif child.tag in ("span", "div"):
                content.append(self._convert_div(child))
            else:
                content.append(self._parse_inline_elements(child))
        return (" ".join(content).strip() + "\n") if content else ""

    def _parse_inline_elements(self, element: _Element) -> str:
        """Parse inline elements from the HTML tree.

        Args:
            element (_Element): The HTML element to parse.

        Returns:
            str: The parsed inline elements as a string.
        """
        parts = []
        for item in element.xpath("node()"):
            if isinstance(item, _Element):
                if self.config.exclude_tags and item.tag in self.config.exclude_tags:
                    continue
                if (
                    self.config.include_tags
                    and item.tag not in self.config.include_tags
                ):
                    continue
                if self.config.simple_mode and item.tag not in SIMPLE_TAG_SET:
                    continue
                if item.tag == "a":
                    parts.append(self._convert_links(item))
                elif item.tag == "img":
                    parts.append(self._convert_images(item))
                elif item.tag == "em":
                    parts.append(self._convert_emphasis(item))
                elif item.tag == "strong":
                    parts.append(self._convert_strong(item))
                elif item.tag == "code":
                    parts.append(self._convert_code(item))
                elif item.tag == "del" and self.config.strikethrough_support:
                    parts.append(self._convert_strikethrough(item))
                else:
                    parts.append(self._parse_inline_elements(item))
            else:
                parts.append(item)
        return "".join(parts).strip()

    def _convert_block_element(self, element: _Element) -> str:
        """Convert a block-level element to its Markdown equivalent.

        Args:
            element (_Element): The lxml Element object to be converted.

        Returns:
            str: The Markdown representation of the block element.
        """
        if element.tag in self.config.exclude_tags:
            return ""
        elif element.tag.startswith("h"):
            return self._convert_headings(element)
        elif element.tag == "p":
            return self._convert_paragraphs(element)
        else:
            return self._parse_inline_elements(element)

    def _convert_headings(self, element: _Element) -> str:
        """Convert HTML headings to Markdown headings.

        Args:
            element (_Element): The lxml Element object representing a heading.

        Returns:
            str: The Markdown representation of the heading.
        """
        level = int(element.tag[1])
        content = self._parse_inline_elements(element).strip()
        if self.config.markdown_style == self.config.markdown_style.CUSTOM:
            return f"{self.config.custom_heading_style * level} {content}"
        return f"{'#' * level} {content}"

    def _convert_paragraphs(self, element: _Element) -> str:
        """Convert HTML paragraphs to Markdown paragraphs.

        Args:
            element (_Element): The lxml Element object representing a paragraph.

        Returns:
            str: The Markdown representation of the paragraph.
        """
        return self._parse_inline_elements(element).strip()

    def _convert_lists(self, element: _Element) -> str:
        """Convert HTML lists (ordered and unordered) to Markdown lists.

        Args:
            element (_Element): The lxml Element object representing a list.

        Returns:
            str: The Markdown representation of the list.
        """
        items = []
        item_number = 1
        for li in element.findall(".//li"):
            item_content = self._parse_inline_elements(li).strip()
            if element.tag == "ol":
                prefix = f"{item_number}. "
                item_number += 1
            else:
                prefix = (
                    self.config.custom_list_marker + " "
                    if self.config.markdown_style == self.config.markdown_style.CUSTOM
                    else "- "
                )
            items.append(f"{prefix}{item_content}")
        return "\n".join(items)

    def _convert_links(self, element: _Element) -> str:
        """Convert HTML links to Markdown links.

        Args:
            element (_Element): The lxml Element object representing a link.

        Returns:
            str: The Markdown representation of the link.
        """
        # if the <a> tag has no inner text or elements, return an empty string since it's an anchor
        if not element.text_content().strip():
            return ""

        href = element.get("href", "")
        content = self._parse_inline_elements(element).strip()

        if self.config.output_links:
            return f"[{content}]({href})"
        return content

    def _convert_images(self, element: _Element) -> str:
        """Convert HTML images to Markdown images.

        Args:
            element (_Element): The lxml Element object representing an image.

        Returns:
            str: The Markdown representation of the image.
        """
        src = element.get("src", "")
        alt = element.get("alt", "")

        if self.config.output_images:
            return f"![{alt}]({src})"
        return alt

    def _convert_emphasis(self, element: _Element) -> str:
        """Convert HTML emphasis to Markdown emphasis.

        Args:
            element (_Element): The lxml Element object representing emphasized text.

        Returns:
            str: The Markdown representation of the emphasized text.
        """
        content = self._parse_inline_elements(element).strip()
        emphasis_style = (
            self.config.custom_emphasis_style
            if self.config.markdown_style == self.config.markdown_style.CUSTOM
            else "*"
        )
        return f"{emphasis_style}{content}{emphasis_style}"

    def _convert_strong(self, element: _Element) -> str:
        """Convert HTML strong text to Markdown strong text.

        Args:
            element (_Element): The lxml Element object representing strong text.

        Returns:
            str: The Markdown representation of the strong text.
        """
        content = self._parse_inline_elements(element).strip()
        strong_style = (
            self.config.custom_strong_style
            if self.config.markdown_style == self.config.markdown_style.CUSTOM
            else "**"
        )
        return f"{strong_style}{content}{strong_style}"

    def _convert_code(self, element: _Element) -> str:
        """Convert HTML code elements to Markdown code blocks or inline code.

        Args:
            element (_Element): The lxml Element object representing a code element.

        Returns:
            str: The Markdown representation of the code element.
        """
        if element.tag == "pre":
            code_element = element.find(".//code")
            if code_element is not None:
                content = code_element.text_content().strip()
                lang = code_element.get("class", "")
                if lang.startswith(self.config.code_language_class or ""):
                    lang = lang[len(self.config.code_language_class) :]
                else:
                    lang = ""
                code_block_style = (
                    self.config.custom_code_block_style
                    if self.config.markdown_style == self.config.markdown_style.CUSTOM
                    else "```"
                )
                return f"{code_block_style}{lang}\n{content}\n{code_block_style}"
        content = element.text_content().strip()
        return f"`{content}`"

    def _convert_blockquote(self, element: _Element) -> str:
        """Convert HTML blockquotes to Markdown blockquotes.

        Args:
            element (_Element): The lxml Element object representing a blockquote.

        Returns:
            str: The Markdown representation of the blockquote.
        """
        content = self._parse_inline_elements(element).strip()
        return "\n".join(f"> {line}" for line in content.split("\n"))

    def _convert_horizontal_rule(self, element: _Element) -> str:
        """Convert HTML horizontal rules to Markdown horizontal rules.

        Args:
            element (_Element): The lxml Element object representing a horizontal rule.

        Returns:
            str: The Markdown representation of the horizontal rule.
        """
        return "---"

    def _convert_table(self, element: _Element) -> str:
        """Convert HTML tables to Markdown tables.

        Args:
            element (_Element): The lxml Element object representing a table.

        Returns:
            str: The Markdown representation of the table.
        """
        if not self.config.table_support:
            return ""

        rows = element.findall(".//tr")
        if not rows:
            return ""

        header = rows[0]
        body = rows[1:]

        # Convert header and calculate column widths
        header_cells = [
            self._parse_inline_elements(cell).strip()
            for cell in header.findall(".//th") or header.findall(".//td")
        ]
        column_widths = [len(cell) for cell in header_cells]

        # Update column widths based on body cells
        for row in body:
            cells = [
                self._parse_inline_elements(cell).strip()
                for cell in row.findall(".//td")
            ]
            for i, cell in enumerate(cells):
                if i < len(column_widths):
                    column_widths[i] = max(column_widths[i], len(cell))

        # Create header row
        header_row = "|" + " | ".join(
            cell.ljust(width) for cell, width in zip(header_cells, column_widths)
        )
        markdown_table = [header_row]

        # Create separator row
        separator_row = "|" + " | ".join("-" * width for width in column_widths)
        markdown_table.append(separator_row)

        # Create body rows
        for row in body:
            cells = [
                self._parse_inline_elements(cell).strip()
                for cell in row.findall(".//td")
            ]
            formatted_row = "|" + " | ".join(
                cell.ljust(width) for cell, width in zip(cells, column_widths)
            )
            markdown_table.append(formatted_row)

        return "\n".join(markdown_table)

    def _convert_strikethrough(self, element: _Element) -> str:
        """Convert HTML strikethrough text to Markdown strikethrough text.

        Args:
            element (_Element): The lxml Element object representing strikethrough text.

        Returns:
            str: The Markdown representation of the strikethrough text.
        """
        if not self.config.strikethrough_support:
            return self._parse_inline_elements(element)

        content = self._parse_inline_elements(element).strip()
        return f"~~{content}~~"
