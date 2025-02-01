"""
mdtransformer.regex_parser - RegexHTMLParser for converting HTML to Markdown.

This module contains the RegexHTMLParser class, which implements
the HTMLToMarkdownParser interface using regular expressions to
convert HTML to Markdown.
"""

# imports
import re
from typing import List

from kl3m_data.logger import LOGGER
# packages

# project
from kl3m_data.parsers.mdtransformer.base.parser import HTMLToMarkdownParser
from kl3m_data.parsers.mdtransformer.base.parser_config import ParserConfig

# constants


class RegexHTMLParser(HTMLToMarkdownParser):
    """A regex-based HTML to Markdown parser.

    This class implements the HTMLToMarkdownParser interface using
    regular expressions to convert HTML to Markdown.
    """

    def __init__(self, config: ParserConfig = None) -> None:
        """Initialize the RegexHTMLParser.

        Args:
            config (ParserConfig, optional): Configuration for the parser.
                Defaults to None.
        """
        super().__init__(config)
        self.config = config or ParserConfig()

    def parse(self, html_str: str, **kwargs) -> str:
        """Parse HTML and convert it to Markdown.

        Args:
            html_str (str): The HTML string to parse.
            **kwargs: Additional keyword arguments.

        Returns:
            str: The converted Markdown string.
        """
        try:
            # Remove comments
            html_str = re.sub(r"<!--.*?-->", "", html_str, flags=re.DOTALL)

            # Strip unwanted tags when simple_mode is True
            if self.config.simple_mode:
                unwanted_tags = [
                    "script",
                    "noscript",
                    "style",
                    "head",
                    "meta",
                    "link",
                    "title",
                ]
                for tag in unwanted_tags:
                    html_str = re.sub(
                        f"<{tag}.*?</{tag}>",
                        "",
                        html_str,
                        flags=re.DOTALL | re.IGNORECASE,
                    )

            # Parse block elements
            blocks = self._parse_block_elements(html_str)

            # Join blocks with double newlines
            return "\n\n".join(blocks)
        except Exception as e:
            LOGGER.error(f"Error parsing HTML: {e}")
            raise

    def _parse_block_elements(self, html: str) -> List[str]:
        """Parse block elements from HTML.

        Args:
            html (str): The HTML string to parse.

        Returns:
            List[str]: A list of parsed Markdown blocks.
        """
        blocks = []
        patterns = {
            "container": r"<(article|section|div|span).*?>(.*?)</\1>",
            "heading": r"<h([1-6]).*?>(.*?)</h\1>",
            "paragraph": r"<p.*?>(.*?)</p>",
            "list": r"<(ul|ol).*?>(.*?)</\1>",
            "blockquote": r"<blockquote.*?>(.*?)</blockquote>",
            "code": r"<pre.*?><code.*?>(.*?)</code></pre>",
            "hr": r"<hr.*?>",
            "table": r"<table.*?>(.*?)</table>",
        }

        for block_type, pattern in patterns.items():
            matches = re.findall(pattern, html, re.DOTALL)
            for match in matches:
                try:
                    if block_type == "container":
                        tag, content = match
                        blocks.append(self._parse_inline_elements(content))
                    elif block_type == "heading":
                        level, content = match
                        blocks.append(self._convert_headings(int(level), content))
                    elif block_type == "paragraph":
                        blocks.append(self._convert_paragraphs(match))
                    elif block_type == "list":
                        list_type, items = match
                        blocks.append(self._convert_lists(list_type, items))
                    elif block_type == "blockquote":
                        blocks.append(self._convert_blockquote(match))
                    elif block_type == "code":
                        blocks.append(self._convert_code(match))
                    elif block_type == "hr":
                        blocks.append(self._convert_horizontal_rule(match))
                    elif block_type == "table":
                        blocks.append(self._convert_table(match))
                except Exception as e:
                    LOGGER.error(f"Error converting {block_type}: {e}")

        return blocks

    def _convert_headings(self, level: int, element: str) -> str:
        """Convert HTML headings to Markdown.

        Args:
            level (int): The heading level (1-6).
            element (str): The heading content.

        Returns:
            str: The Markdown heading.
        """
        return f"{'#' * level} {self._parse_inline_elements(element)}"

    def _convert_paragraphs(self, element: str) -> str:
        """Convert HTML paragraphs to Markdown.

        Args:
            element (str): The paragraph content.

        Returns:
            str: The Markdown paragraph.
        """
        return self._parse_inline_elements(element)

    def _convert_lists(self, list_type: str, items: str) -> str:
        """Convert HTML lists to Markdown.

        Args:
            list_type (str): The type of list ('ul' or 'ol').
            items (str): The HTML list items.

        Returns:
            str: The Markdown list.
        """
        list_items = re.findall(r"<li.*?>(.*?)</li>", items, re.DOTALL)
        converted_items = []
        for i, item in enumerate(list_items, 1):
            if list_type == "ol":
                converted_items.append(f"{i}. {self._parse_inline_elements(item)}")
            else:
                converted_items.append(f"- {self._parse_inline_elements(item)}")
        return "\n".join(converted_items)

    def _convert_blockquote(self, content: str) -> str:
        """Convert HTML blockquotes to Markdown.

        Args:
            content (str): The blockquote content.

        Returns:
            str: The Markdown blockquote.
        """
        lines = self._parse_inline_elements(content).split("\n")
        return "\n".join(f"> {line}" for line in lines)

    def _convert_code(self, content: str) -> str:
        """Convert HTML code blocks to Markdown.

        Args:
            content (str): The code block content.

        Returns:
            str: The Markdown code block.
        """
        return f"```\n{content}\n```"

    def _convert_horizontal_rule(self, _: str) -> str:
        """Convert HTML horizontal rules to Markdown.

        Args:
            _ (str): Unused parameter.

        Returns:
            str: The Markdown horizontal rule.
        """
        return "---"

    def _convert_table(self, content: str) -> str:
        """Convert HTML tables to Markdown.

        Args:
            content (str): The table content.

        Returns:
            str: The Markdown table.
        """
        rows = re.findall(r"<tr.*?>(.*?)</tr>", content, re.DOTALL)
        markdown_rows = []
        for i, row in enumerate(rows):
            cells = re.findall(r"<t[hd].*?>(.*?)</t[hd]>", row, re.DOTALL)
            markdown_cells = [self._parse_inline_elements(cell) for cell in cells]
            markdown_rows.append("| " + " | ".join(markdown_cells) + " |")
            if i == 0:
                markdown_rows.append("| " + " | ".join(["---" for _ in cells]) + " |")
        return "\n".join(markdown_rows)

    def _parse_inline_elements(self, html: str) -> str:
        """Parse inline elements from HTML.

        Args:
            html (str): The HTML string to parse.

        Returns:
            str: The parsed Markdown string.
        """
        # Convert links
        html = re.sub(r'<a\s+href="(.*?)".*?>(.*?)</a>', self._convert_links, html)

        # Convert images
        html = re.sub(
            r'<img\s+src="(.*?)"\s+alt="(.*?)".*?>', self._convert_images, html
        )

        # Convert emphasis
        html = re.sub(r"<em.*?>(.*?)</em>", self._convert_emphasis, html)

        # Convert strong
        html = re.sub(r"<strong.*?>(.*?)</strong>", self._convert_strong, html)

        # Convert inline code
        html = re.sub(r"<code.*?>(.*?)</code>", self._convert_inline_code, html)

        # Remove remaining HTML tags
        html = re.sub(r"<.*?>", "", html)

        # Unescape HTML entities
        html = self._unescape_html_entities(html)

        return html.strip()

    def _convert_links(self, match: re.Match) -> str:
        """Convert HTML links to Markdown.

        Args:
            match (re.Match): The regex match object.

        Returns:
            str: The Markdown link.
        """
        href, content = match.groups()
        if self.config.output_links:
            return f"[{content}]({href})"
        return content

    def _convert_images(self, match: re.Match) -> str:
        """Convert HTML images to Markdown.

        Args:
            match (re.Match): The regex match object.

        Returns:
            str: The Markdown image.
        """
        src, alt = match.groups()
        if self.config.output_images:
            return f"![{alt}]({src})"
        return alt

    def _convert_emphasis(self, match: re.Match) -> str:
        """Convert HTML emphasis to Markdown.

        Args:
            match (re.Match): The regex match object.

        Returns:
            str: The Markdown emphasis.
        """
        content = match.group(1)
        emphasis_style = (
            self.config.custom_emphasis_style
            if self.config.markdown_style == self.config.markdown_style.CUSTOM
            else "*"
        )
        return f"{emphasis_style}{content}{emphasis_style}"

    def _convert_strong(self, match: re.Match) -> str:
        """Convert HTML strong to Markdown.

        Args:
            match (re.Match): The regex match object.

        Returns:
            str: The Markdown strong emphasis.
        """
        content = match.group(1)
        strong_style = (
            self.config.custom_strong_style
            if self.config.markdown_style == self.config.markdown_style.CUSTOM
            else "**"
        )
        return f"{strong_style}{content}{strong_style}"

    def _convert_inline_code(self, match: re.Match) -> str:
        """Convert HTML inline code to Markdown.

        Args:
            match (re.Match): The regex match object.

        Returns:
            str: The Markdown inline code.
        """
        content = match.group(1)
        return f"`{content}`"

    def _unescape_html_entities(self, text: str) -> str:
        """Unescape HTML entities.

        Args:
            text (str): The text containing HTML entities.

        Returns:
            str: The text with unescaped HTML entities.
        """
        entities = {"&lt;": "<", "&gt;": ">", "&amp;": "&", "&quot;": '"', "&#39;": "'"}
        for entity, char in entities.items():
            text = text.replace(entity, char)
        return text
