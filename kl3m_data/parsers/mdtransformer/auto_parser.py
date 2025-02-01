"""
mdtransformer.auto_parser - AutoParser class for automatic HTML -> Markdown parsing.

This module defines the AutoParser class for automatically converting HTML to Markdown using a set of rules.
"""

# imports
import traceback
from pathlib import Path

# project imports
from kl3m_data.parsers.mdtransformer.base.parser_config import ParserConfig

from kl3m_data.logger import LOGGER


class AutoParser:
    """Automatic HTML to Markdown parser, which handles detecting and applying the appropriate
    parser based on the input HTML content.
    """

    def __init__(self, config: ParserConfig = None) -> None:
        """Initialize the AutoParser.

        Args:
            config (ParserConfig, optional): The parser configuration. Defaults to None.
        """
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
        # try the LXML parser first
        try:
            from kl3m_data.parsers.mdtransformer.lxml_parser import LXMLHTMLParser

            parser = LXMLHTMLParser(self.config)
            output = parser.parse(html_str, **kwargs)
            if output is not None and len(output.strip()) > 0:
                return output.strip()

            LOGGER.warning(
                "LXML parser returned empty output, falling back to regex parser."
            )
        except ImportError:
            LOGGER.warning("LXML parser not available, falling back to default parser.")
            raise
        except Exception as e:
            LOGGER.error(f"Error parsing HTML with LXML: {e}")
            LOGGER.debug(traceback.format_exc())
            raise

        # try the regex parser next
        try:
            from kl3m_data.parsers.mdtransformer.regex_parser import RegexHTMLParser

            parser = RegexHTMLParser(self.config)
            output = parser.parse(html_str, **kwargs)
            if output is not None and len(output.strip()) > 0:
                return output.strip()
        except ImportError:
            LOGGER.warning(
                "Regex parser not available, falling back to default parser."
            )
            pass
        except Exception as e:
            LOGGER.error(f"Error parsing HTML with Regex: {e}")
            LOGGER.debug(traceback.format_exc())
            raise


# main method cli
if __name__ == "__main__":
    import argparse

    # setup argument parser
    parser = argparse.ArgumentParser(description="AutoParser CLI")
    # simple mode
    # disable links
    # disable images
    # input html
    # output markdown if file provided

    # 1 or 2 paths required
    parser.add_argument(
        "paths",
        type=Path,
        help="The path to the input HTML file.",
        nargs="+",
    )

    # simple mode
    parser.add_argument(
        "--simple",
        action="store_true",
        help="Use simple mode for parsing.",
    )

    # disable links
    parser.add_argument(
        "--disable_links",
        action="store_true",
        help="Disable links in the output Markdown.",
    )

    # disable images
    parser.add_argument(
        "--disable_images",
        action="store_true",
        help="Disable images in the output Markdown.",
    )

    args = parser.parse_args()

    # create ParserConfig from it
    config = ParserConfig(
        simple_mode=args.simple,
        output_links=not args.disable_links,
        output_images=not args.disable_images,
    )

    # check if one or two paths provided
    if len(args.paths) == 1:
        html_str = args.paths[0].read_text()
        args.output_path = None
    elif len(args.paths) == 2:
        html_str = args.paths[0].read_text()
        args.output_path = args.paths[1]
    else:
        raise ValueError("Invalid number of paths provided.")

    # create AutoParser
    parser = AutoParser(config=config)

    # parse the input html
    markdown_str = parser.parse(html_str)

    # write output markdown
    if args.output_path:
        args.output_path.write_text(markdown_str)
    else:
        print(markdown_str)
