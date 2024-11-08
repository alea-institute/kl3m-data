"""
CLI for the KL3M Data project.
"""

# imports
import argparse
import datetime

# packages
from rich.progress import (
    Progress,
    TextColumn,
    TaskProgressColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

# project imports
from kl3m_data.sources.base_source import BaseSource, SourceDownloadStatus
from kl3m_data.sources.eu.eu_oj.eu_oj import EUOJSource
from kl3m_data.sources.uk.uk_legislation.uk_legislation_source import (
    UKLegislationSource,
)
from kl3m_data.sources.us.dockets.dockets_source import DocketsSource
from kl3m_data.sources.us.dotgov.dotgov_source import DotGovDocSource
from kl3m_data.sources.us.ecfr.ecfr_source import ECFRSource
from kl3m_data.sources.us.edgar.edgar_source import EDGARSource
from kl3m_data.sources.us.fdlp import FDLPSource
from kl3m_data.sources.us.fr.fr_source import FRSource
from kl3m_data.sources.us.govinfo import GovInfoSource
from kl3m_data.sources.us.recap.recap_source import RECAPSource
from kl3m_data.sources.us.recap_docs.recap_docs_source import RECAPDocSource
from kl3m_data.sources.us.reg_docs.reg_docs_source import RegulationsDocSource
from kl3m_data.sources.us.usc import USCSource
from kl3m_data.sources.us.uspto_patents.uspto_patents_source import USPTOPatentSource


# pylint: disable=too-many-return-statements
def get_source(source_id: str, **kwargs) -> BaseSource:
    """
    Get a source based on the given source ID.

    Args:
        source_id: The source ID.

    Returns:
        BaseSource: The source object.
    """
    if source_id in ("fdlp", "us/fdlp"):
        return FDLPSource(**kwargs)
    if source_id in ("govinfo", "us/govinfo"):
        return GovInfoSource(**kwargs)
    if source_id in ("usc", "us/usc"):
        return USCSource(**kwargs)
    if source_id in ("ecfr", "us/ecfr"):
        return ECFRSource(**kwargs)
    if source_id in ("fr", "us/fr"):
        return FRSource(**kwargs)
    if source_id in ("edgar", "us/edgar"):
        return EDGARSource(**kwargs)
    if source_id in ("recap", "us/recap"):
        return RECAPSource(**kwargs)
    if source_id in ("recap_docs", "us/recap_docs"):
        return RECAPDocSource(**kwargs)
    if source_id in ("uspto_patents", "us/uspto_patents"):
        return USPTOPatentSource(**kwargs)
    if source_id in ("eu_oj", "eu/eu_oj"):
        return EUOJSource(**kwargs)
    if source_id in ("dockets", "us/dockets"):
        return DocketsSource(**kwargs)
    if source_id in ("reg_docs", "us/reg_docs"):
        return RegulationsDocSource(**kwargs)
    if source_id in ("dotgov", "us/dotgov"):
        return DotGovDocSource(**kwargs)
    if source_id in ("ukleg", "uk/ukleg"):
        return UKLegislationSource(**kwargs)
    raise ValueError(f"Invalid source ID: {source_id}")


def source_download_id(
    source: BaseSource, document_id: str, **kwargs
) -> SourceDownloadStatus:
    """
    Download data from the given source with a progress bar.

    Args:
        source: The data source to download from.
        document_id: The document ID to download.
        **kwargs: Additional keyword arguments for the download

    Returns:
        bool: Whether the download was successful.
    """
    return source.download_id(document_id, **kwargs)


def source_download_date(source: BaseSource, date: datetime.date, **kwargs) -> None:
    """
    Download data from the given source with a progress bar.

    Args:
        source: The data source to download from.
        date: The date to download.
        **kwargs: Additional keyword arguments for the download

    Returns:
        None
    """
    progress_columns = [
        TextColumn("[bold blue]{task.description}"),
        TextColumn("[progress.percentage]{task.completed}/{task.total}"),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        TextColumn("[bold blue]{task.fields[extra]}"),
    ]
    with Progress(*progress_columns) as progress:
        download_task = progress.add_task(
            f"[bold blue]Downloading {source.metadata.dataset_id}...",
            total=None,
            extra="{}",
        )
        for status in source.download_date(date, **kwargs):
            progress.update(
                download_task,
                completed=status.current,
                total=status.total,
                advance=1,
                description=status.message,
                extra=status.extra,
            )
            if status.message:
                progress.console.log(status.message)


def source_download_date_range(
    source: BaseSource, start_date: datetime.date, end_date: datetime.date, **kwargs
) -> None:
    """
    Download data from the given source with a progress bar.

    Args:
        source: The data source to download from.
        start_date: The start date to download.
        end_date: The end date to download.
        **kwargs: Additional keyword arguments for the download

    Returns:
        None
    """
    progress_columns = [
        TextColumn("[bold blue]{task.description}"),
        TextColumn("[progress.percentage]{task.completed}/{task.total}"),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        TextColumn("[bold blue]{task.fields[extra]}"),
    ]
    with Progress(*progress_columns) as progress:
        download_task = progress.add_task(
            f"[bold blue]Downloading {source.metadata.dataset_id}...",
            total=None,
            extra="{}",
        )
        for status in source.download_date_range(start_date, end_date, **kwargs):
            progress.update(
                download_task,
                completed=status.current,
                total=status.total,
                advance=1,
                description=status.message,
                extra=status.extra,
            )
            if status.message:
                progress.console.log(status.message)


def source_download_all(source: BaseSource, **kwargs) -> None:
    """
    Download all data from the given source with a progress bar.

    Args:
        source: The data source to download from.
        **kwargs: Additional keyword arguments for the download

    Returns:
        None
    """
    progress_columns = [
        TextColumn("[bold blue]{task.description}"),
        TextColumn("[progress.percentage]{task.completed}/{task.total}"),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        TextColumn("[bold blue]{task.fields[extra]}"),
    ]
    with Progress(*progress_columns) as progress:
        download_task = progress.add_task(
            f"[bold blue]Downloading {source.metadata.dataset_id}...",
            total=None,
            extra="{}",
        )
        for status in source.download_all(**kwargs):
            progress.update(
                download_task,
                completed=status.current,
                total=status.total,
                advance=1,
                description=status.message,
                extra=status.extra,
            )
            if status.message:
                progress.console.log(status.message)


def main() -> None:
    """
    Main entry point for the KL3M Data CLI.
    Parses command line arguments and executes the appropriate function.

    Example:
        cli.py fdlp download_id 1234
        cli.py fdlp download_all --update
    """
    parser = argparse.ArgumentParser(description="kl3m data CLI")
    parser.add_argument("source_id", type=str, help="The source ID.")
    parser.add_argument("command", type=str, help="The command to execute.")
    parser.add_argument("args", nargs="*", help="The arguments for the command.")
    args = parser.parse_args()

    # collect the command kwargs
    kwargs = {}
    for arg in args.args:
        if "=" in arg:
            key, value = arg.split("=")
            kwargs[key] = value

    # get the source
    source = get_source(args.source_id, **kwargs)

    # execute the command
    if args.command == "download_id":
        # ensure we have a document ID in kwargs
        if "document_id" not in kwargs:
            raise ValueError("Missing document ID.")
        source_download_id(source, **kwargs)
    elif args.command == "download_date":
        # ensure we have a date in kwargs
        if "date" not in kwargs:
            raise ValueError("Missing date.")
        date = datetime.date.fromisoformat(kwargs.pop("date"))
        source_download_date(source, date, **kwargs)
    elif args.command == "download_date_range":
        # ensure we have start and end dates in kwargs
        if "start_date" not in kwargs:
            raise ValueError("Missing start date.")
        if "end_date" not in kwargs:
            raise ValueError("Missing end date.")
        start_date = datetime.date.fromisoformat(kwargs.pop("start_date"))
        end_date = datetime.date.fromisoformat(kwargs.pop("end_date"))
        source_download_date_range(source, start_date, end_date, **kwargs)
    elif args.command == "download_all":
        source_download_all(source, **kwargs)
    else:
        raise ValueError(f"Invalid command: {args.command}")


if __name__ == "__main__":
    main()
