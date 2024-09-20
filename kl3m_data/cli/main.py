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
from kl3m_data.sources.us.fdlp import FDLPSource
from kl3m_data.sources.us.govinfo import GovInfoSource
from kl3m_data.sources.us.usc import USCSource


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
    elif args.command == "download_all":
        source_download_all(source, **kwargs)
    else:
        raise ValueError(f"Invalid command: {args.command}")


if __name__ == "__main__":
    main()
