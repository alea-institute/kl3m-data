# imports
import argparse
import hashlib
import json
import datetime
from typing import Optional

# packages
from datasets import Dataset
from rich.progress import (
    Progress,
    TextColumn,
    TaskProgressColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    SpinnerColumn,
)

# project imports
from kl3m_data.sources.us.edgar.edgar_source import EDGARSource
from kl3m_data.logger import LOGGER
from kl3m_data.parsers.parser import get_output_key, parse_object
from kl3m_data.utils.s3_utils import (
    check_object_exists,
    get_s3_client,
    iter_prefix,
    put_object_bytes,
)


def parse_form_type(
    form_type: str | list[str] | None = None,
    clobber: bool = False,
    cik: str | list[str] | None = None,
    suffix: Optional[str] = None,
    shard_prefix: Optional[str] = None,
    max_size: int = 4,
) -> None:
    """
    parse_serial but adapted to only process specific form types.

    Args:
        form_type: Optional form type or list of form types to filter on
        clobber: Whether to overwrite existing dataset
        cik: Optional CIK or list of CIKs to filter on
        suffix: Optional suffix for the output file
        shard_prefix: Optional shard key for the output file
        max_size: Maximum size of the file to parse in MB

    Returns:
        None
    """
    # initialize S3 client
    s3_client = get_s3_client()

    # create source
    source = EDGARSource()

    # setup progress tracking
    progress_columns = [
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        TextColumn("[progress.percentage]{task.completed}/{task.total}"),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        TextColumn("[bold]{task.fields[status]}"),
    ]

    # track counters
    good, bad, new = 0, 0, 0

    with Progress(*progress_columns) as progress:
        task = progress.add_task(
            "Processing documents...",
            total=None,
            status=f"good: {good} bad: {bad} new: {new}",
        )

        for filing in source.get_submissions(
            form_type=form_type,
            cik=cik,
        ):
            # get s3 path to test
            accession_number = filing.get("accessionNumber")
            cik_accession_path = accession_number.split("-")[0]
            object_prefix = f"documents/edgar/{cik_accession_path}/{accession_number}/"

            # check accession number blake2b hash prefix is shard_prefix is set
            if shard_prefix:
                accession_hash = hashlib.blake2b(
                    accession_number.encode("utf-8"), digest_size=8
                ).hexdigest()
                if not accession_hash.startswith(shard_prefix):
                    continue

            # iter through this
            for object_key in iter_prefix(s3_client, "data.kl3m.ai", object_prefix):
                # check suffix
                if suffix and not object_key.lower().endswith(suffix.lower()):
                    continue

                # update prog bar
                progress.update(task, advance=1)
                progress.update(
                    task,
                    description=f"Processing {object_key[0:40]:<40}...",
                    status=f"good: {good} bad: {bad} new: {new}",
                )

                # get output key and check if it exists
                output_key = get_output_key(object_key)
                if check_object_exists(s3_client, "data.kl3m.ai", output_key):
                    if not clobber:
                        LOGGER.info("Output key already exists: %s", output_key)
                        good += 1
                        continue
                    else:
                        LOGGER.info("Clobbering existing output key: %s", output_key)
                        new += 1

                try:
                    parsed_docs = parse_object(
                        s3_client,
                        "data.kl3m.ai",
                        object_key,
                        max_size=max_size * 1024 * 1024,
                    )
                    output_docs = []
                    for doc in parsed_docs:
                        if doc.success:
                            good += 1
                            new += 1
                            output_docs.append(doc.to_json_dict())
                        else:
                            bad += 1

                    if len(output_docs) > 0:
                        put_object_bytes(
                            s3_client,
                            "data.kl3m.ai",
                            output_key,
                            json.dumps({"documents": output_docs}).encode("utf-8"),
                        )

                except Exception as e:
                    LOGGER.error("Error processing object key=%s: %s", object_key, e)
                    bad += 1


def create_filing_index(
    form_type: str | list[str] | None = None,
    output_name: str = "kl3m-edgar-filings",
    cik: str | list[str] | None = None,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
    include_xbrl: bool = False,
    include_inline_xbrl: bool = False,
) -> None:
    """
    Create a HuggingFace dataset with records of EDGAR filings matching the specified
    form types.

    Args:
        form_type: Optional form type or list of form types to filter on
        output_name: Name of output dataset
        clobber: Whether to overwrite existing dataset
        cik: Optional CIK or list of CIKs to filter on
        start_date: Optional start date to filter on
        end_date: Optional end date to filter on
        include_xbrl: Whether to include XBRL filings
        include_inline_xbrl: Whether to include inline XBRL filings

    Returns:
        None
    """
    # create source
    source = EDGARSource()

    # stats
    def local_generator():
        """
        Generator function to yield EDGAR filings.
        """
        for filing in source.get_submissions(
            form_type=form_type,
            cik=cik,
            start_date=start_date,
            end_date=end_date,
            include_xbrl=include_xbrl,
            include_inline_xbrl=include_inline_xbrl,
        ):
            yield filing

    # convert to dataset
    dataset = Dataset.from_generator(
        local_generator,
    )

    # Push to hub
    dataset.push_to_hub(output_name)


def main() -> None:
    """
    Main entry point for EDGAR CLI.
    """
    parser = argparse.ArgumentParser(description="EDGAR-specific CLI tasks")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # create-filing-index command
    filing_parser = subparsers.add_parser("create-filing-index")
    filing_parser.add_argument(
        "output_name",
        help="Output dataset name",
        default="kl3m-index-edgar-filings",
    )
    filing_parser.add_argument(
        "--form-type", help="Form type(s) to filter on (comma-separated)"
    )
    filing_parser.add_argument("--cik", help="CIK(s) to filter on (comma-separated)")
    filing_parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    filing_parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    filing_parser.add_argument(
        "--include-xbrl", action="store_true", help="Include XBRL filings"
    )
    filing_parser.add_argument(
        "--include-inline-xbrl", action="store_true", help="Include inline XBRL filings"
    )

    # subparser for parsing a specific form type
    parse_parser = subparsers.add_parser("parse-form-type")
    parse_parser.add_argument(
        "form_type", help="Form type(s) to filter on (comma-separated)"
    )
    parse_parser.add_argument("--cik", help="CIK(s) to filter on (comma-separated)")
    parse_parser.add_argument("--suffix", help="Suffix to filter on")
    parse_parser.add_argument("--shard-prefix", help="Shard prefix to filter on")
    parse_parser.add_argument(
        "--max-size",
        type=int,
        default=4,
        help="Maximum size of the file to parse in MB",
    )
    parse_parser.add_argument(
        "--clobber", action="store_true", help="Overwrite existing dataset"
    )

    # parse arguments
    args = parser.parse_args()

    if args.command == "create-filing-index":
        form_type = (
            [form.strip() for form in args.form_type.split(",")]
            if args.form_type
            else None
        )
        cik = [c.strip() for c in args.cik.split(",")] if args.cik else None
        start_date = (
            datetime.datetime.strptime(args.start_date, "%Y-%m-%d").date()
            if args.start_date
            else None
        )
        end_date = (
            datetime.datetime.strptime(args.end_date, "%Y-%m-%d").date()
            if args.end_date
            else None
        )
        create_filing_index(
            form_type=form_type,
            cik=cik,
            start_date=start_date,
            end_date=end_date,
            include_xbrl=args.include_xbrl,
            include_inline_xbrl=args.include_inline_xbrl,
            output_name=args.output_name,
        )
    elif args.command == "parse-form-type":
        form_type = (
            [form.strip() for form in args.form_type.split(",")]
            if args.form_type
            else None
        )
        cik = [c.strip() for c in args.cik.split(",")] if args.cik else None
        parse_form_type(
            form_type=form_type,
            clobber=args.clobber,
            suffix=args.suffix,
            cik=cik,
            shard_prefix=args.shard_prefix,
            max_size=args.max_size,
        )


if __name__ == "__main__":
    main()
