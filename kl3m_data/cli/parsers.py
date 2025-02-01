"""
CLI for the KL3M Data parsing functionality.
"""

# imports
import argparse
import gzip
import json
from typing import Any, Optional

# packages
from rich.progress import (
    Progress,
    TextColumn,
    TaskProgressColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    SpinnerColumn,
)

# project imports
from kl3m_data.logger import LOGGER
from kl3m_data.parsers.parser import get_output_key, parse_object
from kl3m_data.utils.s3_utils import (
    check_object_exists,
    get_s3_client,
    iter_prefix,
    iter_prefix_shard,
    put_object_bytes,
    list_common_prefixes,
)


def parse_single(object_key: str, **kwargs: Any) -> None:
    """
    Parse a single object by key.

    Args:
        object_key: The S3 object key to parse
        **kwargs: Additional keyword arguments passed to parse_object
    """
    # get the s3 client
    s3_client = get_s3_client()

    # get output key and check if it exists
    output_key = get_output_key(object_key)
    if check_object_exists(s3_client, "data.kl3m.ai", output_key):
        LOGGER.info("Output key already exists: %s", output_key)
        return

    try:
        parsed_docs = parse_object(s3_client, "data.kl3m.ai", object_key, **kwargs)
        output_docs = []
        for doc in parsed_docs:
            if doc.success:
                LOGGER.info("Parsed object key=%s", object_key)
                output_docs.append(doc.to_json_dict())
            else:
                LOGGER.error("Error parsing object key=%s: %s", object_key, doc.error)

        # put the output object if we have at least one good document
        if len(output_docs) > 0:
            put_object_bytes(
                s3_client,
                "data.kl3m.ai",
                output_key,
                json.dumps({"documents": output_docs}).encode("utf-8"),
            )
    except Exception as e:
        LOGGER.error("Error processing object key=%s: %s", object_key, e)


def parse_serial(
    dataset_id: Optional[str] = None,
    shard_prefix: Optional[str] = None,
    clobber: bool = False,
    prefix: Optional[str] = None,
    **kwargs: Any,
) -> None:
    """
    Parse objects serially, with optional dataset_id and shard filtering.

    Args:
        dataset_id: Optional dataset ID to filter objects
        shard_prefix: Optional shard prefix to filter objects
        clobber: Whether to overwrite existing output keys
        prefix: Optional prefix to filter objects
        **kwargs: Additional keyword arguments passed to parse_object
    """
    # initialize S3 client
    s3_client = get_s3_client()

    # handle optional dataset id
    if dataset_id:
        dataset_paths = [f"documents/{dataset_id}/"]
    else:
        dataset_paths = ["documents/"]

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

        for dataset_path in dataset_paths:
            # update description for current dataset
            progress.update(task, description=f"Processing {dataset_path}")

            # get iterator based on shard prefix
            if prefix:
                full_dataset_path = dataset_path.rstrip("/") + "/" + prefix
            else:
                full_dataset_path = dataset_path

            if shard_prefix:
                objects = iter_prefix_shard(
                    s3_client, "data.kl3m.ai", full_dataset_path, shard_prefix
                )
            else:
                objects = iter_prefix(s3_client, "data.kl3m.ai", full_dataset_path)

            for object_key in objects:
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
                        s3_client, "data.kl3m.ai", object_key, **kwargs
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


def build_dataset_index(dataset_id: str):
    """
    Build an index of objects under an S3 prefix to be used for later
    sharding and shuffling.

    Args:
        dataset_id (str): The dataset ID.

    Returns:
        None
    """
    # set up the paths
    representation_path = f"representations/{dataset_id}/"
    index_path = f"index/{dataset_id}.json.gz"

    # get the paths
    c = get_s3_client()

    # track progress
    progress_columns = [
        TextColumn("[bold blue]{task.description}"),
        TextColumn("[progress.percentage]{task.completed}/{task.total}"),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        TimeRemainingColumn(),
    ]

    # track all objects
    all_objects = []
    total = 0
    # for object_key in prog_bar:
    with Progress(*progress_columns) as progress:
        task = progress.add_task(
            f"Building dataset index: {dataset_id}",
            total=None,
        )
        path_iterator = iter_prefix(c, "data.kl3m.ai", representation_path)
        for object_key in path_iterator:
            # update prog bar postfix
            progress.update(task, advance=1)
            progress.update(
                task,
                extra=f"total: {total}",
            )

            # add to the list
            all_objects.append(object_key)

            # increment total
            total += 1

    # output to json index file at target key path
    output_buffer = json.dumps({"objects": all_objects})
    put_object_bytes(
        c,
        "data.kl3m.ai",
        index_path,
        gzip.compress(output_buffer.encode("utf-8")),
    )

    # log destination key
    print(f"index={len(all_objects)}: s3://data.kl3m.ai/" + index_path)


def build_all_dataset_index():
    """
    Build an index of objects under an S3 prefix to be used for later
    sharding and shuffling.

    Args:
        None

    Returns:
        None
    """
    # get the paths
    c = get_s3_client()
    dataset_paths = list_common_prefixes(c, "data.kl3m.ai", "representations/")

    # run build index for each
    for dataset_path in dataset_paths:
        build_dataset_index(dataset_path.rstrip("/").split("/").pop())


def main() -> None:
    """
    Main entry point for the KL3M Data parser CLI.
    """
    parser = argparse.ArgumentParser(description="kl3m data parser CLI")
    parser.add_argument(
        "command",
        choices=["parse_single", "parse_serial", "build_index", "build_all_indexes"],
        help="Command to execute",
    )
    parser.add_argument("--key", help="Object key to parse (for parse_single)")
    parser.add_argument("--dataset-id", help="Dataset ID to process")
    parser.add_argument("--shard-prefix", help="Shard prefix to process")
    parser.add_argument(
        "--key-prefix", help="Prefix to filter objects (for parse_serial)"
    )
    parser.add_argument(
        "--max-size", type=int, default=8, help="Maximum file size in MB to process"
    )
    parser.add_argument(
        "--clobber",
        action="store_true",
        default=False,
        help="Overwrite existing output keys",
    )

    args = parser.parse_args()

    # collect the command kwargs
    kwargs = {
        "max_size": args.max_size * 1024 * 1024,  # convert MB to bytes
    }

    if args.command == "parse_single":
        if not args.key:
            raise ValueError("--key is required for parse_single command")
        parse_single(args.key, **kwargs)
    elif args.command == "parse_serial":
        parse_serial(
            args.dataset_id, args.shard_prefix, args.clobber, args.key_prefix, **kwargs
        )
    elif args.command == "build_index":
        if not args.dataset_id:
            raise ValueError("--dataset-id is required for build_index command")
        build_dataset_index(args.dataset_id)
    elif args.command == "build_all_indexes":
        build_all_dataset_index()


if __name__ == "__main__":
    main()
