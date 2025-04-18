"""CLI for processing and uploading parquet-based files and datasets.

For filtering, common ranges are:
 - score <= 10.0
 - adjusted_score <= 0.003
"""

# imports
import argparse
import gzip
import hashlib
import json
import random
from typing import Callable, Generator, Optional

# packages
from datasets import Dataset, load_dataset
from rich.progress import (
    Progress,
    TextColumn,
    TaskProgressColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from huggingface_hub import hf_api
from huggingface_hub.errors import RepositoryNotFoundError

from kl3m_data.metrics.quality_metrics import get_metrics

# project imports
from kl3m_data.utils.parquet_utils import serialize_document, deserialize_document_bytes
from kl3m_data.utils.s3_utils import (
    S3Stage,
    get_s3_client,
    iter_prefix,
    put_object_bytes,
    check_object_exists,
    get_object_bytes,
    iter_prefix_shard,
    get_stage_prefix,
    get_parquet_key,
    get_index_key,
)
from kl3m_data.logger import LOGGER


def convert_dataset(
    dataset_id: str,
    max_size: int = 1024 * 1024 * 1,
    clobber: bool = False,
    shard_prefix: Optional[str] = None,
) -> None:
    """
    Convert dataset JSON representations to parquet format.

    Args:
        dataset_id (str): The dataset ID to convert
        max_size (int): Maximum file size in bytes to convert
        clobber (bool): Whether to overwrite existing parquet files
        shard_prefix (str): Optional prefix for sharding

    Returns:
        None
    """
    # setup paths using utility functions
    if dataset_id == "all":
        representation_path = get_stage_prefix(S3Stage.REPRESENTATIONS)
        parquet_path = get_stage_prefix(S3Stage.PARQUET)
        dataset_id = "all"
    else:
        representation_path = get_stage_prefix(S3Stage.REPRESENTATIONS, dataset_id)
        parquet_path = get_stage_prefix(S3Stage.PARQUET, dataset_id)

    # get s3 client
    s3_client = get_s3_client()

    # setup progress tracking
    progress_columns = [
        TextColumn("[bold blue]{task.description}"),
        TextColumn("{task.completed}/{task.total}"),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        TextColumn("[bold]{task.fields[status]}"),
    ]

    # track stats
    converted = 0
    skipped = 0
    failed = 0

    with Progress(*progress_columns) as progress:
        task = progress.add_task(
            f"Converting {dataset_id}",
            total=None,
            status=f"converted: {converted} skipped: {skipped} failed: {failed}",
        )

        if shard_prefix:
            path_iterator = iter_prefix_shard(
                s3_client, "data.kl3m.ai", representation_path, shard_prefix
            )
        else:
            path_iterator = iter_prefix(s3_client, "data.kl3m.ai", representation_path)

        for object_key in path_iterator:
            progress.update(
                task,
                advance=1,
                status=f"converted: {converted} skipped: {skipped} failed: {failed}",
            )

            # get parquet key using utility function
            parquet_key = get_parquet_key(object_key)

            # check if already exists
            if not clobber:
                if check_object_exists(s3_client, "data.kl3m.ai", parquet_key):
                    skipped += 1
                    continue

            # get representation data
            try:
                representation_buffer = get_object_bytes(
                    s3_client, "data.kl3m.ai", object_key
                )

                if len(representation_buffer) > max_size:
                    skipped += 1
                    continue

                representation_data = json.loads(representation_buffer)
                documents = representation_data.get("documents", [])

                if len(documents) < 1:
                    skipped += 1
                    continue

                # convert to parquet
                parquet_bytes = serialize_document(documents[0])
                if parquet_bytes is None:
                    failed += 1
                    continue

                # upload parquet file
                put_object_bytes(s3_client, "data.kl3m.ai", parquet_key, parquet_bytes)
                LOGGER.info(f"Converted {object_key} to {parquet_key}")

                converted += 1

            except Exception as e:
                failed += 1
                progress.console.log(f"Error converting {object_key}: {e}")

        # Final stats
        progress.console.log(
            f"Finished {dataset_id}: "
            f"converted={converted} skipped={skipped} failed={failed}"
        )


def get_sample_batch(
    datasets: str | list[str],
    filter_method: Optional[Callable[[dict], bool]] = None,
    limit: Optional[int] = None,
    shuffle: bool = True,
    batch_size: int = 100,  # Process in batches for better throughput
) -> Generator[dict, None, None]:
    """
    Get sample documents from datasets with optimized performance.

    Args:
        datasets: Dataset ID(s) to sample from
        filter_method: Optional filter function for documents
        limit: Maximum number of samples to return
        shuffle: Whether to randomize samples
        batch_size: Number of objects to process in a batch

    Yields:
        dict: Document samples
    """
    # Get an optimized S3 client with better connection pools
    s3_client = get_s3_client(
        pool_size=25,  # Increased connection pool
        connect_timeout=5,
        read_timeout=30,
        retry_count=3,
    )

    # Ensure list of datasets
    if isinstance(datasets, str):
        datasets = [datasets]

    # Load indexes with improved retry logic
    index_objects = []
    for dataset in datasets:
        index_path = get_index_key(dataset)
        index_bytes = get_object_bytes(
            s3_client, "data.kl3m.ai", index_path, retry_count=3, retry_delay=1.0
        )
        if index_bytes is not None:
            index_data = json.loads(gzip.decompress(index_bytes))
            index_objects.extend(index_data.get("objects", []))

    if shuffle:
        random.shuffle(index_objects)

    # Limit the number of objects to process if needed
    if limit:
        # We might need more objects than limit since some might be filtered out
        # Use a multiplier to ensure we have enough objects
        multiplier = 2  # Adjust based on expected filter rate
        index_objects = index_objects[: limit * multiplier]

    # Iterate objects in batches for better performance
    total = 0
    total_processed = 0

    # Process in batches
    for i in range(0, len(index_objects), batch_size):
        batch = index_objects[i : i + batch_size]
        valid_keys = []

        # First filter and check existence in parallel (future optimization: use ThreadPoolExecutor)
        for object_key in batch:
            total_processed += 1

            if filter_method and not filter_method(object_key):
                continue

            parquet_key = get_parquet_key(object_key)

            # Check if parquet file exists with improved check
            if check_object_exists(
                s3_client, "data.kl3m.ai", parquet_key, retry_count=2
            ):
                valid_keys.append((parquet_key, object_key))

        # Then process valid keys
        for parquet_key, object_key in valid_keys:
            object_dataset = parquet_key.split("/")[1]

            try:
                # Get object with optimized retry logic
                parquet_bytes = get_object_bytes(
                    s3_client,
                    "data.kl3m.ai",
                    parquet_key,
                    retry_count=3,
                    retry_delay=0.5,
                )

                if parquet_bytes is None:
                    continue

                document = deserialize_document_bytes(parquet_bytes)

                # Yield tokens for each mime type
                for mime_type in document.get("representations", {}):
                    # Exclude edgar uu data
                    if "edgar" in object_dataset:
                        if document["representations"][mime_type][0] == 47842:
                            continue

                    yield {
                        "identifier": document["identifier"],
                        "dataset": object_dataset,
                        "mime_type": mime_type,
                        "tokens": document["representations"][mime_type],
                    }
                    total += 1

                    if limit and total >= limit:
                        return

            except Exception as e:
                LOGGER.warning(f"Error processing {parquet_key}: {e}")

        # If we've reached the limit, stop processing batches
        if limit and total >= limit:
            return


def main() -> None:
    """
    Main CLI entry point for parquet processing.
    """
    parser = argparse.ArgumentParser(
        description="Process data files to/from parquet format"
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Convert command
    convert_parser = subparsers.add_parser("convert")
    convert_parser.add_argument("dataset", help="Dataset ID to convert")
    convert_parser.add_argument(
        "--max-size",
        type=int,
        default=8,
        help="Max file size in bytes to convert",
    )
    convert_parser.add_argument(
        "--clobber", action="store_true", help="Overwrite existing parquet files"
    )
    convert_parser.add_argument(
        "--shard-prefix", help="Prefix for sharding", default=None
    )

    # upload command
    upload_parser = subparsers.add_parser("upload")
    upload_parser.add_argument(
        "datasets", help="Comma-separated dataset IDs to sample from"
    )
    upload_parser.add_argument("output_name", help="Output dataset name")
    upload_parser.add_argument("--limit", type=int, help="Max samples to generate")
    upload_parser.add_argument(
        "--no-shuffle", action="store_true", help="Disable random sampling"
    )
    upload_parser.add_argument(
        "--starts-with", help="Filter objects starting with text"
    )
    upload_parser.add_argument("--contains", help="Filter objects containing text")
    upload_parser.add_argument("--ends-with", help="Filter objects ending with text")

    # quality filter command is like this:
    # filter [input-dataset(s)] [output-dataset] [score]
    filter_parser = subparsers.add_parser("filter")
    filter_parser.add_argument("datasets", help="Dataset ID or * prefix to sample from")
    filter_parser.add_argument("output_name", help="Output dataset name")
    filter_parser.add_argument("score", type=float, help="Score to filter by")
    filter_parser.add_argument(
        "--adjusted",
        action="store_true",
        help="Use adjusted score for filtering",
    )
    filter_parser.add_argument(
        "--num-hash-tokens",
        type=int,
        default=1024,
        help="Number of tokens to hash for deduplication",
    )
    filter_parser.add_argument(
        "--clobber", action="store_true", help="Overwrite existing dataset"
    )

    # parse all
    args = parser.parse_args()

    if args.command == "convert":
        convert_dataset(
            args.dataset, args.max_size * 1024 * 1024, args.clobber, args.shard_prefix
        )

    elif args.command == "upload":
        # Validate filters
        filter_count = sum(
            1
            for f in [args.starts_with, args.contains, args.ends_with]
            if f is not None
        )
        if filter_count > 1:
            parser.error("Only one filter type can be used")

        # Setup filter function
        filter_fn = None
        if args.starts_with:
            # filter by starts with
            def filter_fn(x: dict[str, str]) -> bool:
                return x["identifier"].lower().startswith(args.starts_with.lower())
        elif args.contains:
            # filter by contains
            def filter_fn(x: dict[str, str]) -> bool:
                return args.contains.lower() in x["identifier"].lower()
        elif args.ends_with:
            # filter by ends with
            def filter_fn(x: dict[str, str]) -> bool:
                return x["identifier"].lower().endswith(args.ends_with.lower())

        dataset = Dataset.from_generator(
            get_sample_batch,
            gen_kwargs={
                "datasets": args.datasets.split(","),
                "filter_method": filter_fn,
                "limit": args.limit,
                "shuffle": not args.no_shuffle,
            },
        )

        # Push to hub
        dataset.push_to_hub(args.output_name)
    elif args.command == "filter":
        # get score threshold
        score_threshold = args.score
        if score_threshold <= 0.0:
            raise ValueError("Score threshold must be float > 0.0")

        # check if the output name already exists
        try:
            if hf_api.dataset_info(args.output_name) is not None and not args.clobber:
                raise ValueError(
                    f"Output dataset {args.output_name} already exists. Choose a different name or run with --clobber to overwrite."
                )
        except RepositoryNotFoundError:
            # need to create for the first time
            pass

        # get list of datasets
        dataset_name = args.datasets
        if dataset_name.endswith("*"):
            dataset_list = []
            for dataset in hf_api.list_datasets(author="alea-institute"):
                if dataset.id.startswith(dataset_name[:-1]):
                    dataset_list.append(dataset.id)
        else:
            dataset_list = [dataset_name]

        score_type = "adjusted_score" if args.adjusted else "score"

        # load and filter each
        def yield_records():
            seen_hashes = set()
            for dataset_id in dataset_list:
                filter_stats = {
                    "dataset": dataset_id,
                    "included": 0,
                    "excluded_score": 0,
                    "excluded_empty": 0,
                    "excluded_duplicate": 0,
                }
                try:
                    current_dataset = load_dataset(
                        dataset_id, split="train", streaming=True
                    )

                    for row in current_dataset:
                        try:
                            if len(row.get("tokens", [])) == 0:
                                filter_stats["excluded_empty"] += 1
                                continue

                            metrics = get_metrics(row)
                            if metrics[score_type] <= score_threshold:
                                # check blake2b hash of tokens
                                tokens_to_hash = row.get("tokens", [])[
                                    0 : args.num_hash_tokens
                                ]
                                token_hash = hashlib.blake2b(
                                    ",".join(map(str, tokens_to_hash)).encode(),
                                    digest_size=16,
                                ).hexdigest()

                                # check for dupe
                                if token_hash in seen_hashes:
                                    filter_stats["excluded_duplicate"] += 1
                                    continue
                                else:
                                    filter_stats["included"] += 1
                                    seen_hashes.add(token_hash)

                                # yield if not seen
                                yield {
                                    "identifier": row.get("identifier"),
                                    "dataset": row.get("dataset"),
                                    "mime_type": row.get("mime_type"),
                                    "score": metrics.get("score"),
                                    "tokens": row.get("tokens", []),
                                }
                            else:
                                filter_stats["excluded_score"] += 1
                        except Exception as e:
                            print(e)
                            continue
                except Exception as e:
                    print(e)
                    continue

                # print filter stats
                print(filter_stats)

        # create dataset
        dataset = Dataset.from_generator(yield_records)

        # push to hub
        dataset.push_to_hub(args.output_name)


if __name__ == "__main__":
    main()
