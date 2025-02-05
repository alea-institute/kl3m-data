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
    get_s3_client,
    iter_prefix,
    put_object_bytes,
    check_object_exists,
    get_object_bytes,
)
from kl3m_data.logger import LOGGER


def convert_dataset(
    dataset_id: str, max_size: int = 1024 * 1024 * 1, clobber: bool = False
) -> None:
    """
    Convert dataset JSON representations to parquet format.

    Args:
        dataset_id (str): The dataset ID to convert
        max_size (int): Maximum file size in bytes to convert
        clobber (bool): Whether to overwrite existing parquet files

    Returns:
        None
    """
    # setup paths
    representation_path = f"representations/{dataset_id}/"
    parquet_path = f"parquet/{dataset_id}/"

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

        path_iterator = iter_prefix(s3_client, "data.kl3m.ai", representation_path)

        for object_key in path_iterator:
            progress.update(
                task,
                advance=1,
                status=f"converted: {converted} skipped: {skipped} failed: {failed}",
            )

            # get parquet key
            parquet_key = object_key.replace(representation_path, parquet_path)[
                : -len(".json")
            ]

            # check if already exists
            if (
                check_object_exists(s3_client, "data.kl3m.ai", parquet_key)
                and not clobber
            ):
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
) -> Generator[dict, None, None]:
    """
    Get sample documents from datasets.

    Args:
        datasets: Dataset ID(s) to sample from
        filter_method: Optional filter function for documents
        limit: Maximum number of samples to return
        shuffle: Whether to randomize samples

    Yields:
        dict: Document samples
    """
    s3_client = get_s3_client()

    # ensure list of datasets
    if isinstance(datasets, str):
        datasets = [datasets]

    # load indexes
    index_objects = []
    for dataset in datasets:
        index_path = f"index/{dataset}.json.gz"
        index_bytes = get_object_bytes(s3_client, "data.kl3m.ai", index_path)
        index_data = json.loads(gzip.decompress(index_bytes))
        index_objects.extend(index_data.get("objects", []))

    if shuffle:
        random.shuffle(index_objects)

    # iterate objects
    total = 0
    for object_key in index_objects:
        if filter_method and not filter_method(object_key):
            continue

        parquet_key = object_key.replace("representations/", "parquet/")[
            : -len(".json")
        ]

        if not check_object_exists(s3_client, "data.kl3m.ai", parquet_key):
            continue

        object_dataset = parquet_key.split("/")[1]

        try:
            parquet_bytes = get_object_bytes(s3_client, "data.kl3m.ai", parquet_key)
            document = deserialize_document_bytes(parquet_bytes)

            # yield tokens for each mime type
            for mime_type in document.get("representations", {}):
                # exclude edgar uu data
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
            print(f"Error processing {parquet_key}: {e}")


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
        default=1024 * 1024,
        help="Max file size in bytes to convert",
    )
    convert_parser.add_argument(
        "--clobber", action="store_true", help="Overwrite existing parquet files"
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
        convert_dataset(args.dataset, args.max_size, args.clobber)

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
