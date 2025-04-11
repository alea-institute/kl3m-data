"""
Retrieve, calculate, or export various information about the dataset(s)
"""

# imports
import argparse
import gzip
import io
import json
import statistics
from pathlib import Path

# packages
import polars as pl
import tqdm
from datasets import load_dataset
from huggingface_hub import hf_api

# project
from kl3m_data.utils.parquet_utils import deserialize_document_bytes
from kl3m_data.utils.s3_utils import (
    iter_prefix,
    get_s3_client,
    get_object_bytes,
    put_object_path,
)


def build_document_index() -> None:
    """
    Build an index with the list of all available source documents on S3.

    Returns:
        None
    """
    # set up client and iterator
    client = get_s3_client()
    prog_bar = tqdm.tqdm(iter_prefix(client, "data.kl3m.ai", "documents/"))

    with gzip.open("documents.index.gz", "wt", encoding="utf-8") as index_file:
        for key in prog_bar:
            index_file.write(key[len("documents/") :] + "\n")

    # push this to s3
    put_object_path(
        client=client,
        bucket="data.kl3m.ai",
        key="documents/index.gz",
        path="documents.index.gz",
    )


def build_representation_index() -> None:
    """
    Build an index with the list of all available source documents on S3.

    Returns:
        None
    """
    # set up client and iterator
    client = get_s3_client()
    prog_bar = tqdm.tqdm(iter_prefix(client, "data.kl3m.ai", "representations/"))

    with gzip.open("representations.index.gz", "wt", encoding="utf-8") as index_file:
        for key in prog_bar:
            index_file.write(key[len("representations/") :] + "\n")

    # push this to s3
    put_object_path(
        client=client,
        bucket="data.kl3m.ai",
        key="representations/index.gz",
        path="representations.index.gz",
    )


def build_parquet_index() -> None:
    """
    Build an index with the list of all available source documents on S3.

    Returns:
        None
    """
    # set up client and iterator
    client = get_s3_client()
    prog_bar = tqdm.tqdm(iter_prefix(client, "data.kl3m.ai", "parquet/"))

    with gzip.open("parquet.index.gz", "wt", encoding="utf-8") as index_file:
        for key in prog_bar:
            index_file.write(key[len("parquet/") :] + "\n")

    # push this to s3
    put_object_path(
        client=client,
        bucket="data.kl3m.ai",
        key="parquet/index.gz",
        path="parquet.index.gz",
    )


def count_parquet_index(prefix: str = "/") -> None:
    """
    Count the number of parquet files in the index.

    Returns:
        None
    """
    # set up client and iterator
    client = get_s3_client()
    parquet_path = "parquet/" + prefix.lstrip("/")
    prog_bar = tqdm.tqdm(iter_prefix(client, "data.kl3m.ai", parquet_path))

    # count the number of parquet files
    total_token_count = 0
    total_docs = 0
    mime_type_token_count = {}
    token_count_list = []
    # token_entropy_list = []
    for key in prog_bar:
        # load bytes
        try:
            object_data = deserialize_document_bytes(
                get_object_bytes(
                    client=client,
                    bucket="data.kl3m.ai",
                    key=key,
                )
            )

            for mime_type, tokens in object_data.get("representations", {}).items():
                # check if mime type in count
                if mime_type not in mime_type_token_count:
                    mime_type_token_count[mime_type] = 0

                # get entropy
                num_tokens = len(tokens)
                # sample_p = Counter(tokens)
                # entropy = -sum(p / num_tokens * math.log2(p / num_tokens) for p in sample_p.values())

                # add to count
                token_count_list.append(num_tokens)
                # token_entropy_list.append(entropy)
                mime_type_token_count[mime_type] += len(tokens)
                total_token_count += len(tokens)
                total_docs += 1
            prog_bar.set_postfix(
                {
                    "tokens": total_token_count,
                    "mime_types": len(mime_type_token_count),
                    "mean_tokens": total_token_count / max(total_docs, 1),
                }
            )
        except Exception as e:
            print(f"Error loading {key}: {e}")
            continue

    # write out into token_count.json
    with gzip.open("token_count.json.gz", "wt", encoding="utf-8") as output_file:
        output_file.write(
            json.dumps(
                {
                    "statistics": {
                        "total_tokens": total_token_count,
                        "total_docs": total_docs,
                        "mean_tokens": statistics.mean(token_count_list),
                        "median_tokens": statistics.median(token_count_list),
                        "max_tokens": max(token_count_list),
                        # "mean_entropy": statistics.mean(token_entropy_list),
                        # "median_entropy": statistics.median(token_entropy_list),
                        # "max_entropy": max(token_entropy_list),
                    },
                    "mime_type_counts": mime_type_token_count,
                    "token_counts": token_count_list,
                    # "token_entropies": token_entropy_list,
                }
            )
        )


def get_token_statistics(dataset_id: str) -> dict[str, int | float]:
    """
    Get the token statistics for a dataset.

    Args:
        dataset_id (str): The dataset id.

    Returns:
        dict: The token statistics.
    """
    # load dataset
    dataset = load_dataset(dataset_id, split="train", streaming=False)

    # use map to add
    def count_tokens(sample: dict):
        return {"num_tokens": [len(r) for r in sample]}

    # map
    dataset = dataset.map(
        count_tokens, batched=True, input_columns=["tokens"], remove_columns=["tokens"]
    )

    # get statistics on the num_tokens column:
    # mean, median, min, max, std, sum
    return {
        "total_tokens": sum(dataset["num_tokens"]),
        "mean_tokens": statistics.mean(dataset["num_tokens"]),
        "median_tokens": statistics.median(dataset["num_tokens"]),
        "min_tokens": min(dataset["num_tokens"]),
        "max_tokens": max(dataset["num_tokens"]),
        "std_tokens": statistics.stdev(dataset["num_tokens"]),
    }


def get_datasets(
    dataset_prefix: str = "kl3m-", count_tokens: bool = False
) -> list[dict]:
    """
    Get the dataset info from the Huggingface hub.

    Args:
        dataset_prefix (str): The dataset prefix.
        count_tokens (bool): Whether to count the number of samples.

    Returns:
        dict: The dataset info.
    """
    datasets = []
    for dataset in hf_api.list_datasets(author="alea-institute", full=True):
        # filter by id
        if dataset.id.startswith("alea-institute/" + dataset_prefix):
            try:
                # print(dataset.cardData.dataset_info)
                num_samples = dataset.cardData.dataset_info.get("splits", [{}])[0].get(
                    "num_examples", 0
                )
                download_size = dataset.cardData.dataset_info.get("download_size", 0)
            except AttributeError:
                num_samples = 0

            record = {
                "id": dataset.id,
                "samples": num_samples,
                "download_size": download_size,
                "last_modified": dataset.lastModified.isoformat(),
                "is_sft": "-sft-" in dataset.id,
                "is_filtered": "-filter-" in dataset.id,
                "is_eval": "-eval-" in dataset.id,
            }
            try:
                if count_tokens:
                    record.update(get_token_statistics(dataset.id))
            except Exception as e:
                print(f"Error getting token statistics for {dataset.id}: {e}")

            # append to list
            datasets.append(record)

    # return sorted by id
    return sorted(datasets, key=lambda x: x["id"])


if __name__ == "__main__":
    # commands supported:
    # - print/export datasets as csv
    # - print/export datasets as jsonl
    arg_parser = argparse.ArgumentParser()
    # command
    arg_parser.add_argument(
        "command",
        type=str,
        choices=[
            "build_document_index",
            "build_representation_index",
            "build_parquet_index",
            "count_parquet_index",
            "build_table",
        ],
    )
    arg_parser.add_argument("--output", type=Path, default=None)
    arg_parser.add_argument("--format", type=str, default="csv")
    arg_parser.add_argument("--counts", action="store_true")
    arg_parser.add_argument("--dataset_prefix", type=str, default="kl3m-")
    args = arg_parser.parse_args()

    if args.command == "build_document_index":
        build_document_index()
    elif args.command == "build_representation_index":
        build_representation_index()
    elif args.command == "build_parquet_index":
        build_parquet_index()
    elif args.command == "count_parquet_index":
        count_parquet_index(args.dataset_prefix)
    elif args.command == "build_table":
        datasets = get_datasets(args.dataset_prefix, args.counts)
        df = pl.DataFrame(datasets)

        if args.output is not None:
            with open(args.output, "wt", encoding="utf-8") as f:
                if args.format == "jsonl":
                    for record in df.to_dicts():
                        f.write(json.dumps(record, ensure_ascii=False) + "\n")
                elif args.format == "csv":
                    df.write_csv(f)
                else:
                    raise ValueError(f"Unsupported format: {args.format}")
        else:
            if args.format == "jsonl":
                for record in df.to_dicts():
                    print(json.dumps(record, ensure_ascii=False))
            elif args.format == "csv":
                output_buffer = io.StringIO()
                df.write_csv(output_buffer)
                print(output_buffer.getvalue())
            else:
                raise ValueError(f"Unsupported format: {args.format}")
