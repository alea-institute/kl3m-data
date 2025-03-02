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
from kl3m_data.utils.s3_utils import (
    iter_prefix,
    get_s3_client,
    put_object_path,
)


def build_index() -> None:
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
    arg_parser.add_argument("command", type=str, choices=["build_index", "build_table"])
    arg_parser.add_argument("--output", type=Path, default=None)
    arg_parser.add_argument("--format", type=str, default="csv")
    arg_parser.add_argument("--counts", action="store_true")
    arg_parser.add_argument("--dataset_prefix", type=str, default="kl3m-")
    args = arg_parser.parse_args()

    if args.command == "build_index":
        build_index()
        exit(0)
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
