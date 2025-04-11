"""
CLI for processing and uploading parquet-based files and datasets.

For filtering, common ranges are:
 - score <= 10.0
 - adjusted_score <= 0.003
"""

# imports
import argparse
import base64
import json
import math
import random
import statistics
import zlib
from collections import Counter
from pathlib import Path

# packages
import polars as pl
import tqdm
from huggingface_hub import hf_api
from datasets import load_dataset
from tokenizers import Tokenizer

# project imports
from kl3m_data.utils.s3_utils import (
    get_s3_client,
    get_object_bytes,
)

DEFAULT_DATASETS = sorted(
    [
        d.id
        for d in hf_api.list_datasets(author="alea-institute")
        if "kl3m-data-" in d.id
    ]
)


def get_token_distribution(tokens: list[int]) -> dict[str, float]:
    """Get the token distribution for a given text.

    Args:
        tokens (list): The tokens to analyze.

    Returns:
        list: The distribution of tokens.
    """
    # get the tokens
    sum_tokens = len(tokens)
    return {str(k): v / sum_tokens for k, v in Counter(tokens).items()}


def build_filter_model(samples_per_dataset: int = 1000) -> None:
    """Build a filter model for the datasets.

    Args:
        samples_per_dataset (int): The number of samples per dataset.
    """
    all_tokens = Counter()
    doc_tokens: list[dict[str, float]] = []
    doc_ids = []

    filter_datasets = DEFAULT_DATASETS.copy()
    random.shuffle(filter_datasets)

    for dataset_id in tqdm.tqdm(filter_datasets):
        try:
            d = load_dataset(dataset_id, split="train", streaming=True).shuffle()

            for row in d.take(samples_per_dataset):
                if row["mime_type"] in ["text/plain", "text/markdown"]:
                    doc_ids.append(row["identifier"])
                    doc_record = get_token_distribution(row["tokens"])
                    doc_tokens.append(doc_record)
                    all_tokens.update(doc_record)

            del d
        except Exception as e:
            print(f"Error processing {dataset_id}: {e}")

    # most common tokens
    tokens = list(k for k, _ in all_tokens.most_common(100))
    tokenizer = Tokenizer.from_pretrained("alea-institute/kl3m-004-128k-cased")
    for token in tokens:
        print(token, tokenizer.id_to_token(int(token)))

    # filter all doc_tokens records
    doc_tokens = [
        {k: doc_record.get(k, 0.0) for k in tokens} for doc_record in doc_tokens
    ]

    # get a polars dataframe from dicts
    df = pl.DataFrame(doc_tokens).fill_null(0.0).fill_nan(0.0)

    # get the centroid/mean
    centroid = df.mean().row(0)

    # get distances for all rows
    distances = [
        math.sqrt(sum((row[k] - centroid[k]) ** 2 for k in range(len(centroid))))
        for row in df.iter_rows()
    ]

    # get median distance
    median_distance = statistics.median(distances)
    tokenizer_name = "alea-institute/kl3m-004-128k-cased"
    tokens = [int(c) for c in df.columns]

    # write tokens, tokenizer name, and median distnace to filter model file
    with open(Path(__file__).parent / "filter_config.json", "wt") as output_file:
        output_file.write(
            json.dumps(
                {
                    "tokens": tokens,
                    "tokenizer_name": tokenizer_name,
                    "median_distance": median_distance,
                    "centroid": centroid,
                },
                indent=2,
            )
        )


def apply_filter(key: str) -> None:
    """Apply the filter to a given key.

    Args:
        key (str): The key to filter.

    Returns:
        bool: Whether the key passed the filter.
    """
    # load the model
    with open(Path(__file__).parent / "filter_config.json", "rt") as input_file:
        filter_config = json.load(input_file)

    # tokenizer
    tokenizer = Tokenizer.from_pretrained(filter_config["tokenizer_name"])
    filter_tokens = filter_config["tokens"]
    filter_median_distance = filter_config["median_distance"]
    filter_centroid = filter_config["centroid"]

    # get the object
    client = get_s3_client()

    # get the object
    object_bytes = get_object_bytes(client, "data.kl3m.ai", key)

    # print the object metadata excluding tokens and representations
    object_data = json.loads(object_bytes.decode("utf-8"))

    for document_number, document in enumerate(object_data.get("documents", [])):
        for mime_type, representation in document.get("representations", {}).items():
            content = zlib.decompress(
                base64.b64decode(representation["content"])
            ).decode()  # type: ignore
            tokens = tokenizer.encode(content).ids
            token_distribution = get_token_distribution(tokens)
            doc_record = [token_distribution.get(str(k), 0.0) for k in filter_tokens]

            # score with model
            distance = math.sqrt(
                sum(
                    (doc_record[i] - filter_centroid[i]) ** 2
                    for i in range(len(doc_record))
                )
            )
            print(key, document_number, mime_type, distance, filter_median_distance)


if __name__ == "__main__":
    # argparse with both commands
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("command", type=str, choices=["build", "apply"])
    arg_parser.add_argument("--key", type=str, default=None)
    args = arg_parser.parse_args()

    if args.command == "build":
        build_filter_model()
    elif args.command == "apply":
        apply_filter(args.key)
    else:
        print("Unknown command")
