"""
Hugging Face hub loader
"""

# improts
import argparse
import asyncio
import json
from pathlib import Path

# packages
from datasets import (
    load_dataset,
    Dataset,
    interleave_datasets,
    concatenate_datasets,
    Sequence,
    Value
)
from huggingface_hub import hf_api

# projects
from kl3m_data.api.loader.base import BaseLoader


class HFLoader(BaseLoader):
    """Hugging Face hub loader."""

    @staticmethod
    def get_datasets_by_prefix(prefix: str) -> list[str]:
        """Get datasets by prefix.

        Returns:
            list[str]: List of dataset names
        """
        # check if prefix is valid
        if not prefix.startswith("alea-institute/"):
            prefix = "alea-institute/" + prefix

        datasets = []
        for dataset in hf_api.list_datasets(author="alea-institute"):
            if dataset.id.startswith(prefix):
                datasets.append(dataset.id)

        return datasets

    def load_dataset(self, **kwargs) -> Dataset:
        """Load datasets.

        Kwargs:
            datasets (list[str]): List of dataset names
            stream (bool): Whether to stream datasets
            interleave (bool): Whether to interleave datasets
            shuffle (bool): Whether to shuffle datasets
        """
        # handle dataset var
        datasets = kwargs.get("datasets", [])
        if isinstance(datasets, str):
            datasets = [datasets]
        elif not isinstance(datasets, list):
            raise TypeError("datasets must be a list or a string")

        # get other kwargs
        stream = kwargs.get("stream", False)
        interleave = kwargs.get("interleave", False)
        shuffle = kwargs.get("shuffle", True)

        # handle any prefix via terminal wildcard
        dataset_id_list: list[str] = []
        for dataset in datasets:
            if dataset.endswith("*"):
                dataset_id_list.extend(self.get_datasets_by_prefix(dataset.rstrip("*")))
            else:
                dataset_id_list.append(dataset)

        # load from hf
        partial_datasets: list[Dataset] = []
        for dataset_id in dataset_id_list:
            try:
                partial_datasets.append(
                    load_dataset(dataset_id, split="train", streaming=stream)\
                        .cast_column("tokens", Sequence(Value("int64")))
                )
            except Exception as e:
                self.logger.error(f"Error loading dataset {dataset_id}: {e}")
                continue

        # deal with args around combine/interleave and shuffle
        if interleave:
            combined_dataset = interleave_datasets(
                partial_datasets, stopping_strategy="all_exhausted"
            )
        else:
            combined_dataset = concatenate_datasets(partial_datasets)

        if shuffle:
            combined_dataset = combined_dataset.shuffle()

        return combined_dataset


if __name__ == "__main__":
    # argparser
    parser = argparse.ArgumentParser()
    parser.add_argument("config_file", type=str, help="The config file to load")
    args = parser.parse_args()

    config_file_path = Path(args.config_file)
    if not config_file_path.exists():
        raise FileNotFoundError(f"Config file {config_file_path} not found")

    with open(config_file_path, "rt", encoding="utf-8") as input_file:
        config = json.load(input_file)

    # init loader and process args
    loader_args = {}
    process_args = {}

    # get the rest of the config
    if "valkey_host" in config:
        loader_args["valkey_host"] = config.pop("valkey_host")
    if "valkey_port" in config:
        loader_args["valkey_port"] = config.pop("valkey_port")
    if "valkey_db" in config:
        loader_args["valkey_db"] = config.pop("valkey_db")
    if "valkey_batch_size" in config:
        loader_args["valkey_batch_size"] = config.pop("valkey_batch_size")
    if "sequence_length" in config:
        loader_args["sequence_length"] = config.pop("sequence_length")
    if "min_queue_size" in config:
        loader_args["min_queue_size"] = config.pop("min_queue_size")
    if "input_tokenizer_name" in config:
        loader_args["input_tokenizer_name"] = config.pop("input_tokenizer_name")
    if "output_tokenizer_name" in config:
        loader_args["output_tokenizer_name"] = config.pop("output_tokenizer_name")

    # check for mime types
    mime_types = config.get("mime_types", None)
    if mime_types is not None:
        loader_args["mime_types"] = mime_types

    # handle task types
    # tasks
    tasks = config.get("tasks", None)
    if tasks is not None:
        loader_args["enabled_tasks"] = tasks

    # get the dataset config
    datasets = config.pop("datasets", [])
    if len(datasets) == 0:
        raise ValueError("No datasets provided")
    if "shuffle" in config:
        process_args["shuffle"] = config.pop("shuffle")
    if "interleave" in config:
        process_args["interleave"] = config.pop("interleave")
    if "stream" in config:
        process_args["stream"] = config.pop("stream")

    async def amain():
        hf_loader = HFLoader(**loader_args)
        await hf_loader.process(datasets=datasets, **process_args)

    asyncio.run(amain())
