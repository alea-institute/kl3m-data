"""
Base loader class
"""

# imports
import json
import random
import time
import traceback
import zlib
from abc import ABC, abstractmethod
from collections import deque
from logging import Logger
from typing import List, Optional

# packages
import valkey
import tqdm
from tokenizers import Tokenizer
from datasets import (
    Dataset,
    IterableDataset,
)

# project
from kl3m_data.api.loader.task.clm import CLMTask
from kl3m_data.logger import LOGGER
from kl3m_data.api.loader.task.base import TaskType
from kl3m_data.api.loader.task.lmlm import LMLMTask
from kl3m_data.api.loader.task.mlm import MLMTask
from kl3m_data.api.loader.task.pmlm import PMLMTask


DEFAULT_TASK_TYPES = [
    TaskType.MLM,
    TaskType.LMLM,
    TaskType.PMLM,
]

TASK_CLASS_MAP = {
    TaskType.MLM: MLMTask,
    TaskType.LMLM: LMLMTask,
    TaskType.PMLM: PMLMTask,
    TaskType.CLM: CLMTask,
}

DEFAULT_MIME_TYPES = [
    "text/plain",
    "text/markdown",
]

DEFAULT_MIN_QUEUE_SIZE = 10000
DEFAULT_SLEEP = 10


class BaseLoader(ABC):
    """Base class for data loaders."""

    def __init__(
        self,
        valkey_host: str = "localhost",
        valkey_port: int = 6379,
        valkey_db: int = 0,
        valkey_batch_size: int = 1000,
        sequence_length: int = 1024,
        min_queue_size: int | dict[str, int] = 100,
        enabled_tasks: Optional[List[str | TaskType]] = None,
        mime_types: Optional[List[str]] = None,
        input_tokenizer_name: str = "alea-institute/kl3m-004-128k-cased",
        output_tokenizer_name: str = "alea-institute/kl3m-004-128k-cased",
        logger: Optional[Logger] = LOGGER,
    ):
        """
        Initialize the KL3M data loader.

        Args:
            valkey_host (str): Redis host address
            valkey_port (int): Redis port number
            valkey_db (int): Redis database number
            valkey_batch_size (int): Number of samples to batch before writing to Redis
            sequence_length (int): Maximum sequence length for processing
            min_queue_size (int | dict[str, int]): Minimum queue size before pushing to Redis
            enabled_tasks (Optional[List[str | TaskType]]): List of enabled tasks
            mime_types (Optional[List[str]]): List of MIME types to process
            input_tokenizer_name (str): Name of the input tokenizer
            output_tokenizer_name (str): Name of the output tokenizer
            logger (Optional[Logger]): Logger instance
        """
        # set logger
        self.logger = logger or LOGGER

        # Redis configuration
        self.valkey_client = valkey.Valkey(
            host=valkey_host, port=valkey_port, db=valkey_db
        )
        self.valkey_batch_size = valkey_batch_size

        # Processing configuration
        self.sequence_length = sequence_length

        # Load tokenizers
        self.input_tokenizer = Tokenizer.from_pretrained(input_tokenizer_name)
        self.output_tokenizer = Tokenizer.from_pretrained(output_tokenizer_name)

        # Initialize task handlers
        self.task_handlers = {}
        if enabled_tasks is None:
            enabled_tasks = [task_type.value for task_type in DEFAULT_TASK_TYPES]

        if len(enabled_tasks) == 0:
            raise ValueError("No tasks enabled. Please enable at least one task.")

        for task_type in enabled_tasks:
            if isinstance(task_type, str):
                task_type = TaskType(task_type)
            if task_type not in TASK_CLASS_MAP:
                raise ValueError(f"Invalid task type: {task_type}")
            self.task_handlers[task_type] = TASK_CLASS_MAP[task_type](
                tokenizer=self.input_tokenizer,
                max_tokens=self.sequence_length,
            )

        # now make sure that min_queue_size is a dict[str, int], even if passed as int
        if isinstance(min_queue_size, int):
            self.min_queue_size = {
                task_type.value: min_queue_size
                for task_type in self.task_handlers.keys()
            }
        elif isinstance(min_queue_size, dict):
            self.min_queue_size = {
                task_type.value: min_queue_size.get(
                    task_type.value, DEFAULT_MIN_QUEUE_SIZE
                )
                for task_type in self.task_handlers.keys()
            }
        else:
            raise ValueError("min_queue_size must be an int or dict[str, int]")

        # set mime types
        if mime_types is None:
            mime_types = DEFAULT_MIME_TYPES

        self.mime_types = mime_types

        # set up queue and stats
        self.queue: dict[tuple[str, str], deque] = {}
        self.pushed = 0

        # log initialization
        self.logger.info(
            "Initialized KL3M data loader with %d tasks, batch size %d",
            len(self.task_handlers),
            self.valkey_batch_size,
        )

    def list_task_sources(self, task_type: str) -> List[str]:
        """
        List all task queues in Redis.

        Args:
            task_type (str): Type of masking task

        Returns:
            List[str]: List of task queue names
        """
        return self.valkey_client.keys(f"kl3m:samples:*:{task_type}:*")

    def get_task_queue_length(
        self, task_type: str, source_name: Optional[str] = None
    ) -> int:
        """
        Get the length of the task queue.

        Args:
            task_type (str): Type of masking task
            source_name (Optional[str]): Name of the queue

        Returns:
            int: Length of the task queue
        """
        if source_name is None:
            # list all keys
            queue_keys = self.valkey_client.keys(f"kl3m:samples:{task_type}:*")
            return sum(self.valkey_client.llen(key) for key in queue_keys)
        else:
            # get length of specific queue
            return self.valkey_client.llen(f"kl3m:samples:{task_type}:{source_name}")

    def push_samples(
        self,
    ) -> None:
        """
        Push samples to Redis.
        """
        # iterate over queues and push
        new_items = {task_type.value: 0 for task_type in self.task_handlers.keys()}

        # full state
        any_full = False

        # iterate over all queues
        for queue_key in self.queue.keys():
            # get task type and source
            task_type, source = queue_key

            # get redis queue size
            redis_queue_size = self.get_task_queue_length(task_type, source)
            if redis_queue_size >= self.min_queue_size[task_type]:
                any_full = True

            # randomly choose a side to pop from
            side_left = random.choice([True, False])

            while self.queue[queue_key]:
                # pop from queue
                if side_left:
                    sample = self.queue[queue_key].popleft()
                else:
                    sample = self.queue[queue_key].pop()

                # get source
                source = ",".join(sample.get("source", ["packed"]))

                # assert all lengths correct before pushing
                for key in [
                    "input_ids",
                    "labels",
                    "attention_mask",
                    "token_type_ids",
                ]:
                    # check lengths
                    if key in sample:
                        assert len(sample[key]) == self.sequence_length

                # total tokens and attention mask
                sum_attention_mask = sum(sample["attention_mask"])
                sum_labels = sum(1 for label in sample["labels"] if label >= 0)

                if (
                    sum_attention_mask < 0.01 * self.sequence_length
                    or sum_labels < 0.01 * self.sequence_length
                ):
                    # log it
                    self.logger.info(
                        "Skipping sample with too few tokens: %s < 0.05 * %d",
                        sample["identifier"],
                        self.sequence_length,
                    )
                    continue

                # push to redis
                if side_left:
                    self.valkey_client.lpush(
                        f"kl3m:samples:{task_type}:{source}",
                        zlib.compress(json.dumps(sample, default=str).encode("utf-8")),
                    )
                else:
                    self.valkey_client.rpush(
                        f"kl3m:samples:{task_type}:{source}",
                        zlib.compress(json.dumps(sample, default=str).encode("utf-8")),
                    )

                self.pushed += 1
                if queue_key not in new_items:
                    new_items[":".join(queue_key)] = 0
                new_items[":".join(queue_key)] += 1

        # log
        self.logger.info(
            "Pushed samples to Redis: %s", json.dumps(new_items, default=str)
        )
        if any_full:
            self.logger.info("Some queues full, sleeping 1 second...")
            time.sleep(DEFAULT_SLEEP // 2)

    def convert_tokenizer(self, input_tokens: list[int]) -> list[int]:
        """
        Convert input tokens to output tokens.

        Args:
            input_tokens (list[int]): List of input token IDs

        Returns:
            list[int]: List of token IDs in output vocabulary
        """
        if self.output_tokenizer != self.input_tokenizer:
            return self.output_tokenizer.encode(
                self.input_tokenizer.decode(input_tokens, skip_special_tokens=True),
                add_special_tokens=False,
            ).ids
        else:
            return input_tokens

    @abstractmethod
    def load_dataset(self, **kwargs) -> Dataset | IterableDataset:
        """
        Load the dataset.

        Args:
            **kwargs: Additional keyword arguments

        Returns:
            Dataset | IterableDataset: Loaded dataset
        """
        pass

    def generate_padded_samples(self, dataset: Dataset | IterableDataset) -> None:
        """Process samples from the dataset and pad (or truncate) each sample
        so that its length is exactly `self.sequence_length`.

        Instead of concatenating multiple processed samples into one long sequence,
        this version creates one sample per processed sample.
        """
        prog_bar = tqdm.tqdm(dataset)
        for row in prog_bar:
            # Skip samples with disallowed mime types.
            identifier = row.get("identifier", None)
            source = (
                row.get("dataset", None)
                or row.get("source", None)
                or row.get("dataset_id", None)
            )
            mime_type = row.get("mime_type", None)
            if mime_type and mime_type not in self.mime_types:
                self.logger.info("Skipping sample with mime type %s", mime_type)
                continue

            # Convert the tokens to the proper output tokenizer.
            document_tokens = self.convert_tokenizer(row["tokens"])
            self.logger.info("Processing %d tokens", len(document_tokens))

            # Process the sample for each task.
            for task_type, task_handler in self.task_handlers.items():
                queue_key = (task_type.value, source)
                self.logger.info("Processing task:source=%s", queue_key)
                try:
                    for i, processed_sample in enumerate(
                        task_handler.process_sample(document_tokens)
                    ):
                        self.logger.info(
                            "Processing sample %d from task %s", i, task_type
                        )
                        if processed_sample is None:
                            self.logger.info("Skipping sample with no tokens")
                            continue

                        # Create a new sample using the task handler's empty sample template.
                        sample = task_handler.create_empty_sample()
                        sample["identifier"].append(identifier)
                        sample["dataset_id"].append(source)
                        sample["source"].append(source)
                        sample["mime_type"].append(mime_type)

                        # stride chunks
                        for chunk_index in range(
                            0, len(processed_sample.input_ids), self.sequence_length - 2
                        ):
                            # create sample from each
                            chunk_input_ids = processed_sample.input_ids[
                                chunk_index : chunk_index + self.sequence_length
                            ]
                            chunk_labels = processed_sample.labels[
                                chunk_index : chunk_index + self.sequence_length
                            ]
                            chunk_length = len(chunk_input_ids)
                            pad_length = self.sequence_length - chunk_length - 2

                            # create the input_ids, labels, attention mask, and token type ids
                            # remember to add the end of sequence
                            sample["input_ids"] = (
                                [task_handler.start_sequence]
                                + chunk_input_ids
                                + [task_handler.end_sequence]
                                + [task_handler.pad_token_id] * pad_length
                            )
                            sample["labels"] = (
                                [task_handler.label_mask_id]
                                + chunk_labels
                                + [task_handler.label_mask_id]
                                + [task_handler.label_mask_id] * pad_length
                            )
                            sample["attention_mask"] = [1] * (chunk_length + 2) + [
                                0
                            ] * pad_length
                            sample["token_type_ids"] = [0] * (chunk_length + 2) + [
                                0
                            ] * pad_length

                            # add to queue
                            if queue_key not in self.queue:
                                self.queue[queue_key] = deque()
                            self.queue[queue_key].append(sample)

                    # push if long enough
                    if len(self.queue[queue_key]) >= self.valkey_batch_size:
                        self.push_samples()

                    # Update progress bar with current queue stats.
                    prog_bar.set_postfix(
                        {
                            "tasks": len(self.queue),
                            "queued": sum(len(queue) for queue in self.queue.values()),
                            "pushed": self.pushed,
                        }
                    )
                except Exception as e:
                    self.logger.error(
                        "Error for queue key %s: %s", queue_key, traceback.format_exc()
                    )
                    traceback.print_exc()
                    raise e

    async def process(self, **kwargs) -> None:
        """
        Process data and push to Redis.

        Args:
            **kwargs: Additional keyword arguments
        """
        # pass kwargs
        while True:
            dataset = self.load_dataset(**kwargs)
            self.generate_padded_samples(dataset)
            self.logger.info("Restarting data load and process...")
