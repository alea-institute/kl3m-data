# imports
from logging import Logger
from typing import Any, Dict, List, Generator, Optional

# packages
import numpy.random
from tokenizers import Tokenizer

# project
from kl3m_data.api.loader.task.base import BaseTask, TaskType, ProcessedSample


class MLMTask(BaseTask):
    """Masked Language Modeling task implementation."""

    def __init__(
        self,
        tokenizer: Optional[Tokenizer] = None,
        logger: Optional[Logger] = None,
        max_tokens: Optional[int] = None,
    ) -> None:
        """
        Initialize the MLM task.

        Args:
            tokenizer (Optional[Tokenizer]): Tokenizer to use.
            logger (Optional[Logger]): Logger to use.
            max_tokens (Optional[int]): Maximum number of tokens to process for a single sample
                (used for OMLM or other chunked tasks).
        """
        super().__init__(tokenizer, logger, max_tokens)
        self.start_sequence = self.cls_token_id
        self.end_sequence = self.sep_token_id

    @property
    def task_type(self) -> TaskType:
        return TaskType.MLM

    def create_empty_sample(self) -> Dict[str, Any]:
        return {
            "identifier": [],
            "dataset_id": [],
            "source": [],
            "mime_type": [],
            "task": "mlm",
            "input_ids": [self.cls_token_id],
            "labels": [self.label_mask_id],
            "attention_mask": [1],
            "token_type_ids": [0],
        }

    def process_sample(
        self, tokens: List[int]
    ) -> Generator[ProcessedSample, None, None]:
        """
        Process a sample for MLM task.

        Args:
            tokens (List[int]): List of token IDs.

        Yields:
            ProcessedSample: Processed sample.
        """
        #  get constants
        sample_length = len(tokens)
        min_mask = int(max(0.1 * sample_length, 1.0))
        max_mask = int(max(0.2 * sample_length, 2.0))

        mask_indices = set(
            numpy.random.choice(
                a=sample_length,
                size=numpy.random.randint(min_mask, max_mask),
                replace=False,
            )
        )

        masked_ids = [
            tokens[i] if i not in mask_indices else self.mask_token_id
            for i in range(sample_length)
        ]
        labels = [
            tokens[i] if i in mask_indices else self.label_mask_id
            for i in range(sample_length)
        ]

        yield ProcessedSample(
            input_ids=masked_ids,
            labels=labels,
            task=self.task_type.value,
        )
