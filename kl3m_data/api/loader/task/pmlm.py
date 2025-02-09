"""
Traditional BERT-style MLM task

TODO: decide if we want to limit randomized tokens to stopwords/filler
TODO: decide if we want to exclude some special tokens from being randomized
"""

# imports
import random
from typing import Any, Dict, List, Generator, Optional
from logging import Logger

# packages
import numpy
from tokenizers import Tokenizer

# project
from kl3m_data.api.loader.task.base import BaseTask, TaskType, ProcessedSample


class PMLMTask(BaseTask):
    """Perturbed Masked Language Modeling task implementation."""

    def __init__(
        self,
        tokenizer: Optional[Tokenizer] = None,
        logger: Optional[Logger] = None,
        max_tokens: Optional[int] = None,
    ) -> None:
        """Initialize the task."""
        super().__init__(
            tokenizer=tokenizer,
            logger=logger,
            max_tokens=max_tokens,
        )

        # set tokenizer vocab size once since it can be expensive to lookup true value with added tokens
        self.vocab_size = len(self.tokenizer.get_vocab())

        # set start and end sequence
        self.start_sequence = self.cls_token_id
        self.end_sequence = self.sep_token_id

    @property
    def task_type(self) -> TaskType:
        return TaskType.PMLM

    def create_empty_sample(self) -> Dict[str, Any]:
        return {
            "identifier": [],
            "dataset_id": [],
            "source": [],
            "mime_type": [],
            "task": "pmlm",
            "input_ids": [self.cls_token_id],
            "labels": [self.label_mask_id],
            "attention_mask": [1],
            "token_type_ids": [0],
        }

    def process_sample(
        self, tokens: List[int]
    ) -> Generator[ProcessedSample, None, None]:
        """
        Process a sample for PMLM task.

        Args:
            tokens (List[int]): List of token IDs.

        Yields:
            ProcessedSample: Processed sample.
        """
        # get lengths
        sample_length = len(tokens)
        min_mask = int(max(0.1 * sample_length, 1.0))
        max_mask = int(max(0.2 * sample_length, 2.0))
        num_mask = numpy.random.randint(min_mask, max_mask)
        min_perturb = int(max(0, (num_mask - 1) // 2 - 1))
        max_perturb = int(max(0, num_mask // 2 - 1))

        # get random mask indices
        mask_indices = set(
            numpy.random.choice(
                a=sample_length,
                size=num_mask,
                replace=False,
            )
        )

        if max_perturb > min_perturb > 0:
            num_perturb = numpy.random.randint(min_perturb, max_perturb)
            perturb_indices = set(
                numpy.random.choice(
                    a=[i for i in range(sample_length) if i not in mask_indices],
                    size=num_perturb,
                    replace=False,
                )
            )
        else:
            perturb_indices = set()

        masked_ids = [
            random.randint(100, self.vocab_size - 1)
            if i in perturb_indices
            else self.mask_token_id
            if i in mask_indices
            else tokens[i]
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
