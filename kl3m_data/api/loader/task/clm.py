# imports
from logging import Logger
from typing import Any, Dict, List, Generator, Optional

# packages
from tokenizers import Tokenizer

# project
from kl3m_data.api.loader.task.base import BaseTask, TaskType, ProcessedSample


class CLMTask(BaseTask):
    """Causal Language Modeling (next token prediction) task implementation."""

    def __init__(
        self,
        tokenizer: Optional[Tokenizer] = None,
        logger: Optional[Logger] = None,
        max_tokens: Optional[int] = None,
    ) -> None:
        """
        Initialize the CLM task.

        Args:
            tokenizer (Optional[Tokenizer]): Tokenizer to use.
            logger (Optional[Logger]): Logger to use.
            max_tokens (Optional[int]): Maximum number of tokens to process for a single sample
                (used for OMLM or other chunked tasks).
        """
        super().__init__(tokenizer, logger, max_tokens)
        self.start_sequence = self.start_token_id
        self.end_sequence = self.end_token_id

    @property
    def task_type(self) -> TaskType:
        return TaskType.CLM

    def create_empty_sample(self) -> Dict[str, Any]:
        return {
            "identifier": [],
            "dataset_id": [],
            "source": [],
            "mime_type": [],
            "task": "clm",
            "input_ids": [self.start_token_id],
            "labels": [self.label_mask_id],
            "attention_mask": [1],
            "token_type_ids": [0],
        }

    def process_sample(
        self, tokens: List[int]
    ) -> Generator[ProcessedSample, None, None]:
        """
        Process a sample for CLM task.

        Args:
            tokens (List[int]): List of token IDs.

        Yields:
            ProcessedSample: Processed sample for CLM task.
        """
        yield ProcessedSample(
            input_ids=tokens,
            labels=tokens,
            task=self.task_type.value,
        )
