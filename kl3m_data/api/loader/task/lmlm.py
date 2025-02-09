# imports
import random
from logging import Logger
from typing import Any, Dict, List, Optional, Generator

# packages
from tokenizers import Tokenizer


# project
from kl3m_data.api.loader.task.base import BaseTask, TaskType, ProcessedSample


class LMLMTask(BaseTask):
    """Newline-based Language Prediction task implementation."""

    def __init__(
        self,
        tokenizer: Optional[Tokenizer] = None,
        logger: Optional[Logger] = None,
        max_tokens: Optional[int] = None,
    ) -> None:
        """Initialize the task with newline token IDs available for modification.

        Args:
            tokenizer (Optional[Tokenizer]): Tokenizer to use.
            logger (Optional[Logger]): Logger to use.
            max_tokens (Optional[int]): Maximum number of tokens to process.
        """
        super().__init__(tokenizer=tokenizer, logger=logger, max_tokens=max_tokens)
        self.newline_token_ids = self._init_newline_tokens()
        self.start_sequence = self.cls_token_id
        self.end_sequence = self.sep_token_id

    def _init_newline_tokens(self) -> List[int]:
        """Initialize newline token IDs.

        Returns:
            List[int]: List of newline token IDs.
        """
        raw_tokens = ["\r", "\r\n"] + ["\n" * i for i in range(1, 10)]
        return [
            self.tokenizer.encode(token).ids[0]
            for token in raw_tokens
            if len(self.tokenizer.encode(token).ids) == 1
        ]

    @property
    def task_type(self) -> TaskType:
        return TaskType.LMLM

    def create_empty_sample(self) -> Dict[str, Any]:
        return {
            "identifier": [],
            "dataset_id": [],
            "source": [],
            "mime_type": [],
            "task": "lmlm",
            "input_ids": [self.cls_token_id],
            "labels": [self.label_mask_id],
            "attention_mask": [1],
            "token_type_ids": [0],
        }

    def process_sample(
        self, tokens: List[int]
    ) -> Generator[ProcessedSample, None, None]:
        in_masked_line = random.choice([True, False])
        num_masked = 0
        input_ids = []
        labels = []

        for token in tokens:
            if token in self.newline_token_ids:
                in_masked_line = not in_masked_line

            if in_masked_line:
                input_ids.append(self.mask_token_id)
                labels.append(token)
                num_masked += 1
            else:
                input_ids.append(token)
                labels.append(self.label_mask_id)

        if num_masked > 0:
            yield ProcessedSample(
                input_ids=input_ids, labels=labels, task=self.task_type.value
            )
