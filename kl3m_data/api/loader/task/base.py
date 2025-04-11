# imports
from abc import ABC, abstractmethod
from enum import Enum
from dataclasses import dataclass
from logging import Logger
from typing import Optional, List, Dict, Generator

# packages
from tokenizers import Tokenizer

# project
from kl3m_data.logger import LOGGER

DEFAULT_TOKENIZER_NAME = "alea-institute/kl3m-004-128k-cased"
DEFAULT_TOKENIZER = Tokenizer.from_pretrained(DEFAULT_TOKENIZER_NAME)


class TaskType(Enum):
    """Enum for different masking task types."""

    # masked tasks
    MLM = "mlm"  # Masked Language Modeling (masking without random token replacement)
    PMLM = "pmlm"  # Perturbed Masked Language Modeling (trad mlm task with random token replacement)
    LMLM = "lmlm"  # Line-Masked Language Modeling (alternate line masking)
    # OMLM = "omlm"  # Order-Masked Language Modeling  (permute tokens)
    # clm tasks
    CLM = "clm"  # Causal Language Modeling (trad next token)
    # PCLM = "pclm"  # Perturbed Causal Language Modeling (synthetic errors)


@dataclass
class ProcessedSample:
    """Container for processed sample data."""

    input_ids: List[int]
    labels: List[int]
    task: str
    attention_mask: Optional[List[int]] = None
    token_type_ids: Optional[List[int]] = None


class BaseTask(ABC):
    """Abstract base class for KL3M masking tasks."""

    def __init__(
        self,
        tokenizer: Optional[Tokenizer] = None,
        logger: Optional[Logger] = None,
        max_tokens: Optional[int] = None,
    ) -> None:
        """
        Initialize the task.

        Args:
            tokenizer (Optional[Tokenizer]): Tokenizer to use.
            logger (Optional[Logger]): Logger to use.
            max_tokens (Optional[int]): Maximum number of tokens to process for a single sample
                (used for OMLM or other chunked tasks).
        """
        # set vars
        self.tokenizer: Tokenizer = tokenizer or DEFAULT_TOKENIZER
        self.label_mask_id: int = -100
        self.pad_side: str = "right"
        self.logger: Logger = logger or LOGGER

        # optional max token length for tasks where we have to be careful to keep them together
        self.max_tokens: Optional[int] = max_tokens

        # get special token + id
        self.start_token = "<|start|>"
        self.end_token = "<|end|>"
        self.mask_token = "<|mask|>"
        self.unk_token = "<|unk|>"
        self.cls_token = "<|cls|>"
        self.sep_token = "<|sep|>"
        self.pad_token = "<|pad|>"
        self.start_token_id: int = self.tokenizer.token_to_id(self.start_token)
        self.end_token_id: int = self.tokenizer.token_to_id(self.end_token)
        self.mask_token_id: int = self.tokenizer.token_to_id(self.mask_token)
        self.unk_token_id: int = self.tokenizer.token_to_id(self.unk_token)
        self.cls_token_id: int = self.tokenizer.token_to_id(self.cls_token)
        self.sep_token_id: int = self.tokenizer.token_to_id(self.sep_token)
        self.pad_token_id: int = self.tokenizer.token_to_id(self.pad_token)

        # start and end sequence tokens
        self.start_sequence: Optional[int] = None
        self.end_sequence: Optional[int] = None

    @property
    @abstractmethod
    def task_type(self) -> TaskType:
        """Return the task type."""
        pass

    @abstractmethod
    def create_empty_sample(self) -> Dict[str, str | int | float | list]:
        """Create an empty sample."""
        pass

    @abstractmethod
    def process_sample(
        self, tokens: List[int]
    ) -> Generator[ProcessedSample, None, None]:
        """
        Process a sample for the task.

        Args:
            tokens (List[int]): Input tokens to process

        Returns:
            Optional[ProcessedSample]: Processed sample or None if sample should be skipped
        """
        pass
