"""
S3 Pipeline module for processing documents through the three-stage pipeline:
1. Documents (Stage 1)
2. Representations (Stage 2)
3. Parquet (Stage 3)
"""

from kl3m_data.pipeline.s3.dataset import DatasetDocument, DatasetPipeline
from kl3m_data.utils.s3_utils import S3Stage

__all__ = ["DatasetDocument", "DatasetPipeline", "S3Stage"]