# KL3M Pipeline Architecture and Operations

This document provides a detailed guide to the KL3M data processing pipeline, including its architecture, implementation, operations, and best practices.

## Pipeline Overview

The KL3M pipeline is a scalable, multi-stage system designed to process large volumes of legal and government documents through a series of transformations, from raw content to tokenized representations ready for language model training.

The pipeline consists of three main stages:

![KL3M Pipeline Flow](https://mermaid.ink/img/pako:eNp1kU9PwzAMxb_KyCdQ2UQ5cIPDJA47wtFqEstE9B_ZbmFVv3u9rhNlDHHxx_N78isa71ACBW5qX8WTrwOK5fLU-NoLezDNDSHslVNlJHHoEFGEiQ6qQ0RnrCMNjW9KGm4KpwbWZfv91z7_bDLLkJdJXHRSMV5H1tVhJ_D8s8tzHHBuRkwl-3Lpm8Qdd3iAHtqtTbaBlfPIe3I8IQ66LhTQ1xAYrQlkz9jlJRTdQwF6TEA-0CKvbduLsrbNUXcZz4e7gRvgDV-bJEzjbJ0rrXU0UTsJDaHJH7fwDl-SQ97XyZZIGrA2m-Ri10GzzQRMuXtb39bPsBL2u5CxnfA6sxZ7xxAo7FxYwzMIGxzaMUzEyL5EK7wWNgYSEsJptm3Fw2fNYxd-AXZwnQI?type=png)

1. **Documents Stage** - Collection and storage of raw document content
2. **Representations Stage** - Processing and tokenization of document content
3. **Parquet Stage** - Conversion to efficient binary format for ML consumption

The pipeline handles:
- Document retrieval from various sources
- Format-specific parsing
- Content extraction and cleaning
- Tokenization using language models
- Conversion to efficient storage formats
- Building indexes for fast access
- Deduplication and filtering
- Export to machine learning datasets

## Core Components

### DatasetPipeline

The `DatasetPipeline` class is the central component that orchestrates the entire pipeline process:

```python
class DatasetPipeline:
    def __init__(self, dataset_id, key_prefix=None, s3_client=None, bucket="data.kl3m.ai"):
        # Initialize pipeline settings and S3 connections
```

Key responsibilities:
- Tracking dataset status and progress
- Managing the transformation between pipeline stages
- Coordinating parallel processing
- Building dataset indexes

### DatasetDocument

Each document in the pipeline is represented by the `DatasetDocument` class:

```python
class DatasetDocument:
    def __init__(self, document_key, dataset_id, s3_client=None, bucket="data.kl3m.ai", key_prefix=None):
        # Initialize document representation with keys for each stage
```

Key responsibilities:
- Maintaining consistent document identity across stages
- Processing document content between stages
- Tracking document metadata and status

### S3 Storage Architecture

All data is stored in Amazon S3 using a structured organization:

#### Stage Prefixes

```
s3://data.kl3m.ai/
├── documents/                 # Raw document content
│   └── [dataset_id]/
│       └── [subfolder]/
│           └── [document_id].json
├── representations/           # Processed document representations
│   └── [dataset_id]/
│       └── [subfolder]/
│           └── [document_id].json
├── parquet/                   # Optimized binary format
│   └── [dataset_id]/
│       └── [subfolder]/
│           └── [document_id]
└── index/                     # Dataset indexes
    └── [dataset_id].json.gz
```

#### Key Naming Convention

Document keys maintain consistency across stages by:
- Preserving the same path structure in each stage
- Converting between formats as needed (e.g., removing .json extension for parquet)
- Supporting filtering by key prefix for targeted processing

## Pipeline Operations

### Processing Workflow

The pipeline follows a consistent workflow for processing documents:

1. **Collection Phase**:
   - Documents are collected from various sources
   - Raw content is stored with metadata in the Documents stage
   - Dublin Core standard is used for metadata

2. **Representation Phase**:
   - Documents are parsed based on format
   - Text is extracted and normalized
   - Content is tokenized using language models
   - Multiple representations are created (plain text, markdown)
   - Quality metrics are calculated

3. **Parquet Phase**:
   - JSON representations are converted to Parquet format
   - Tokens are stored in an efficient binary layout
   - Metadata is preserved

4. **Index Phase**:
   - Dataset indexes are built for efficient access
   - Metadata about dataset contents is compiled

### Parallel Processing

The pipeline leverages parallel processing for efficiency:

```python
def process_stage(self, source_stage, target_stage, max_workers=10, max_size=None, clobber=False):
    # Process all documents from source_stage to target_stage in parallel
```

Key features:
- Uses ThreadPoolExecutor for parallel document processing
- Configurable number of worker threads
- Automatic task distribution and result collection
- Progress tracking and reporting

### Incremental Processing

For large datasets, the pipeline supports incremental processing:

```python
def process_stage_missing_only(self, source_stage, target_stage, max_workers=10, max_size=None):
    # Process only documents that are missing from the target stage
```

Key features:
- Efficiently identifies missing documents
- Skips already processed documents
- Optimizes processing time for large datasets
- Supports resuming interrupted operations

### Clobbering and Reprocessing

The pipeline supports both incremental and complete reprocessing:

```python
def process_stage_with_clobber(self, source_stage, target_stage, max_workers=10, max_size=None):
    # Process all documents, overwriting existing files in the target stage
```

Key features:
- Option to overwrite existing processed files
- Complete reprocessing of entire datasets
- Useful for applying updated processing logic

## Command-Line Interface

The pipeline is controlled through a comprehensive CLI in `kl3m_data/cli/pipeline.py`:

### Key Commands

- **List Datasets**: Show all available datasets
  ```bash
  python -m kl3m_data.cli.pipeline list
  ```

- **Dataset Status**: Check the status of a dataset
  ```bash
  python -m kl3m_data.cli.pipeline status [dataset_id] [--key-prefix KEY_PREFIX]
  ```

- **Process Stages**: Process documents between stages
  ```bash
  python -m kl3m_data.cli.pipeline process-stage [dataset_id] documents representations [options]
  python -m kl3m_data.cli.pipeline process-stage [dataset_id] representations parquet [options]
  ```

- **Process All**: Process through all pipeline stages
  ```bash
  python -m kl3m_data.cli.pipeline process-all [dataset_id] [options]
  ```

- **Build Index**: Build dataset index
  ```bash
  python -m kl3m_data.cli.pipeline build-index [dataset_id] [options]
  ```

- **Export to JSONL**: Export dataset to JSONL format
  ```bash
  python -m kl3m_data.cli.pipeline export-jsonl [dataset_id] [output_path] [options]
  ```

- **Export to HuggingFace**: Push dataset to HuggingFace
  ```bash
  python -m kl3m_data.cli.pipeline push-to-hf [dataset_id] [output_name] [options]
  ```

### Command Options

Common options for processing commands:

- `--max-workers INT`: Maximum number of parallel workers (default: 10)
- `--max-size INT`: Maximum file size in bytes to process (default: None)
- `--clobber`: Force overwrite of existing files (default: False)
- `--key-prefix STR`: Process only files with this prefix
- `--batch-size INT`: Number of documents per batch (default: 1000)

## Data Formats

### Document Stage Format

Raw documents are stored as JSON objects with metadata:

```json
{
  "content": "base64_encoded_zlib_compressed_content",
  "source": "source_identifier",
  "format": "mime_type",
  "identifier": "unique_document_id",
  "title": "document_title",
  ...
}
```

### Representation Stage Format

Processed documents include tokenized content and metadata:

```json
{
  "documents": [
    {
      "source": "source_identifier",
      "identifier": "unique_document_id",
      "original_uri": "s3://data.kl3m.ai/documents/...",
      "representations": {
        "text/plain": {
          "content": "base64_encoded_zlib_compressed_content",
          "tokens": {
            "tokenizer_name": [token_ids]
          },
          "mime_type": "text/plain"
        },
        "text/markdown": {
          "content": "base64_encoded_zlib_compressed_content",
          "tokens": {
            "tokenizer_name": [token_ids]
          },
          "mime_type": "text/markdown"
        }
      },
      "metadata": { ... },
      "success": true
    }
  ]
}
```

### Parquet Stage Format

Binary format optimized for ML consumption:
- Efficient columnar storage
- Fast random access
- Reduced storage overhead
- Direct integration with ML frameworks

## Export Mechanisms

### JSONL Export

Export to JSONL for flexible downstream use:

```python
def export_to_jsonl(
    dataset_id, output_path, key_prefix=None, tokenizer=DEFAULT_TOKENIZER,
    representation="text/plain", batch_size=1000, min_tokens=0,
    max_tokens=None, min_percentile=None, max_percentile=None,
    deduplicate=False, sample_rate=1.0
):
    # Export dataset to JSONL format with filtering options
```

Key features:
- Configurable token filtering (min/max)
- Percentile-based filtering
- Deduplication support
- Random sampling
- Batch processing for memory efficiency

### HuggingFace Export

Push directly to HuggingFace Datasets:

```python
def push_to_huggingface(
    dataset_id, output_name, key_prefix=None, tokenizer=DEFAULT_TOKENIZER,
    representation="text/plain", batch_size=1000, min_tokens=0,
    max_tokens=None, decode=False, deduplicate=False
):
    # Push dataset to HuggingFace with options
```

Key features:
- Direct upload to HuggingFace
- Option to export tokenized IDs or decoded text
- Configurable filtering
- Deduplication support
- Batch processing for large datasets

## Advanced Topics

### Filtering Mechanisms

The pipeline supports various filtering mechanisms:

- **Size Filtering**: Skip documents larger than a threshold
  ```python
  if max_size is not None and len(object_data["content"]) > max_size:
      LOGGER.error("Content is larger than current max size setting")
      return []
  ```

- **Token Count Filtering**: Filter by token count during export
  ```python
  if min_tokens > 0 and len(tokens) < min_tokens:
      continue  # Skip documents with too few tokens
  if max_tokens is not None and len(tokens) > max_tokens:
      continue  # Skip documents with too many tokens
  ```

- **Percentile Filtering**: Filter based on distribution percentiles
  ```python
  if min_percentile is not None or max_percentile is not None:
      # Filter documents based on token count percentiles
  ```

### Deduplication

The pipeline supports content deduplication during export:

```python
def _deduplicate_jsonl(input_path, output_path):
    # Hash-based deduplication of documents
```

Key features:
- Uses document content hashing
- Keeps only one copy of duplicate content
- Preserves the first occurrence of each document
- Critical for training dataset quality

### Error Handling

The pipeline implements robust error handling:

- **Document-Level Recovery**: Errors in one document don't affect others
  ```python
  try:
      # Process document
  except Exception as e:
      LOGGER.error(f"Exception processing {doc_id}: {e}")
      # Continue with next document
  ```

- **Logging**: Comprehensive logging of errors and warnings
  ```python
  LOGGER.error(f"Error processing to representations: {e}")
  ```

- **Progress Reporting**: Regular progress updates
  ```python
  LOGGER.info(f"Progress: {completed_count}/{len(future_to_key)} documents ({progress_pct:.1f}%)")
  ```

## Best Practices

### Memory Management

- Use batch processing for large datasets
- Configure `max_workers` based on available memory
- Consider `max_size` limits for very large documents
- Process large datasets incrementally rather than all at once

### Performance Optimization

- Use parallel processing with appropriate worker count
- Process only missing documents when possible
- Use key prefixes to limit scope for targeted operations
- Consider batching documents for export operations

### Workflow Patterns

Recommended processing workflow:

1. Check dataset status
   ```bash
   python -m kl3m_data.cli.pipeline status [dataset_id]
   ```

2. Process through each stage
   ```bash
   python -m kl3m_data.cli.pipeline process-stage [dataset_id] documents representations
   python -m kl3m_data.cli.pipeline process-stage [dataset_id] representations parquet
   ```

3. Build the dataset index
   ```bash
   python -m kl3m_data.cli.pipeline build-index [dataset_id]
   ```

4. Export to desired format
   ```bash
   python -m kl3m_data.cli.pipeline push-to-hf [dataset_id] [output_name]
   ```

### Monitoring and Maintenance

- Regularly check pipeline status
- Monitor error logs
- Use clobber option judiciously
- Build indexes after major updates

## Further Reading

- [DATA.md](./DATA.md) - Overview of data storage architecture
- [SOURCES.md](./SOURCES.md) - Information about data sources
- [PARSERS.md](./PARSERS.md) - Details on document parsing