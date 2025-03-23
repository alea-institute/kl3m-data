# KL3M Data Storage and Pipeline Architecture

This document outlines how data is stored and processed in the KL3M data pipeline, with a focus on the S3 storage architecture.

## S3 Storage Architecture

KL3M data is organized in Amazon S3 using a structured three-stage pipeline:

1. **Documents Stage** (`documents/`): 
   - Contains the raw document files in JSON format
   - Organized by dataset ID and optional subfolder path
   - Example path: `documents/[dataset_id]/[optional_subfolder]/[document_id].json`

2. **Representations Stage** (`representations/`):
   - Contains processed document representations with extracted tokens
   - Maintains the same organizational structure as the documents stage
   - Example path: `representations/[dataset_id]/[optional_subfolder]/[document_id].json`

3. **Parquet Stage** (`parquet/`): 
   - Contains optimized binary Parquet format of document representations
   - The file extension is removed from the document path
   - Example path: `parquet/[dataset_id]/[optional_subfolder]/[document_id]`

4. **Index Stage** (`index/`):
   - Contains metadata and listings of dataset contents
   - Example path: `index/[dataset_id].json.gz`

All data is stored in the `data.kl3m.ai` S3 bucket.

## Processing Pipeline

The pipeline processes data through three main stages:

1. **Documents → Representations**:
   - Raw document content is parsed using appropriate parsers
   - Text is extracted and tokenized
   - Metadata is preserved and standardized
   - Quality metrics are calculated

2. **Representations → Parquet**:
   - JSON representations are converted to optimized Parquet format
   - Tokens are stored in a format ready for model training
   - Metadata is preserved

3. **Index Building**:
   - After processing, an index is created with metadata about the dataset
   - Lists all objects in the dataset for efficient querying

## Data Export and Distribution

The pipeline supports exporting data in multiple formats:

1. **JSONL Export**:
   - Exports tokens and metadata to compressed JSONL files
   - Supports filtering by quality score
   - Supports deduplication to avoid duplicate content

2. **HuggingFace Export**:
   - Pushes dataset directly to HuggingFace Datasets
   - Can export either token IDs or decoded text
   - Supports filtering and deduplication
   - Can process specific subfolders or key prefixes

## CLI Usage

The KL3M data pipeline can be controlled via a command-line interface in `kl3m_data/cli/pipeline.py`, which provides commands for:

- Listing datasets and their status
- Processing documents through the pipeline stages
- Building dataset indexes
- Exporting data to JSONL or HuggingFace

Examples:
```bash
# List all datasets
python kl3m_data/cli/pipeline.py list

# Process a specific dataset through all pipeline stages
python kl3m_data/cli/pipeline.py process-all [dataset_id]

# Export a dataset to HuggingFace
python kl3m_data/cli/pipeline.py push-to-hf [dataset_id] [output_name]
```

## Document Structure

Documents and representations are stored in a standardized format:

- **Document Stage**: Raw document data with source information
- **Representation Stage**: Includes tokenized content and metadata
- **Parquet Stage**: Binary format optimized for downstream tasks

Each document includes:
- Unique identifier
- Dataset identifier
- MIME type
- Tokenized content
- Quality metrics

## Parallel Processing

The pipeline leverages parallel processing for efficiency:
- Uses thread pools for parallel document processing
- Implements batched processing for large datasets
- Tracks missing documents to avoid redundant processing
- Uses deduplication to avoid duplicate content