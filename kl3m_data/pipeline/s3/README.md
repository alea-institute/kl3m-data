# Overview

S3 Pipelines involve three stages.  All objects are stored in S3 under the `data.kl3m.ai` bucket.  

The three stages are:

## Stage 1 - Source Retrieval

**S3 Location**: s3://data.kl3m.ai/documents/
**Example**: s3://data.kl3m.ai/documents/{dataset-id}/{document_id}.json
**Script**: kl3m_data/cli/sources.py

In this stage, we retrieve source documents from their original location.


## Stage 2 - Extracting Representations

**S3 Location**: s3://data.kl3m.ai/representations/
**Example**: s3://data.kl3m.ai/representations/{dataset-id}/{document_id}.json
**Script**: kl3m_data/cli/parsers.py

In this stage, we extract text, Markdown, or other representations from the source documents.

## Stage 3 - Converting to Parquet

**S3 Location**: s3://data.kl3m.ai/parquet/
**Example**: s3://data.kl3m.ai/parquet/{dataset-id}/{document_id}
**Script**: kl3m_data/cli/parquet.py

In this stage, we convert the extracted representations into Parquet format for efficient storage and retrieval.


# Package

## Design Notes

* Class-based for convenience
* Support sharing an S3 client across multiple instances or creating a new one for each instance
* Does NOT need to integrate into kl3m_data.cli.sources or kl3m_data.sources
* DOES need to integrate into kl3m_data.cli.parsers and kl3m_data.parsers
* DOES need to integrate into kl3m_data.cli.parquet and kl3m_data.utils.parquet_utils functionality


## Classes

### DatasetPipeline

* Handles iterating over all documents in a dataset
* Supports collecting index of all documents in all stages (e.g., identify which documents are missing from Stage 2 and 3)
* Supports processing documents in parallel with producer-consumer pattern
* Supports processing documents in parallel with a pool of workers
* Supports collecting all final Stage 3 parquet objects
* Supports uploading the final Stage 3 parquet file to Hugging Face

### DatasetDocument

* Represents a single document in a dataset
* Supports checking for and retrieving all three stages of the pipeline
* Supports converting from a Stage 1 to Stage 2 (kl3m_data.parsers)
* Supports converting from a Stage 2 to Stage 3 (kl3m_data.utils.parquet_utils)
