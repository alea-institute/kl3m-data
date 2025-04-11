"""
Module for collecting documents from the S3 pipeline and exporting them to
JSONL or Hugging Face datasets format.

This module uses DatasetPipeline to collect representations or parquet objects
and supports parallel processing for efficient data retrieval and transformation.
"""

# imports
import gzip
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Generator, List, Optional, Set, Union

# packages
import boto3
import datasets
from rich.console import Console
from rich.progress import (
    Progress,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
    TimeElapsedColumn,
)
from rich.table import Table

# project imports
from kl3m_data.logger import LOGGER
from kl3m_data.metrics.quality_metrics import get_metrics
from kl3m_data.pipeline.s3.dataset import DatasetPipeline
from kl3m_data.utils.s3_utils import (
    S3Stage,
    get_object_bytes,
    get_s3_client,
    list_common_prefixes,
    iter_prefix,
)
from kl3m_data.utils.parquet_utils import deserialize_document_bytes


def extract_tokens_and_metadata(
    doc_data: Dict,
    include_score: bool = True,
    include_metrics: bool = False,
    format_type: str = "tokens",
) -> Dict:
    """
    Extract tokens and metadata from a document.

    Args:
        doc_data: Document data (either from representation or parquet)
        include_score: Whether to include quality score
        include_metrics: Whether to include detailed metrics
        format_type: Output format type, either "tokens" for token IDs or "text" for decoded text

    Returns:
        Dict containing tokens/text and metadata
    """
    result = {
        "identifier": doc_data.get("identifier", "unknown"),
        "dataset": doc_data.get("dataset", "unknown"),
        "mime_type": "unknown",
    }

    # Extract tokens from appropriate format
    tokens = []

    # Handle parquet format (where representations is a dict mapping mime_type to tokens)
    if "representations" in doc_data and isinstance(doc_data["representations"], dict):
        # For parquet format, the first representation's tokens are used
        if doc_data["representations"]:
            # Get the first mime type
            first_mime_type = next(iter(doc_data["representations"]))
            result["mime_type"] = first_mime_type
            tokens = doc_data["representations"][first_mime_type]

    # Add tokens or text to result based on format_type
    if format_type == "text":
        # Decode tokens to text using the same tokenizer from parquet_utils
        from kl3m_data.utils.parquet_utils import DEFAULT_TOKENIZER

        try:
            # Convert token IDs to text using the same tokenizer that was used for encoding
            text = DEFAULT_TOKENIZER.decode(tokens) if tokens else ""
            result["text"] = text
        except Exception as e:
            LOGGER.error(f"Error decoding tokens to text: {e}")
            # Fallback to empty text
            result["text"] = ""
    else:
        # Default to tokens format - will be cast to int32 when creating the dataset
        result["tokens"] = tokens

    # Calculate scores if requested
    if include_score or include_metrics:
        metrics_result = get_metrics(result)

        if include_score:
            result["score"] = metrics_result.get("score", 0.0)

        if include_metrics:
            result["metrics"] = metrics_result.get("metrics", {})

    return result


def export_to_jsonl(
    dataset_id: str,
    output_path: str,
    source_stage: S3Stage = S3Stage.PARQUET,
    key_prefix: Optional[str] = None,
    max_workers: int = 10,
    max_documents: Optional[int] = None,
    score_threshold: Optional[float] = None,
    deduplicate: bool = True,
    include_metrics: bool = False,
    format_type: str = "tokens",
) -> int:
    """
    Export documents from S3 pipeline to a local JSONL file.

    Args:
        dataset_id: Dataset ID to export
        output_path: Path to output JSONL file
        source_stage: Which pipeline stage to export from (REPRESENTATIONS or PARQUET)
        key_prefix: Optional key prefix to filter objects
        max_workers: Maximum number of worker threads for parallel processing
        max_documents: Maximum number of documents to export
        score_threshold: Optional quality score threshold
        deduplicate: Whether to deduplicate documents based on first 1024 tokens
        include_metrics: Whether to include detailed metrics in the output
        format_type: Output format type, either "tokens" for token IDs or "text" for decoded text

    Returns:
        int: Number of exported documents
    """
    # Initialize pipeline
    pipeline = DatasetPipeline(dataset_id, key_prefix=key_prefix)
    console = Console()

    # Validate source stage
    if source_stage not in [S3Stage.REPRESENTATIONS, S3Stage.PARQUET]:
        raise ValueError(
            f"Invalid source stage: {source_stage}. Must be REPRESENTATIONS or PARQUET."
        )

    # Track processed documents and deduplication
    processed_count = 0
    skipped_count = 0
    exported_count = 0
    error_count = 0
    seen_hashes = set()

    # Thread safety
    stats_lock = threading.Lock()
    output_lock = threading.Lock()

    # Initialize S3 client for better connection pooling during parallel processing
    s3_client = get_s3_client()

    # Setup progress bar
    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TextColumn("{task.completed} of {task.total}"),
        TimeElapsedColumn(),
    ) as progress:
        # Process stage keys in parallel for better performance
        # First count the number of objects
        console.print(f"Counting objects in {source_stage.value}/{dataset_id}...")
        keys = list(pipeline.iter_stage_keys(source_stage))
        total_keys = len(keys)

        # Create progress tasks
        main_task = progress.add_task(
            f"Exporting {dataset_id} from {source_stage.value}", total=total_keys
        )

        # Define worker function to process each object
        def process_object(key: str) -> None:
            nonlocal processed_count, skipped_count, exported_count, error_count

            try:
                # Get object bytes
                object_bytes = get_object_bytes(s3_client, "data.kl3m.ai", key)
                if not object_bytes:
                    with stats_lock:
                        skipped_count += 1
                    return

                # Parse based on stage
                doc_data = None
                if source_stage == S3Stage.REPRESENTATIONS:
                    try:
                        # Load JSON representation
                        rep_data = json.loads(object_bytes)

                        # Extract document(s) - handle both single doc and list formats
                        documents = rep_data.get("documents", [])
                        if not documents and isinstance(rep_data, dict):
                            # Handle case where the representation is a single document
                            documents = [rep_data]

                        # Process only the first document if multiple exist
                        if documents:
                            doc_data = documents[0]

                            # Add dataset info from key if not in document
                            if "dataset" not in doc_data:
                                doc_data["dataset"] = dataset_id
                    except Exception as e:
                        LOGGER.error(
                            f"Error parsing representation data for {key}: {e}"
                        )
                        with stats_lock:
                            error_count += 1
                        return

                elif source_stage == S3Stage.PARQUET:
                    try:
                        # Deserialize parquet bytes
                        doc_data = deserialize_document_bytes(object_bytes)

                        # Add dataset info from key if not in document
                        if "dataset" not in doc_data:
                            doc_data["dataset"] = dataset_id
                    except Exception as e:
                        LOGGER.error(f"Error parsing parquet data for {key}: {e}")
                        with stats_lock:
                            error_count += 1
                        return

                # Skip if no valid document was found
                if not doc_data:
                    with stats_lock:
                        skipped_count += 1
                    return

                # Extract tokens and metadata
                result = extract_tokens_and_metadata(
                    doc_data,
                    include_score=True,
                    include_metrics=include_metrics,
                    format_type=format_type,
                )

                # Skip documents with no tokens or text
                if (
                    format_type == "tokens"
                    and not result.get("tokens")
                    or format_type == "text"
                    and not result.get("text")
                ):
                    with stats_lock:
                        skipped_count += 1
                    return

                # Apply score filter if specified
                if (
                    score_threshold is not None
                    and result.get("score", 0.0) > score_threshold
                ):
                    with stats_lock:
                        skipped_count += 1
                    return

                # Deduplicate if requested
                if deduplicate:
                    if format_type == "tokens" and result.get("tokens"):
                        # Use first 1024 tokens for deduplication
                        tokens_hash = hash(tuple(result["tokens"][:1024]))
                        with stats_lock:
                            if tokens_hash in seen_hashes:
                                skipped_count += 1
                                return
                            seen_hashes.add(tokens_hash)
                    elif format_type == "text" and result.get("text"):
                        # Use first 1000 characters for deduplication with text format
                        text_hash = hash(result["text"][:1000])
                        with stats_lock:
                            if text_hash in seen_hashes:
                                skipped_count += 1
                                return
                            seen_hashes.add(text_hash)

                # Write to output file
                with output_lock:
                    with gzip.open(output_path, "at", encoding="utf-8") as f:
                        f.write(json.dumps(result) + "\n")

                # Update stats
                with stats_lock:
                    processed_count += 1
                    exported_count += 1

                    # Check if we've reached the maximum number of documents
                    if max_documents and exported_count >= max_documents:
                        # Signal other threads to stop
                        return True

            except Exception as e:
                LOGGER.error(f"Error processing {key}: {e}")
                with stats_lock:
                    error_count += 1

            # Continue processing
            return False

        # Create an empty output file
        with gzip.open(output_path, "wt", encoding="utf-8") as f:
            pass

        # Process objects in parallel using a thread pool
        stop_processing = False
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_key = {executor.submit(process_object, key): key for key in keys}

            # Process results as they complete
            for future in future_to_key:
                if stop_processing:
                    # Cancel remaining futures if we've hit the limit
                    future.cancel()
                    continue

                key = future_to_key[future]
                try:
                    # Check if the future returns True, which means we've hit the limit
                    stop_processing = future.result() or False
                except Exception as e:
                    LOGGER.error(f"Error processing {key}: {e}")
                    with stats_lock:
                        error_count += 1

                # Update progress
                progress.update(main_task, advance=1)

        # Final status update
        console.print(f"Exported {exported_count} documents to {output_path}")
        console.print(
            f"Processed: {processed_count}, Skipped: {skipped_count}, Errors: {error_count}"
        )

        return exported_count


def list_dataset_subfolders(
    dataset_id: str,
    source_stage: Optional[S3Stage] = None,
) -> Dict[S3Stage, List[str]]:
    """
    List all top-level subfolders (prefixes) inside a dataset.
    If source_stage is None, lists subfolders for all stages.

    Args:
        dataset_id: Dataset ID to list subfolders for
        source_stage: Which pipeline stage to check (DOCUMENTS, REPRESENTATIONS, or PARQUET)
                     If None, lists all stages

    Returns:
        Dict[S3Stage, List[str]]: Dictionary mapping each stage to its list of subfolder names
    """
    # Initialize S3 client
    s3_client = get_s3_client()
    console = Console()
    bucket = "data.kl3m.ai"

    # Determine which stages to list
    stages_to_list = (
        [source_stage]
        if source_stage
        else [S3Stage.DOCUMENTS, S3Stage.REPRESENTATIONS, S3Stage.PARQUET]
    )

    # Dictionary to store results for all stages
    results = {}

    # Process each stage
    for stage in stages_to_list:
        # Define the bucket and prefix
        prefix = f"{stage.value}/{dataset_id}/"

        # Get all common prefixes (folders) within the dataset
        subfolders = list_common_prefixes(s3_client, bucket, prefix)

        # Extract just the folder names (without the full prefix path)
        stage_results = []
        for folder in subfolders:
            # Remove the stage/dataset_id/ part from the folder
            if folder.startswith(prefix):
                # Extract just the subfolder name (removing trailing slash)
                subfolder_name = folder[len(prefix) :].rstrip("/")
                if subfolder_name:  # Only add non-empty names
                    stage_results.append(subfolder_name)

        # Store results for this stage
        results[stage] = sorted(stage_results)

    # If only one stage was requested, display a simple table for that stage
    if source_stage:
        table = Table(title=f"Subfolders in {dataset_id} ({source_stage.value})")
        table.add_column("Subfolder", style="cyan")

        # Add rows
        for subfolder in results[source_stage]:
            table.add_row(subfolder)

        # Display the table
        console.print(table)
    else:
        # Get all unique subfolder names across all stages
        all_subfolders = set()
        for stage_results in results.values():
            all_subfolders.update(stage_results)

        # Create a table with all stages
        table = Table(title=f"Subfolders in {dataset_id} (All Stages)")
        table.add_column("Subfolder", style="cyan")
        table.add_column("Documents", style="green")
        table.add_column("Representations", style="green")
        table.add_column("Parquet", style="green")

        # Add rows
        for subfolder in sorted(all_subfolders):
            table.add_row(
                subfolder,
                "✓" if subfolder in results[S3Stage.DOCUMENTS] else "✗",
                "✓" if subfolder in results[S3Stage.REPRESENTATIONS] else "✗",
                "✓" if subfolder in results[S3Stage.PARQUET] else "✗",
            )

        # Display the table
        console.print(table)

    # Return results for programmatic use
    return results


def get_subfolder_status(
    dataset_id: str,
    subfolder: str,
    csv_path: Optional[str] = None,
) -> Dict[S3Stage, int]:
    """
    Get the document counts for a specific subfolder across all stages.

    Args:
        dataset_id: Dataset ID to check
        subfolder: Subfolder within the dataset to check
        csv_path: Optional path to save results as CSV

    Returns:
        Dict[S3Stage, int]: Dictionary mapping each stage to the number of documents
    """
    # Initialize S3 client
    s3_client = get_s3_client()
    console = Console()
    bucket = "data.kl3m.ai"

    # Get counts for each stage
    counts = {}
    missing_representations = 0
    missing_parquet = 0

    # Clean subfolder (remove trailing slash if present)
    clean_subfolder = subfolder.rstrip("/")

    # Track document IDs for calculating missing
    document_ids = set()
    representation_ids = set()
    parquet_ids = set()

    # Process each stage
    for stage in [S3Stage.DOCUMENTS, S3Stage.REPRESENTATIONS, S3Stage.PARQUET]:
        # Define the bucket and prefix
        prefix = f"{stage.value}/{dataset_id}/{clean_subfolder}/"

        # Count all objects with this prefix
        count = 0

        # Using set for tracking unique IDs
        doc_id_set = set()

        for key in iter_prefix(s3_client, bucket, prefix):
            count += 1

            # Extract document ID for missing calculation
            # The ID is the filename without extension
            filename = key.split("/")[-1]
            doc_id = filename.split(".")[0] if "." in filename else filename
            doc_id_set.add(doc_id)

        # Store the count and IDs
        counts[stage] = count

        # Store IDs for missing calculation
        if stage == S3Stage.DOCUMENTS:
            document_ids = doc_id_set
        elif stage == S3Stage.REPRESENTATIONS:
            representation_ids = doc_id_set
        elif stage == S3Stage.PARQUET:
            parquet_ids = doc_id_set

    # Calculate missing documents
    missing_representations = len(document_ids - representation_ids)
    missing_parquet = len(representation_ids - parquet_ids)

    # Create a table for display
    table = Table(title=f"Subfolder Status: {dataset_id}/{clean_subfolder}")

    # Add columns
    table.add_column("Stage", style="cyan")
    table.add_column("Count", style="green")
    table.add_column("Missing", style="red")

    # Add rows
    table.add_row(S3Stage.DOCUMENTS.value, str(counts.get(S3Stage.DOCUMENTS, 0)), "")
    table.add_row(
        S3Stage.REPRESENTATIONS.value,
        str(counts.get(S3Stage.REPRESENTATIONS, 0)),
        str(missing_representations),
    )
    table.add_row(
        S3Stage.PARQUET.value,
        str(counts.get(S3Stage.PARQUET, 0)),
        str(missing_parquet),
    )

    # Display the table
    console.print(table)

    # Write CSV output if requested
    if csv_path:
        try:
            import csv

            with open(csv_path, "w", newline="") as csvfile:
                csvwriter = csv.writer(csvfile)
                # Write header
                csvwriter.writerow(["Stage", "Count", "Missing"])
                # Write data rows
                csvwriter.writerow(
                    [S3Stage.DOCUMENTS.value, counts.get(S3Stage.DOCUMENTS, 0), ""]
                )
                csvwriter.writerow(
                    [
                        S3Stage.REPRESENTATIONS.value,
                        counts.get(S3Stage.REPRESENTATIONS, 0),
                        missing_representations,
                    ]
                )
                csvwriter.writerow(
                    [
                        S3Stage.PARQUET.value,
                        counts.get(S3Stage.PARQUET, 0),
                        missing_parquet,
                    ]
                )

            console.print(f"[green]CSV output written to {csv_path}[/green]")
        except Exception as e:
            console.print(f"[red]Error writing CSV: {e}[/red]")

    # Also return the stats for programmatic use
    stats = {
        "counts": counts,
        "missing": {
            S3Stage.REPRESENTATIONS: missing_representations,
            S3Stage.PARQUET: missing_parquet,
        },
    }

    return stats


def find_prefixes_containing(
    client: boto3.client,
    bucket: str,
    parent_prefix: str,
    substring: str,
) -> List[str]:
    """
    Find all prefixes under a parent prefix that contain a given substring.
    
    This is useful for matching variations of a prefix without knowing the exact form.
    
    Args:
        client (boto3.client): S3 client
        bucket (str): Bucket name
        parent_prefix (str): Parent prefix to search within
        substring (str): Substring to match in the prefixes
        
    Returns:
        List[str]: List of matching prefixes
    """
    # Make sure parent_prefix ends with a slash
    if not parent_prefix.endswith('/'):
        parent_prefix += '/'
        
    # List all common prefixes at the parent level
    all_prefixes = list_common_prefixes(client, bucket, parent_prefix)
    
    # Filter prefixes containing the substring
    matching_prefixes = []
    for prefix in all_prefixes:
        # Get the actual subfolder name by removing the parent prefix
        subfolder = prefix[len(parent_prefix):].rstrip('/')
        if substring.lower() in subfolder.lower():  # Case-insensitive match
            matching_prefixes.append(prefix)
            
    return matching_prefixes


def push_to_huggingface(
    dataset_id: str,
    output_name: str,
    source_stage: S3Stage = S3Stage.PARQUET,
    key_prefix: Optional[str] = None,
    key_prefix_contains: Optional[str] = None,
    max_workers: int = 10,
    max_documents: Optional[int] = None,
    score_threshold: Optional[float] = None,
    deduplicate: bool = True,
    include_metrics: bool = False,
    use_temp_file: bool = True,
    format_type: str = "tokens",
    temp_file_path: Optional[str] = None,
) -> None:
    """
    Export documents from S3 pipeline directly to Hugging Face.

    Args:
        dataset_id: Dataset ID to export
        output_name: Name of the Hugging Face dataset to create/update
        source_stage: Which pipeline stage to export from (REPRESENTATIONS or PARQUET)
        key_prefix: Optional key prefix to filter objects
        key_prefix_contains: Optional substring to match in prefixes 
                             (will search for all matching prefixes)
        max_workers: Maximum number of worker threads for parallel processing
        max_documents: Maximum number of documents to export
        score_threshold: Optional quality score threshold
        deduplicate: Whether to deduplicate documents based on first 1024 tokens
        include_metrics: Whether to include detailed metrics in the output
        use_temp_file: Whether to use a temporary file (True) or stream directly (False)
        format_type: Output format type, either "tokens" for token IDs or "text" for decoded text
        temp_file_path: Optional custom path for the temporary file. If None, uses system temp directory.
    """
    console = Console()
    bucket = "data.kl3m.ai"
    
    # Handle key_prefix_contains parameter
    if key_prefix_contains and not key_prefix:
        # Initialize S3 client
        s3_client = get_s3_client()
        
        # Construct the parent prefix for the dataset and stage
        parent_prefix = f"{source_stage.value}/{dataset_id}/"
        
        # Find all prefixes containing the substring
        matching_prefixes = find_prefixes_containing(
            s3_client, bucket, parent_prefix, key_prefix_contains
        )
        
        if matching_prefixes:
            # If we found matching prefixes, process each one and combine results
            console.print(f"Found {len(matching_prefixes)} prefixes containing '{key_prefix_contains}':")
            
            # Create a temporary dataset for each prefix and then combine
            all_datasets = []
            temp_path = None
            
            for prefix in matching_prefixes:
                subfolder = prefix[len(parent_prefix):].rstrip('/')
                console.print(f"  - {subfolder}")
                
                # Extract the key_prefix to use for this specific match
                current_key_prefix = subfolder
                
                # Process this prefix using a unique temporary file
                console.print(f"Processing prefix: {current_key_prefix}")
                
                if use_temp_file:
                    # Create a temporary file specific to this prefix
                    import tempfile
                    with tempfile.NamedTemporaryFile(suffix=".jsonl.gz", delete=False) as temp_file:
                        current_temp_path = temp_file.name
                        
                    # Export to the temporary file
                    doc_count = export_to_jsonl(
                        dataset_id=dataset_id,
                        output_path=current_temp_path,
                        source_stage=source_stage,
                        key_prefix=current_key_prefix,
                        max_workers=max_workers,
                        max_documents=None,  # We'll handle max documents later when combining
                        score_threshold=score_threshold,
                        deduplicate=False,  # We'll do deduplication at the end
                        include_metrics=include_metrics,
                        format_type=format_type,
                    )
                    
                    if doc_count > 0:
                        console.print(f"  Exported {doc_count} documents from {current_key_prefix}")
                        
                        # Load as dataset
                        try:
                            # Define features based on format_type
                            if format_type == "tokens":
                                from datasets import Features, Value, Sequence
                                
                                features = Features(
                                    {
                                        "identifier": Value("string"),
                                        "dataset": Value("string"),
                                        "mime_type": Value("string"),
                                        "tokens": Sequence(Value("int32")),
                                        "score": Value("float32"),
                                    }
                                )
                                if include_metrics:
                                    features["metrics"] = Value("string")  # JSON serialized
                                
                                # Load dataset with specified features
                                current_dataset = datasets.load_dataset(
                                    "json", data_files=current_temp_path, features=features
                                )
                            else:
                                # For text format, default features are fine
                                current_dataset = datasets.load_dataset("json", data_files=current_temp_path)
                                
                            # Add to our list of datasets to merge
                            all_datasets.append(current_dataset['train'])
                            
                        except Exception as e:
                            console.print(f"Error loading dataset: {e}")
                        
                        # Clean up temporary file
                        try:
                            os.unlink(current_temp_path)
                        except Exception as e:
                            console.print(f"Warning: Failed to remove temporary file {current_temp_path}: {e}")
                else:
                    # Direct streaming is not supported for the key_prefix_contains option
                    # as it requires combining multiple datasets which is difficult to do
                    # in a streaming fashion
                    console.print("Direct streaming is not supported with --key-prefix-contains.")
                    console.print("Using temporary files instead for reliable processing.")
                    
                    # Create a temporary file specific to this prefix
                    import tempfile
                    with tempfile.NamedTemporaryFile(suffix=".jsonl.gz", delete=False) as temp_file:
                        current_temp_path = temp_file.name
                        
                    # Export to the temporary file (same as above)
                    doc_count = export_to_jsonl(
                        dataset_id=dataset_id,
                        output_path=current_temp_path,
                        source_stage=source_stage,
                        key_prefix=current_key_prefix,
                        max_workers=max_workers,
                        max_documents=None,
                        score_threshold=score_threshold,
                        deduplicate=False,
                        include_metrics=include_metrics,
                        format_type=format_type,
                    )
                    
                    if doc_count > 0:
                        console.print(f"  Exported {doc_count} documents from {current_key_prefix}")
                        
                        # Load as dataset (same as above)
                        try:
                            # Define features based on format_type
                            if format_type == "tokens":
                                from datasets import Features, Value, Sequence
                                
                                features = Features(
                                    {
                                        "identifier": Value("string"),
                                        "dataset": Value("string"),
                                        "mime_type": Value("string"),
                                        "tokens": Sequence(Value("int32")),
                                        "score": Value("float32"),
                                    }
                                )
                                if include_metrics:
                                    features["metrics"] = Value("string")  # JSON serialized
                                
                                # Load dataset with specified features
                                current_dataset = datasets.load_dataset(
                                    "json", data_files=current_temp_path, features=features
                                )
                            else:
                                # For text format, default features are fine
                                current_dataset = datasets.load_dataset("json", data_files=current_temp_path)
                                
                            # Add to our list of datasets to merge
                            all_datasets.append(current_dataset['train'])
                            
                        except Exception as e:
                            console.print(f"Error loading dataset: {e}")
                        
                        # Clean up temporary file
                        try:
                            os.unlink(current_temp_path)
                        except Exception as e:
                            console.print(f"Warning: Failed to remove temporary file {current_temp_path}: {e}")
            
            # If we have datasets to merge
            if all_datasets:
                # Merge all datasets
                console.print(f"Merging {len(all_datasets)} datasets...")
                merged_dataset = datasets.concatenate_datasets(all_datasets)
                
                # Apply additional filters that couldn't be applied per-dataset
                if max_documents and len(merged_dataset) > max_documents:
                    console.print(f"Trimming to {max_documents} documents")
                    merged_dataset = merged_dataset.select(range(max_documents))
                
                if deduplicate:
                    console.print("Removing duplicates...")
                    # Create a deduplication function based on format_type
                    if format_type == "tokens":
                        def get_hash(example):
                            # Use first 1024 tokens for deduplication
                            tokens = example.get("tokens", [])[:1024]
                            return hash(tuple(tokens))
                    else:
                        def get_hash(example):
                            # Use first 1000 characters for deduplication
                            text = example.get("text", "")[:1000]
                            return hash(text)
                    
                    # Track hashes we've seen
                    seen_hashes = set()
                    keep_indices = []
                    
                    # Identify indices to keep (first occurrence of each hash)
                    for i, example in enumerate(merged_dataset):
                        h = get_hash(example)
                        if h not in seen_hashes:
                            seen_hashes.add(h)
                            keep_indices.append(i)
                    
                    # Select just the unique items
                    merged_dataset = merged_dataset.select(keep_indices)
                    console.print(f"After deduplication: {len(merged_dataset)} documents")
                
                # Push to Hugging Face
                console.print(f"Pushing {len(merged_dataset)} documents to Hugging Face dataset '{output_name}'...")
                merged_dataset.push_to_hub(output_name)
                console.print(f"Successfully pushed to Hugging Face as {output_name}")
                return
            else:
                console.print("No documents were found in any of the matching prefixes")
                return
                
        else:
            console.print(f"No prefixes found containing '{key_prefix_contains}'")
            return

    if use_temp_file:
        # Use a temporary file approach for better reliability
        import tempfile

        if temp_file_path:
            # Use the provided custom temp file path
            temp_path = temp_file_path
            console.print(
                f"Exporting to custom temporary file {temp_path} before uploading to Hugging Face..."
            )
        else:
            # Use system temp directory
            with tempfile.NamedTemporaryFile(
                suffix=".jsonl.gz", delete=False
            ) as temp_file:
                temp_path = temp_file.name
                console.print(
                    f"Exporting to temporary file {temp_path} before uploading to Hugging Face..."
                )

        try:
            # First export to the temporary file
            doc_count = export_to_jsonl(
                dataset_id=dataset_id,
                output_path=temp_path,
                source_stage=source_stage,
                key_prefix=key_prefix,
                max_workers=max_workers,
                max_documents=max_documents,
                score_threshold=score_threshold,
                deduplicate=deduplicate,
                include_metrics=include_metrics,
                format_type=format_type,
            )

            if doc_count == 0:
                console.print(
                    f"No documents were exported. Skipping upload to Hugging Face."
                )
                return

            # Then load and push to Hugging Face
            console.print(f"Loading dataset from {temp_path}...")

            # Define features based on format_type
            if format_type == "tokens":
                from datasets import Features, Value, Sequence

                features = Features(
                    {
                        "identifier": Value("string"),
                        "dataset": Value("string"),
                        "mime_type": Value("string"),
                        "tokens": Sequence(Value("int32")),
                        "score": Value("float32"),
                    }
                )
                if include_metrics:
                    # Add metrics feature if included
                    features["metrics"] = Value("string")  # JSON serialized

                # Load dataset with specified features to ensure int32 tokens
                dataset = datasets.load_dataset(
                    "json", data_files=temp_path, features=features
                )
            else:
                # For text format, default features are fine
                dataset = datasets.load_dataset("json", data_files=temp_path)

            # Push to Hugging Face
            console.print(
                f"Pushing {doc_count} documents to Hugging Face dataset '{output_name}'..."
            )
            dataset.push_to_hub(output_name)

            console.print(f"Successfully pushed to Hugging Face as {output_name}")

        finally:
            # Clean up temporary file
            try:
                os.unlink(temp_path)
            except Exception as e:
                LOGGER.warning(f"Failed to remove temporary file {temp_path}: {e}")
    else:
        # Define a generator function for streaming to Hugging Face
        def generate_documents() -> Generator[Dict, None, None]:
            # Initialize pipeline
            pipeline = DatasetPipeline(dataset_id, key_prefix=key_prefix)

            # Track stats
            nonlocal doc_count
            doc_count = 0
            seen_hashes = set()
            s3_client = get_s3_client(pool_size=max(25, max_workers * 2))

            # Process documents in batches for better performance
            keys = list(pipeline.iter_stage_keys(source_stage))

            # Process in smaller batches
            batch_size = 100
            for i in range(0, len(keys), batch_size):
                batch_keys = keys[i : i + batch_size]

                # Process batch in parallel
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # Submit all tasks in this batch
                    futures = []
                    for key in batch_keys:
                        futures.append(
                            executor.submit(
                                process_single_document,
                                key,
                                s3_client,
                                source_stage,
                                seen_hashes if deduplicate else None,
                                score_threshold,
                                include_metrics,
                                format_type,
                            )
                        )

                    # Process results as they complete
                    for future in futures:
                        try:
                            result = future.result()
                            if result:
                                doc_count += 1
                                yield result

                                # Check if we've reached the limit
                                if max_documents and doc_count >= max_documents:
                                    return
                        except Exception as e:
                            LOGGER.error(f"Error processing document: {e}")

        # Function to process a single document
        def process_single_document(
            key: str,
            s3_client,
            source_stage: S3Stage,
            seen_hashes: Optional[Set[int]] = None,
            score_threshold: Optional[float] = None,
            include_metrics: bool = False,
            format_type: str = "tokens",
        ) -> Optional[Dict]:
            try:
                # Get object bytes
                object_bytes = get_object_bytes(s3_client, "data.kl3m.ai", key)
                if not object_bytes:
                    return None

                # Parse based on stage
                doc_data = None
                if source_stage == S3Stage.REPRESENTATIONS:
                    try:
                        # Load JSON representation
                        rep_data = json.loads(object_bytes)

                        # Extract document(s) - handle both single doc and list formats
                        documents = rep_data.get("documents", [])
                        if not documents and isinstance(rep_data, dict):
                            # Handle case where the representation is a single document
                            documents = [rep_data]

                        # Process only the first document if multiple exist
                        if documents:
                            doc_data = documents[0]

                            # Add dataset info from key if not in document
                            if "dataset" not in doc_data:
                                doc_data["dataset"] = dataset_id
                    except Exception as e:
                        LOGGER.error(
                            f"Error parsing representation data for {key}: {e}"
                        )
                        return None

                elif source_stage == S3Stage.PARQUET:
                    try:
                        # Deserialize parquet bytes
                        doc_data = deserialize_document_bytes(object_bytes)

                        # Add dataset info from key if not in document
                        if "dataset" not in doc_data:
                            doc_data["dataset"] = dataset_id
                    except Exception as e:
                        LOGGER.error(f"Error parsing parquet data for {key}: {e}")
                        return None

                # Skip if no valid document was found
                if not doc_data:
                    return None

                # Extract tokens and metadata
                result = extract_tokens_and_metadata(
                    doc_data,
                    include_score=True,
                    include_metrics=include_metrics,
                    format_type=format_type,
                )

                # Skip documents with no tokens or text
                if (
                    format_type == "tokens"
                    and not result.get("tokens")
                    or format_type == "text"
                    and not result.get("text")
                ):
                    return None

                # Apply score filter if specified
                if (
                    score_threshold is not None
                    and result.get("score", 0.0) > score_threshold
                ):
                    return None

                # Deduplicate if requested
                if seen_hashes is not None:
                    if format_type == "tokens" and result.get("tokens"):
                        # Use first 1024 tokens for deduplication
                        tokens_hash = hash(tuple(result["tokens"][:1024]))
                        if tokens_hash in seen_hashes:
                            return None
                        seen_hashes.add(tokens_hash)
                    elif format_type == "text" and result.get("text"):
                        # Use first 1000 characters for deduplication with text format
                        text_hash = hash(result["text"][:1000])
                        if text_hash in seen_hashes:
                            return None
                        seen_hashes.add(text_hash)

                return result

            except Exception as e:
                LOGGER.error(f"Error processing {key}: {e}")
                return None

        # Stream directly to Hugging Face
        console.print(
            f"Streaming documents directly to Hugging Face dataset '{output_name}'..."
        )

        # Track document count for reporting
        doc_count = 0

        # Create and push dataset
        if format_type == "tokens":
            # Define features to ensure int32 tokens
            from datasets import Features, Value, Sequence

            features = Features(
                {
                    "identifier": Value("string"),
                    "dataset": Value("string"),
                    "mime_type": Value("string"),
                    "tokens": Sequence(Value("int32")),
                    "score": Value("float32"),
                }
            )
            if include_metrics:
                # Add metrics feature if included
                features["metrics"] = Value("string")  # JSON serialized

            # Create dataset with specified features
            dataset = datasets.Dataset.from_generator(
                generate_documents, features=features
            )
        else:
            # For text format, use default features
            dataset = datasets.Dataset.from_generator(generate_documents)

        dataset.push_to_hub(output_name)

        console.print(
            f"Successfully pushed {doc_count} documents to Hugging Face as {output_name}"
        )
