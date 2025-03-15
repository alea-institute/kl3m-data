"""
Module for collecting documents from the S3 pipeline and exporting them to
JSONL or Hugging Face datasets format.

This module uses DatasetPipeline to collect representations or parquet objects
and supports parallel processing for efficient data retrieval and transformation.
"""

# imports
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Generator, List, Optional, Set, Union

# packages
import datasets
from rich.console import Console
from rich.progress import Progress, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn

# project imports
from kl3m_data.logger import LOGGER
from kl3m_data.metrics.quality_metrics import get_metrics
from kl3m_data.pipeline.s3.dataset import DatasetPipeline
from kl3m_data.utils.s3_utils import S3Stage, get_object_bytes, get_s3_client
from kl3m_data.utils.parquet_utils import deserialize_document_bytes


def extract_tokens_and_metadata(
    doc_data: Dict, 
    include_score: bool = True,
    include_metrics: bool = False
) -> Dict:
    """
    Extract tokens and metadata from a document.
    
    Args:
        doc_data: Document data (either from representation or parquet)
        include_score: Whether to include quality score
        include_metrics: Whether to include detailed metrics
        
    Returns:
        Dict containing tokens and metadata
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
    
    # Add tokens to result
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
        
    Returns:
        int: Number of exported documents
    """
    # Initialize pipeline
    pipeline = DatasetPipeline(dataset_id, key_prefix=key_prefix)
    console = Console()
    
    # Validate source stage
    if source_stage not in [S3Stage.REPRESENTATIONS, S3Stage.PARQUET]:
        raise ValueError(f"Invalid source stage: {source_stage}. Must be REPRESENTATIONS or PARQUET.")
    
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
            f"Exporting {dataset_id} from {source_stage.value}", 
            total=total_keys
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
                        LOGGER.error(f"Error parsing representation data for {key}: {e}")
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
                    include_metrics=include_metrics
                )
                
                # Skip documents with no tokens
                if not result.get("tokens"):
                    with stats_lock:
                        skipped_count += 1
                    return
                
                # Apply score filter if specified
                if score_threshold is not None and result.get("score", 0.0) > score_threshold:
                    with stats_lock:
                        skipped_count += 1
                    return
                
                # Deduplicate if requested
                if deduplicate:
                    # Use first 1024 tokens for deduplication
                    tokens_hash = hash(tuple(result["tokens"][:1024]))
                    with stats_lock:
                        if tokens_hash in seen_hashes:
                            skipped_count += 1
                            return
                        seen_hashes.add(tokens_hash)
                
                # Write to output file
                with output_lock:
                    with open(output_path, "a", encoding="utf-8") as f:
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
        with open(output_path, "w", encoding="utf-8") as f:
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
        console.print(f"Processed: {processed_count}, Skipped: {skipped_count}, Errors: {error_count}")
        
        return exported_count


def push_to_huggingface(
    dataset_id: str,
    output_name: str,
    source_stage: S3Stage = S3Stage.PARQUET,
    key_prefix: Optional[str] = None,
    max_workers: int = 10,
    max_documents: Optional[int] = None,
    score_threshold: Optional[float] = None,
    deduplicate: bool = True,
    include_metrics: bool = False,
    use_temp_file: bool = True,
) -> None:
    """
    Export documents from S3 pipeline directly to Hugging Face.
    
    Args:
        dataset_id: Dataset ID to export
        output_name: Name of the Hugging Face dataset to create/update
        source_stage: Which pipeline stage to export from (REPRESENTATIONS or PARQUET)
        key_prefix: Optional key prefix to filter objects
        max_workers: Maximum number of worker threads for parallel processing
        max_documents: Maximum number of documents to export
        score_threshold: Optional quality score threshold
        deduplicate: Whether to deduplicate documents based on first 1024 tokens
        include_metrics: Whether to include detailed metrics in the output
        use_temp_file: Whether to use a temporary file (True) or stream directly (False)
    """
    console = Console()
    
    if use_temp_file:
        # Use a temporary file approach for better reliability
        import tempfile
        
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as temp_file:
            temp_path = temp_file.name
            console.print(f"Exporting to temporary file {temp_path} before uploading to Hugging Face...")
        
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
            )
            
            if doc_count == 0:
                console.print(f"No documents were exported. Skipping upload to Hugging Face.")
                return
                
            # Then load and push to Hugging Face
            console.print(f"Loading dataset from {temp_path}...")
            dataset = datasets.load_dataset("json", data_files=temp_path)
            
            # Push to Hugging Face
            console.print(f"Pushing {doc_count} documents to Hugging Face dataset '{output_name}'...")
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
                batch_keys = keys[i:i+batch_size]
                
                # Process batch in parallel
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # Submit all tasks in this batch
                    futures = []
                    for key in batch_keys:
                        futures.append(executor.submit(
                            process_single_document, 
                            key, 
                            s3_client, 
                            source_stage, 
                            seen_hashes if deduplicate else None,
                            score_threshold,
                            include_metrics
                        ))
                    
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
            include_metrics: bool = False
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
                        LOGGER.error(f"Error parsing representation data for {key}: {e}")
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
                    include_metrics=include_metrics
                )
                
                # Skip documents with no tokens
                if not result.get("tokens"):
                    return None
                
                # Apply score filter if specified
                if score_threshold is not None and result.get("score", 0.0) > score_threshold:
                    return None
                
                # Deduplicate if requested
                if seen_hashes is not None:
                    # Use first 1024 tokens for deduplication
                    tokens_hash = hash(tuple(result["tokens"][:1024]))
                    if tokens_hash in seen_hashes:
                        return None
                    seen_hashes.add(tokens_hash)
                
                return result
                
            except Exception as e:
                LOGGER.error(f"Error processing {key}: {e}")
                return None
        
        # Stream directly to Hugging Face
        console.print(f"Streaming documents directly to Hugging Face dataset '{output_name}'...")
        
        # Track document count for reporting
        doc_count = 0
        
        # Create and push dataset
        dataset = datasets.Dataset.from_generator(generate_documents)
        dataset.push_to_hub(output_name)
        
        console.print(f"Successfully pushed {doc_count} documents to Hugging Face as {output_name}")