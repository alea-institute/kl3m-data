"""
CLI for end-to-end data processing: parse, convert, score, and upload to Hugging Face.
Uses multithreading to parallelize document processing in a streamlined pipeline.

This module is configured to use the kl3m-004-128k-cased tokenizer for all processing.
All metrics and filtering are based on this tokenizer.
"""

# imports
import argparse
import base64
import hashlib
import json
import queue
import threading
import time
import traceback
import zlib
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from typing import Any, Dict, Generator, List, Optional, Set, Tuple

# packages
from datasets import Dataset  # Base Dataset class
from huggingface_hub import hf_api
from huggingface_hub.errors import RepositoryNotFoundError
from rich.console import Console
from rich.panel import Panel
from tokenizers.tokenizers import Tokenizer

# project imports
from kl3m_data.logger import LOGGER
from kl3m_data.metrics.quality_metrics import get_metrics
from kl3m_data.utils.s3_utils import (
    get_s3_client,
    get_s3_config,
    iter_prefix,
)
from kl3m_data.parsers.parser import parse_object
from kl3m_data.utils.parquet_utils import serialize_document

# Define the tokenizer being used throughout this module
DEFAULT_TOKENIZER_NAME = "alea-institute/kl3m-004-128k-cased"

# Global tokenizer cache to avoid redundant loading
_TOKENIZER_CACHE = {}
_TOKENIZER_CACHE_LOCK = threading.RLock()  # Reentrant lock for thread safety

def get_tokenizer(tokenizer_name: str) -> Tokenizer:
    """
    Get a tokenizer instance from cache or create a new one.
    This function ensures we don't waste resources loading the same tokenizer multiple times.
    Thread-safe implementation with proper locking.
    
    Args:
        tokenizer_name: Name of the tokenizer to load
        
    Returns:
        Tokenizer instance
    """
    with _TOKENIZER_CACHE_LOCK:
        if tokenizer_name not in _TOKENIZER_CACHE:
            LOGGER.debug(f"Loading tokenizer {tokenizer_name} into global cache")
            _TOKENIZER_CACHE[tokenizer_name] = Tokenizer.from_pretrained(tokenizer_name)
        
        return _TOKENIZER_CACHE[tokenizer_name]


# Utility functions for document processing
def extract_mime_type(doc_dict: Dict[str, Any]) -> str:
    """
    Extract mime_type from document dictionary, checking both representations and top-level.

    Args:
        doc_dict: Document dictionary

    Returns:
        Extracted mime_type or "unknown" if not found
    """
    mime_type = "unknown"

    # First check representations
    if "representations" in doc_dict:
        for rep_type, rep_data in doc_dict.get("representations", {}).items():
            if "mime_type" in rep_data:
                mime_type = rep_data.get("mime_type")
                break

            # Specifically check the representation with tokens if available
            if "tokens" in rep_data and "mime_type" in rep_data:
                mime_type = rep_data.get("mime_type")
                break

    # Fall back to top-level mime_type if available
    if mime_type == "unknown" and "mime_type" in doc_dict:
        mime_type = doc_dict.get("mime_type")

    return mime_type


def extract_dataset(doc_dict: Dict[str, Any]) -> str:
    """
    Extract dataset from document dictionary, checking both direct field and identifier pattern.

    Args:
        doc_dict: Document dictionary

    Returns:
        Extracted dataset name or "unknown" if not found
    """
    # First check if dataset is directly provided
    dataset = doc_dict.get("dataset", "unknown")

    # If not available, try to extract from identifier
    if dataset == "unknown" and "identifier" in doc_dict:
        identifier = doc_dict.get("identifier", "")

        # Check for S3 path format (s3://bucket/documents/dataset/...)
        if "documents/" in identifier:
            parts = identifier.split("documents/")
            if len(parts) > 1:
                dataset_path = parts[1].split("/")[0]
                if dataset_path:
                    dataset = dataset_path

    return dataset


def decompress_content(content_data: str) -> str:
    """
    Decompress and decode content data from base64+zlib format.
    This function is intended to be used with a thread pool for parallel processing.
    
    Args:
        content_data: Base64 encoded, zlib compressed content string
        
    Returns:
        Decompressed and decoded content as string, or empty string on error
    """
    try:
        return zlib.decompress(base64.b64decode(content_data)).decode('utf-8')
    except Exception as e:
        # Just return empty string on error - errors will be handled by caller
        return ""


def process_documents_batch(docs: List[Dict], tokenizer: Tokenizer, max_workers: int = 4) -> List[Tuple[Dict, List[int]]]:
    """
    Process a batch of documents in parallel, extracting tokens for each.
    This is useful when working with multiple documents that can be processed independently.
    
    Args:
        docs: List of document dictionaries to process
        tokenizer: Tokenizer to use for encoding
        max_workers: Maximum number of worker threads to use
        
    Returns:
        List of tuples containing (document, tokens) pairs
    """
    if not docs:
        return []
    
    # Define worker function that processes a single document
    def process_doc(doc):
        return (doc, extract_tokens(doc, tokenizer))
    
    # Use thread pool to process documents in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        return list(executor.map(process_doc, docs))

def extract_tokens(doc_dict: Dict[str, Any], output_tokenizer: Tokenizer) -> List[int]:
    """
    Extract tokens from document dictionary, checking all possible locations.
    Tokens should be from the DEFAULT_TOKENIZER_NAME (kl3m-004-128k-cased).
    Uses parallel processing for content decompression when multiple representations exist.

    Args:
        doc_dict: Document dictionary
        output_tokenizer: Tokenizer instance to use for encoding tokens from content

    Returns:
        List of token IDs from kl3m-004-128k-cased tokenizer, or empty list if none found
    """
    tokens = []

    # First check for pre-existing tokens in the right format
    if "representations" in doc_dict:
        for rep_type, rep_data in doc_dict.get("representations", {}).items():
            if "tokens" in rep_data:
                # If tokens is a dict with model names as keys, find tokens for our model
                if isinstance(rep_data["tokens"], dict):
                    model_name = DEFAULT_TOKENIZER_NAME.split("/")[-1]
                    # Look for exact model name match first
                    if model_name in rep_data["tokens"]:
                        return rep_data["tokens"][model_name]
                    # Otherwise take the first available model's tokens
                    for model_tokens in rep_data["tokens"].values():
                        tokens = model_tokens
                        break
                else:
                    tokens = rep_data["tokens"]
                
                # If we found tokens, return them immediately
                if tokens:
                    return tokens
    
    # If we didn't find pre-existing tokens, try to generate tokens from content
    if not tokens and "representations" in doc_dict:
        # Collect all representation content for parallel processing
        content_tasks = []
        rep_types = []
        
        for rep_type, rep_data in doc_dict.get("representations", {}).items():
            # Skip if no content available
            if "content" not in rep_data:
                continue
                
            # Add to parallel processing queue - store rep_type for later identification
            content_tasks.append(rep_data.get("content", ""))
            rep_types.append(rep_type)
        
        # If we have content to process, use parallel decompression
        if content_tasks:
            # Create a thread pool with max(5, number of representations) workers 
            # Small pool avoids too many threads for few representations
            # but allows parallelism for docs with many representations
            with ThreadPoolExecutor(max_workers=max(5, len(content_tasks))) as executor:
                # Process all content in parallel
                decoded_contents = list(executor.map(decompress_content, content_tasks))
                
                # Process the results in order
                for i, content in enumerate(decoded_contents):
                    rep_type = rep_types[i]
                    
                    # Skip empty content (decompression failed)
                    if not content:
                        continue
                    
                    try:
                        # Optimize tokenization for larger documents (>100KB) using batching
                        content_size = len(content)
                        if content_size > 100000:  # 100KB threshold
                            # Batch tokenization for large content
                            LOGGER.debug(f"Using batch tokenization for large content ({content_size/1024:.1f}KB)")
                            # Split into chunks of ~50KB to avoid tokenizer limits
                            chunk_size = 50000
                            chunks = [content[i:i+chunk_size] for i in range(0, content_size, chunk_size)]
                            
                            # Use parallel tokenization for chunks
                            def encode_chunk(text_chunk):
                                return output_tokenizer.encode(text_chunk).ids
                                
                            # Process chunks in parallel with another thread pool
                            with ThreadPoolExecutor(max_workers=min(8, len(chunks))) as chunk_executor:
                                chunk_tokens_list = list(chunk_executor.map(encode_chunk, chunks))
                                
                            # Combine all token lists in order
                            all_tokens = []
                            for chunk_tokens in chunk_tokens_list:
                                all_tokens.extend(chunk_tokens)
                            tokens = all_tokens
                        else:
                            # Standard tokenization for smaller content
                            tokens = output_tokenizer.encode(content).ids
                        
                        # If tokenization was successful, return immediately
                        if tokens:
                            return tokens
                    except Exception as e:
                        LOGGER.debug(f"Failed to tokenize content from {rep_type}: {e}")
                        continue

    # Fall back to top-level tokens if available and we haven't found any yet
    if not tokens and "tokens" in doc_dict:
        tokens = doc_dict.get("tokens", [])

    # Ensure we always return a list even if it's empty
    return tokens


# Define data structures for thread communication
@dataclass
class S3Object:
    """Class for tracking S3 object details."""

    key: str
    status: str = "pending"  # pending, processed, skipped, failed
    error_message: str = ""


@dataclass
class ProcessedDocument:
    """Class for tracking processed document details."""

    identifier: str
    dataset: str
    mime_type: str
    score: float
    tokens: List[int]


def process_and_upload(
    dataset_id: str,
    output_name: str,
    key_prefix: Optional[str] = None,
    score_threshold: float = 10.0,
    limit: Optional[int] = None,
    clobber: bool = False,
    max_size: int = 8,
    num_hash_tokens: int = 1024,
    num_producer_threads: int = 4,
    num_consumer_threads: int = 8,
    queue_size: int = 1000,
    all_documents: bool = False,
) -> None:
    """
    End-to-end process: parse, convert, score and upload to Hugging Face using multithreading.
    Streamlined pipeline that processes each document fully before moving to the next.

    Args:
        dataset_id: Dataset ID to process
        output_name: HuggingFace dataset name to create
        key_prefix: Optional prefix for filtering keys
        score_threshold: Quality score threshold
        limit: Maximum number of samples to upload
        clobber: Whether to overwrite existing files
        max_size: Maximum file size in MB to process
        num_hash_tokens: Number of tokens to hash for deduplication
        num_producer_threads: Number of threads to use for S3 object retrieval
        num_consumer_threads: Number of threads to use for document processing
        queue_size: Size of the queue for communication between threads
    """
    console = Console()
    
    # Preload tokenizer in the main thread to warm up the cache
    # This ensures the first thread doesn't have to wait for loading
    LOGGER.info(f"Preloading tokenizer: {DEFAULT_TOKENIZER_NAME}")
    get_tokenizer(DEFAULT_TOKENIZER_NAME)
    
    # Create optimized S3 client config
    s3_config = get_s3_config(
        pool_size=max(
            32, num_producer_threads * 4
        ),  # Ensure enough connections for multiple threads
        connect_timeout=10,
        read_timeout=60,
        retry_count=3,
        retry_mode="adaptive",
    )
    s3_client = get_s3_client(config=s3_config)

    # Check if output dataset already exists
    try:
        if hf_api.dataset_info(output_name) is not None and not clobber:
            raise ValueError(
                f"Output dataset {output_name} already exists. Choose a different name or use --clobber to overwrite."
            )
    except RepositoryNotFoundError:
        # Need to create for the first time
        pass

    bytes_to_mb = 1024 * 1024
    max_size_bytes = max_size * bytes_to_mb

    # =========================================================================
    # Single end-to-end pipeline: Parse → Convert → Score → Upload
    # =========================================================================
    console.print(
        Panel(
            f"[bold blue]Processing documents from {dataset_id} for HuggingFace upload...[/bold blue]"
        )
    )

    # Log key configuration parameters to help with debugging
    LOGGER.info(f"Processing dataset: {dataset_id}")
    LOGGER.info(f"Quality score threshold: {score_threshold}")
    LOGGER.info(f"Maximum document size: {max_size} MB")
    LOGGER.info(f"Number of hash tokens for deduplication: {num_hash_tokens}")
    LOGGER.info(
        f"Using {num_producer_threads} producer thread(s) and {num_consumer_threads} consumer thread(s)"
    )

    # Create shared objects for thread communication
    object_queue = queue.Queue(maxsize=queue_size)
    scored_doc_queue = queue.Queue(maxsize=queue_size)
    result_queue = queue.Queue()
    stop_event = threading.Event()

    # Stats counters (protected by lock)
    stats_lock = threading.Lock()
    stats = {
        "total_objects": 0,
        "objects_processed": 0,
        "objects_skipped": 0,
        "objects_failed": 0,
        "documents_parsed": 0,
        "documents_converted": 0,
        "documents_scored": 0,
        "documents_included": 0,
        "documents_excluded": {
            "score": 0,
            "filter_score": 0,  # Separate tracking for filter score failures
            "empty": 0,
            "duplicate": 0,
            "large": 0,
            "serialize_error": 0,
        },
        "score_stats": {
            "sum_included": 0.0,  # Sum of scores for included documents
            "sum_excluded": 0.0,  # Sum of scores for excluded documents by threshold 
            "count_included": 0,  # Count of documents with scores (included)
            "count_excluded": 0,  # Count of documents with scores (excluded by threshold)
        },
        "score_bins": {  # Track score distribution in bins
            "0-1": 0,
            "1-2": 0,
            "2-5": 0,
            "5-10": 0,
            "10+": 0
        }
    }

    # Deduplication set with lock
    seen_hashes: Set[str] = set()
    seen_hashes_lock = threading.Lock()

    # Define producer function - lists and enqueues S3 objects
    def producer_fn():
        try:
            if dataset_id:
                dataset_paths = [f"documents/{dataset_id}/"]
            else:
                dataset_paths = ["documents/"]

            for dataset_path in dataset_paths:
                # Get iterator based on prefix
                if key_prefix:
                    full_dataset_path = dataset_path.rstrip("/") + "/" + key_prefix
                else:
                    full_dataset_path = dataset_path

                # Get objects iterator with optimized parameters
                objects = iter_prefix(
                    s3_client,
                    "data.kl3m.ai",
                    full_dataset_path,
                    page_size=100,  # Smaller batch size for better parallelism
                )

                # Enqueue objects for processing
                for object_key in objects:
                    if stop_event.is_set():
                        break

                    # Create and enqueue object
                    s3_obj = S3Object(key=object_key)
                    object_queue.put(s3_obj)

                    # Update stats
                    with stats_lock:
                        stats["total_objects"] += 1

                        # Print progress update periodically
                        if stats["total_objects"] % 100 == 0:
                            result_queue.put(f"Listed {stats['total_objects']} objects")
        except Exception as e:
            result_queue.put(f"Producer error: {e}")
        finally:
            # Signal end of production
            object_queue.put(None)

    # Define end-to-end consumer function - processes S3 objects from retrieval to ready-for-upload
    def end_to_end_consumer_fn():
        # Create optimized local S3 client
        local_s3_config = get_s3_config(
            pool_size=10,
            connect_timeout=10,
            read_timeout=60,
            retry_count=3,
            retry_mode="adaptive",
        )
        local_s3_client = get_s3_client(config=local_s3_config)
        
        # Get tokenizer from cache for better efficiency
        # This avoids redundant tokenizer loading across threads
        local_tokenizer = get_tokenizer(DEFAULT_TOKENIZER_NAME)

        while not stop_event.is_set():
            try:
                # Get object from queue with timeout
                s3_obj = object_queue.get(timeout=1.0)

                # Check for end of queue
                if s3_obj is None:
                    # Re-add None for other consumers and exit
                    object_queue.put(None)
                    break

                # STEP 1: Parse the object
                try:
                    LOGGER.debug(f"Parsing object: {s3_obj.key}")
                    parsed_docs = parse_object(
                        local_s3_client,
                        "data.kl3m.ai",
                        s3_obj.key,
                        max_size=max_size_bytes,
                    )

                    # If no successful docs were parsed, skip to next object
                    successful_docs = [doc for doc in parsed_docs if doc.success]
                    if not successful_docs:
                        with stats_lock:
                            stats["objects_skipped"] += 1
                            stats["objects_processed"] += 1

                        # Check if any docs were parsed but failed
                        failed_docs = [doc for doc in parsed_docs if not doc.success]
                        if failed_docs:
                            LOGGER.warning(
                                f"Found {len(failed_docs)} failed documents in {s3_obj.key}"
                            )
                            for doc in failed_docs[:3]:  # Log first few failures
                                LOGGER.warning(f"  - Failed document: {doc.error}")

                        LOGGER.info(
                            f"No successful documents parsed from {s3_obj.key}, skipping"
                        )
                        s3_obj.status = "no_successful_docs"
                        object_queue.task_done()
                        continue

                    # Update parsing stats
                    with stats_lock:
                        stats["documents_parsed"] += len(successful_docs)

                    # STEP 2: Process each document fully (convert to parquet and score)
                    # Skip processing if stop event is set
                    if stop_event.is_set():
                        continue
                        
                    # Convert all documents to dictionaries for processing
                    doc_dicts = [doc.to_json_dict() for doc in successful_docs]
                    
                    # Use bulk parallel processing if we have enough documents
                    if len(doc_dicts) >= 4:
                        # Process documents in parallel batches
                        # Use max_workers based on number of documents, but cap at 8
                        max_workers = min(8, len(doc_dicts))
                        LOGGER.debug(f"Processing {len(doc_dicts)} documents in parallel with {max_workers} workers")
                        
                        # Process all documents in parallel, getting (doc_dict, tokens) pairs
                        processed_docs = process_documents_batch(doc_dicts, local_tokenizer, max_workers)
                        
                        # Continue processing each document with their extracted tokens
                        for doc_dict, tokens in processed_docs:
                            # Skip if stop event is set
                            if stop_event.is_set():
                                break
                    else:
                        # For small numbers of documents, process serially
                        LOGGER.debug(f"Processing {len(doc_dicts)} documents serially")
                        processed_docs = [(doc_dict, extract_tokens(doc_dict, local_tokenizer)) 
                                         for doc_dict in doc_dicts]
                        
                    # Continue with the rest of processing for each document
                    for doc_dict, tokens in processed_docs:
                        # Skip if stop event is set
                        if stop_event.is_set():
                            break

                        if not tokens:
                            with stats_lock:
                                stats["documents_excluded"]["empty"] += 1
                            LOGGER.info(
                                f"Empty document (no tokens), skipping: {doc_dict.get('identifier', 'unknown')}"
                            )
                            continue

                        # Check document size
                        if len(json.dumps(doc_dict).encode("utf-8")) > max_size_bytes:
                            with stats_lock:
                                stats["documents_excluded"]["large"] += 1
                            LOGGER.info(
                                f"Document too large, skipping: {doc_dict.get('identifier', 'unknown')}"
                            )
                            continue

                        # STEP 3: Convert to parquet format
                        try:
                            # We don't need to save the parquet bytes, just verify the document can be serialized
                            parquet_bytes = serialize_document(doc_dict)
                            if parquet_bytes is None:
                                with stats_lock:
                                    stats["documents_excluded"]["serialize_error"] += 1
                                LOGGER.warning(
                                    f"Parquet serialization failed for {doc_dict.get('identifier', 'unknown')}"
                                )
                                continue

                            # Update conversion stats
                            with stats_lock:
                                stats["documents_converted"] += 1

                        except Exception as e:
                            with stats_lock:
                                stats["documents_excluded"]["serialize_error"] += 1
                            LOGGER.error(
                                f"Error serializing document {doc_dict.get('identifier', 'unknown')}: {e}"
                            )
                            continue

                        # STEP 4: Score the document
                        try:
                            # Create a simplified record for metrics calculation - get_metrics expects tokens directly
                            metrics_record = {
                                "identifier": doc_dict.get("identifier", "unknown"),
                                "tokens": tokens,
                            }

                            # Extract mime_type using the helper function
                            mime_type = extract_mime_type(doc_dict)
                            metrics_record["mime_type"] = mime_type

                            metrics = get_metrics(metrics_record)
                            score = metrics.get("score", float("inf"))

                            # Update scoring stats
                            with stats_lock:
                                stats["documents_scored"] += 1

                            # Log details about the score for debugging
                            identifier = doc_dict.get("identifier", "unknown")
                            LOGGER.debug(
                                f"Document {identifier} scored {score:.2f} (threshold: {score_threshold})"
                            )
                            if "metrics" in metrics:
                                for metric_name, metric_value in metrics.get(
                                    "metrics", {}
                                ).items():
                                    LOGGER.debug(f"  - {metric_name}: {metric_value}")

                        except Exception as e:
                            with stats_lock:
                                stats["documents_excluded"]["score"] += 1
                            LOGGER.error(
                                f"Error scoring document {doc_dict.get('identifier', 'unknown')}: {e}"
                            )
                            continue

                        # Debug log the raw score value and components for analysis
                        identifier = doc_dict.get("identifier", "unknown")
                        LOGGER.debug(
                            f"Document {identifier} raw score before threshold check: {score}"
                        )

                        # Log score components if available (from our updated metrics code)
                        if "_score_components" in metrics:
                            components = metrics["_score_components"]
                            LOGGER.debug(f"Score components for {identifier}:")
                            for metric_name, component_data in components.items():
                                LOGGER.debug(
                                    f"  {metric_name}: value={component_data['value']}, range={component_data['range']}, weight={component_data['weight']}, component={component_data['component']:.4f}"
                                )

                        # Check the score against threshold
                        # If all_documents flag is true, we'll set a very high threshold effectively including all documents
                        effective_threshold = (
                            float("inf") if all_documents else score_threshold
                        )
                        if score <= effective_threshold:
                            # Optimize hashing by using a smaller portion of tokens for very large docs
                            hash_size = min(num_hash_tokens, len(tokens))
                            tokens_to_hash = tokens[0:hash_size]

                            # Create token hash for deduplication
                            token_hash = hashlib.blake2b(
                                ",".join(map(str, tokens_to_hash)).encode(),
                                digest_size=16,
                            ).hexdigest()

                            # Check for duplicates (thread-safe)
                            with seen_hashes_lock:
                                if token_hash in seen_hashes:
                                    with stats_lock:
                                        stats["documents_excluded"]["duplicate"] += 1

                                    identifier = doc_dict.get("identifier", "unknown")
                                    dataset = doc_dict.get("dataset", "unknown")
                                    LOGGER.info(
                                        f"Duplicate document based on token hash, skipping: {identifier} ({dataset})"
                                    )
                                    continue

                                # Add to seen hashes
                                seen_hashes.add(token_hash)

                            # Track inclusion and score stats
                            with stats_lock:
                                stats["documents_included"] += 1
                                stats["score_stats"]["sum_included"] += score
                                stats["score_stats"]["count_included"] += 1
                                
                                # Update score bins
                                if score <= 1.0:
                                    stats["score_bins"]["0-1"] += 1
                                elif score <= 2.0:
                                    stats["score_bins"]["1-2"] += 1
                                elif score <= 5.0:
                                    stats["score_bins"]["2-5"] += 1
                                elif score <= 10.0:
                                    stats["score_bins"]["5-10"] += 1
                                else:
                                    stats["score_bins"]["10+"] += 1

                            # Extract mime_type and dataset using helper functions
                            mime_type = extract_mime_type(doc_dict)
                            dataset = extract_dataset(doc_dict)

                            # Create processed document and put in scored queue
                            scored_doc = {
                                "identifier": doc_dict.get("identifier"),
                                "dataset": dataset,
                                "mime_type": mime_type,
                                "score": score,
                                "tokens": tokens,
                            }
                            scored_doc_queue.put(scored_doc)
                        else:
                            with stats_lock:
                                stats["documents_excluded"]["filter_score"] += 1
                                stats["score_stats"]["sum_excluded"] += score
                                stats["score_stats"]["count_excluded"] += 1
                                
                                # Update score bins even for excluded docs
                                if score <= 1.0:
                                    stats["score_bins"]["0-1"] += 1
                                elif score <= 2.0:
                                    stats["score_bins"]["1-2"] += 1
                                elif score <= 5.0:
                                    stats["score_bins"]["2-5"] += 1
                                elif score <= 10.0:
                                    stats["score_bins"]["5-10"] += 1
                                else:
                                    stats["score_bins"]["10+"] += 1

                            # Log that the document was excluded due to score
                            identifier = doc_dict.get("identifier", "unknown")
                            dataset = doc_dict.get("dataset", "unknown")
                            mime_type = doc_dict.get("mime_type", "unknown")
                            LOGGER.info(
                                f"Document score {score:.2f} > threshold {score_threshold}, skipping: {identifier} ({dataset}, {mime_type})"
                            )

                    # Mark object as successfully processed
                    s3_obj.status = "processed"

                except Exception as e:
                    with stats_lock:
                        stats["objects_failed"] += 1
                    s3_obj.status = "failed"
                    s3_obj.error_message = str(e)
                    error_type = type(e).__name__
                    LOGGER.error(f"Error ({error_type}) processing {s3_obj.key}: {e}")
                    traceback.print_exc()
                    raise e

                # Update processed objects count
                with stats_lock:
                    stats["objects_processed"] += 1

                # Mark task as done
                object_queue.task_done()

            except queue.Empty:
                # Queue timeout, check if we should continue
                continue
            except Exception as e:
                result_queue.put(f"Consumer error: {e}")
                continue

    # Define progress monitor function
    def monitor_fn():
        update_interval = 3.0  # seconds
        last_counts = {"objects": 0, "included": 0}

        try:
            while not stop_event.is_set():
                time.sleep(update_interval)

                with stats_lock:
                    current_stats = stats.copy()

                # Only update if there's been progress
                if (
                    current_stats["objects_processed"] > last_counts["objects"]
                    or current_stats["documents_included"] > last_counts["included"]
                ):
                    last_counts["objects"] = current_stats["objects_processed"]
                    last_counts["included"] = current_stats["documents_included"]

                    # Create a summary message
                    excluded_total = sum(current_stats["documents_excluded"].values())

                    # Get current queue depths safely
                    try:
                        obj_queue_size = object_queue.qsize()
                    except NotImplementedError:
                        # Some platforms don't support qsize()
                        obj_queue_size = "?"

                    try:
                        scored_queue_size = scored_doc_queue.qsize()
                    except NotImplementedError:
                        scored_queue_size = "?"

                    # Add exclusion breakdown to help diagnose filtering issues
                    excluded_breakdown = ", ".join(
                        [
                            f"{key}={val}"
                            for key, val in current_stats["documents_excluded"].items()
                            if val > 0
                        ]
                    )

                    # Calculate current average scores
                    avg_score_msg = ""
                    if current_stats["score_stats"]["count_included"] > 0:
                        avg_included = current_stats["score_stats"]["sum_included"] / current_stats["score_stats"]["count_included"]
                        avg_score_msg = f" | Avg score: {avg_included:.4f}"
                    
                    result_queue.put(
                        f"Progress: {current_stats['objects_processed']}/{current_stats['total_objects']} objects | "
                        f"Parsed: {current_stats['documents_parsed']} docs | "
                        f"Included: {current_stats['documents_included']} | "
                        f"Excluded: {excluded_total} ({excluded_breakdown}){avg_score_msg} | "
                        f"Queue: {obj_queue_size}/{scored_queue_size}"
                    )

                # Break if both queues are empty and no active threads
                if (
                    object_queue.empty()
                    and scored_doc_queue.empty()
                    and all_tasks_done.is_set()
                ):
                    break
        except Exception as e:
            result_queue.put(f"Monitor error: {e}")

    # Start producer thread
    producer_thread = threading.Thread(target=producer_fn)
    producer_thread.daemon = True
    producer_thread.start()

    # Start consumer threads
    consumer_threads = []
    for _ in range(num_consumer_threads):
        t = threading.Thread(target=end_to_end_consumer_fn)
        t.daemon = True
        consumer_threads.append(t)

    # Start all consumer threads (after they're all created)
    for t in consumer_threads:
        t.start()

    # Create event for signaling completion
    all_tasks_done = threading.Event()

    # Start monitor thread
    monitor_thread = threading.Thread(target=monitor_fn)
    monitor_thread.daemon = True
    monitor_thread.start()

    # Helper function to check if all processing is complete
    def are_threads_done():
        return (
            not producer_thread.is_alive()
            and all(not t.is_alive() for t in consumer_threads)
            and scored_doc_queue.empty()
        )

    # Start a separate thread to handle messages from the result queue
    def result_handler_fn():
        try:
            while not stop_event.is_set():
                try:
                    # Get message from result queue with timeout
                    message = result_queue.get(timeout=0.5)
                    console.print(message)
                    result_queue.task_done()
                except queue.Empty:
                    # Check if all threads are done
                    if are_threads_done():
                        all_tasks_done.set()
                        LOGGER.debug(
                            "All processing threads complete, setting all_tasks_done event"
                        )
                        break
                    continue
        except Exception as e:
            LOGGER.error(f"Result handler error: {e}")
            console.print(f"Result handler error: {e}")

    # Start result handler thread
    result_handler_thread = threading.Thread(target=result_handler_fn)
    result_handler_thread.daemon = True
    result_handler_thread.start()

    # Collect documents from the queue first, then create a simple generator
    # This avoids pickle issues with thread-local data in the Console object
    def collect_documents():
        """Collect all documents from the queue into a list first."""
        documents = []
        documents_collected = 0

        console.print("[cyan]Collecting documents for HuggingFace upload...[/cyan]")

        # Create a reporting thread to show progress while collecting
        def collection_reporter():
            last_count = 0
            while not stop_event.is_set():
                time.sleep(2.0)
                if len(documents) > last_count:
                    last_count = len(documents)
                    console.print(f"[cyan]Collected {last_count} documents[/cyan]")
                # Exit if all processing is complete
                if are_threads_done():
                    break

        # Start the reporter thread
        reporter_thread = threading.Thread(target=collection_reporter)
        reporter_thread.daemon = True
        reporter_thread.start()

        # Collect documents until we have enough or processing is complete
        try:
            while True:
                # Check if we should stop collecting
                if limit and len(documents) >= limit:
                    console.print(f"[cyan]Reached limit of {limit} documents[/cyan]")
                    break

                # Check if all processing is complete and queue is empty
                if are_threads_done():
                    break

                # Get document from queue with timeout
                try:
                    doc = scored_doc_queue.get(timeout=0.5)
                    # Verify document has required fields
                    if not all(k in doc for k in ["identifier", "tokens"]):
                        LOGGER.warning(
                            f"Document missing required fields, skipping: {doc.get('identifier', 'unknown')}"
                        )
                        missing_fields = [
                            k for k in ["identifier", "tokens"] if k not in doc
                        ]
                        LOGGER.warning(
                            f"  - Missing fields: {', '.join(missing_fields)}"
                        )
                    else:
                        documents.append(doc)
                        # Log periodically
                        if len(documents) % 1000 == 0:
                            LOGGER.info(
                                f"Collected {len(documents)} documents so far..."
                            )

                    scored_doc_queue.task_done()
                except queue.Empty:
                    continue
                except Exception as e:
                    LOGGER.error(f"Error collecting document: {e}")
                    console.print(f"Error collecting document: {e}")
        finally:
            # Stop the reporter thread
            stop_event.set()
            reporter_thread.join(timeout=2.0)

        # Return the collected documents
        return documents

    # Use a simple generator function that doesn't reference any unpicklable objects
    def yield_scored_documents() -> Generator[Dict[str, Any], None, None]:
        """Yield documents from the prepared list."""
        # This is a clean generator function that can be pickled
        for doc in collected_documents:
            yield doc

    # Create and upload dataset
    try:
        # Wait a bit for initial documents to be processed with a maximum wait time
        max_wait_time = 30.0  # Maximum time to wait in seconds
        start_time = time.time()
        while scored_doc_queue.empty() and producer_thread.is_alive():
            # Check if we've waited too long and there's no progress
            if time.time() - start_time > max_wait_time:
                LOGGER.warning(
                    f"No documents processed after waiting {max_wait_time} seconds. Continuing anyway."
                )
                break
            time.sleep(0.5)

        # Collect documents first to avoid pickling issues
        collected_documents = collect_documents()

        # Check if we have any documents to upload
        if not collected_documents:
            LOGGER.error(
                "No documents available for upload. This could be due to filtering criteria or parsing errors."
            )
            console.print(
                "[bold red]No documents available for upload. Check logs for details.[/bold red]"
            )
            stop_event.set()
        else:
            document_count = len(collected_documents)
            LOGGER.info(f"Successfully collected {document_count} documents for upload")
            console.print(
                f"[cyan]Collected {document_count} documents for upload[/cyan]"
            )

            # Create and upload the dataset with our clean generator
            console.print("[cyan]Creating HuggingFace dataset...[/cyan]")
            try:
                # Import here to avoid circular imports
                from datasets import Dataset, Sequence, Value
                
                dataset = Dataset.from_generator(yield_scored_documents)
                
                # Add tokenizer info to dataset metadata
                dataset = dataset.cast_column("tokens", Sequence(Value("int32")))

                console.print(
                    f"[cyan]Pushing dataset to HuggingFace as {output_name}...[/cyan]"
                )
                dataset.push_to_hub(output_name)
                LOGGER.info(
                    f"Successfully pushed dataset to HuggingFace as {output_name} using tokenizer: {DEFAULT_TOKENIZER_NAME}"
                )
            except Exception as e:
                LOGGER.error(f"Failed to create or upload dataset to HuggingFace: {e}")
                raise

            # Wait for threads to complete with proper logging
            LOGGER.debug("Setting stop event and waiting for threads to complete...")
            stop_event.set()

            producer_thread.join(timeout=5.0)
            if producer_thread.is_alive():
                LOGGER.warning(
                    "Producer thread did not terminate cleanly within timeout"
                )

            for i, t in enumerate(consumer_threads):
                t.join(timeout=5.0)
                if t.is_alive():
                    LOGGER.warning(
                        f"Consumer thread {i} did not terminate cleanly within timeout"
                    )

            monitor_thread.join(timeout=5.0)
            if monitor_thread.is_alive():
                LOGGER.warning(
                    "Monitor thread did not terminate cleanly within timeout"
                )

            result_handler_thread.join(timeout=5.0)
            if result_handler_thread.is_alive():
                LOGGER.warning(
                    "Result handler thread did not terminate cleanly within timeout"
                )

            # Calculate final statistics
            with stats_lock:
                final_stats = stats.copy()

            # Log final statistics
            excluded_total = sum(final_stats["documents_excluded"].values())

            console.print(f"[green]Dataset {dataset_id} processing complete:[/green]")
            console.print(
                f"[green]- Objects processed: {final_stats['objects_processed']}/{final_stats['total_objects']}[/green]"
            )
            console.print(
                f"[green]- Documents parsed: {final_stats['documents_parsed']}[/green]"
            )

            if final_stats["documents_parsed"] > 0:  # Avoid division by zero
                included_pct = round(
                    final_stats["documents_included"]
                    / final_stats["documents_parsed"]
                    * 100,
                    1,
                )
                excluded_pct = round(
                    excluded_total / final_stats["documents_parsed"] * 100, 1
                )

                console.print(
                    f"[green]- Documents included: {final_stats['documents_included']} ({included_pct}%)[/green]"
                )
                console.print(
                    f"[green]- Documents excluded: {excluded_total} ({excluded_pct}%)[/green]"
                )
                console.print(
                    f"[green]  - Score errors: {final_stats['documents_excluded']['score']}[/green]"
                )
                console.print(
                    f"[green]  - Filter score: {final_stats['documents_excluded']['filter_score']}[/green]"
                )
                console.print(
                    f"[green]  - Empty documents: {final_stats['documents_excluded']['empty']}[/green]"
                )
                console.print(
                    f"[green]  - Large documents: {final_stats['documents_excluded']['large']}[/green]"
                )
                console.print(
                    f"[green]  - Serialization errors: {final_stats['documents_excluded']['serialize_error']}[/green]"
                )
                console.print(
                    f"[green]  - Duplicates: {final_stats['documents_excluded']['duplicate']}[/green]"
                )
                
                # Calculate and display average scores
                if final_stats["score_stats"]["count_included"] > 0:
                    avg_included = final_stats["score_stats"]["sum_included"] / final_stats["score_stats"]["count_included"]
                    console.print(f"[green]- Average score (included documents): {avg_included:.4f}[/green]")
                
                if final_stats["score_stats"]["count_excluded"] > 0:
                    avg_excluded = final_stats["score_stats"]["sum_excluded"] / final_stats["score_stats"]["count_excluded"]
                    console.print(f"[green]- Average score (excluded documents): {avg_excluded:.4f}[/green]")
                
                # Display score distribution
                total_scored = sum(final_stats["score_bins"].values())
                if total_scored > 0:
                    console.print(f"[green]- Score distribution:[/green]")
                    for bin_name, count in final_stats["score_bins"].items():
                        if count > 0:
                            percentage = round(count / total_scored * 100, 1)
                            console.print(f"[green]  - {bin_name}: {count} ({percentage}%)[/green]")
            else:
                console.print(
                    f"[green]- Documents included: {final_stats['documents_included']}[/green]"
                )
                console.print(f"[green]- Documents excluded: {excluded_total}[/green]")

            console.print(
                f"[bold green]Upload completed! Dataset available at: https://huggingface.co/datasets/{output_name}[/bold green]"
            )
            console.print(
                f"[green]Dataset uses tokenizer: {DEFAULT_TOKENIZER_NAME}[/green]"
            )

    except KeyboardInterrupt:
        console.print("[bold red]Upload interrupted by user[/bold red]")
        stop_event.set()
    except Exception as e:
        console.print(f"[bold red]Error during upload: {e}[/bold red]")
    finally:
        # Ensure all threads are stopped
        stop_event.set()


def main() -> None:
    """
    Main entry point for end-to-end HuggingFace data processing with streamlined pipeline.
    """
    parser = argparse.ArgumentParser(
        description="Parse, convert, score and upload documents to HuggingFace in a streamlined pipeline"
    )

    # Required arguments
    parser.add_argument("dataset_id", help="Dataset ID to process")
    parser.add_argument("output_name", help="HuggingFace dataset name to create")

    # Processing options
    parser.add_argument("--key-prefix", help="Optional prefix for filtering keys")
    parser.add_argument(
        "--score-threshold",
        type=float,
        default=10.0,
        help="Quality score threshold (default: 10.0)",
    )
    parser.add_argument(
        "--all-documents",
        action="store_true",
        help="Include all documents regardless of quality score",
    )
    parser.add_argument("--limit", type=int, help="Maximum number of samples to upload")
    parser.add_argument(
        "--clobber", action="store_true", help="Overwrite existing files"
    )
    parser.add_argument(
        "--max-size", type=int, default=8, help="Maximum file size in MB to process"
    )
    parser.add_argument(
        "--num-hash-tokens",
        type=int,
        default=1024,
        help="Number of tokens to hash for deduplication",
    )

    # Threading options
    parser.add_argument(
        "--producer-threads",
        type=int,
        default=8,
        help="Number of threads for retrieving S3 objects (default: 4)",
    )
    parser.add_argument(
        "--consumer-threads",
        type=int,
        default=16,
        help="Number of threads for processing documents (default: 4)",
    )
    parser.add_argument(
        "--queue-size",
        type=int,
        default=1000,
        help="Size of queue for communication between threads (default: 200)",
    )

    args = parser.parse_args()

    # Print multithreading info
    print(
        f"Using {args.producer_threads} producer thread(s) and {args.consumer_threads} consumer thread(s)"
    )

    process_and_upload(
        dataset_id=args.dataset_id,
        output_name=args.output_name,
        key_prefix=args.key_prefix,
        score_threshold=args.score_threshold,
        limit=args.limit,
        clobber=args.clobber,
        max_size=args.max_size,
        num_hash_tokens=args.num_hash_tokens,
        num_producer_threads=args.producer_threads,
        num_consumer_threads=args.consumer_threads,
        queue_size=args.queue_size,
        all_documents=args.all_documents,
    )


if __name__ == "__main__":
    main()
