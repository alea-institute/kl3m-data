"""
CLI for end-to-end data processing: parse, convert, score, and upload to Hugging Face.
Uses multithreading to parallelize document processing in a streamlined pipeline.
"""

# imports
import argparse
import hashlib
import json
import os
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple, Set

# packages
from datasets import Dataset
from huggingface_hub import hf_api
from huggingface_hub.errors import RepositoryNotFoundError
from rich import print as rprint
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

# project imports
from kl3m_data.logger import LOGGER
from kl3m_data.metrics.quality_metrics import get_metrics
from kl3m_data.cli.parsers import get_output_key
from kl3m_data.utils.s3_utils import (
    get_s3_client,
    get_s3_config,
    check_object_exists,
    iter_prefix,
    iter_prefix_shard,
    get_object_bytes
)
from kl3m_data.parsers.parser import parse_object
from kl3m_data.utils.parquet_utils import serialize_document

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
    # Create optimized S3 client config
    s3_config = get_s3_config(
        pool_size=max(32, num_producer_threads * 4),  # Ensure enough connections for multiple threads
        connect_timeout=10,
        read_timeout=60,
        retry_count=3,
        retry_mode="adaptive"
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
    console.print(Panel(f"[bold blue]Processing documents from {dataset_id} for HuggingFace upload...[/bold blue]"))
    
    # Log key configuration parameters to help with debugging
    LOGGER.info(f"Processing dataset: {dataset_id}")
    LOGGER.info(f"Quality score threshold: {score_threshold}")
    LOGGER.info(f"Maximum document size: {max_size} MB")
    LOGGER.info(f"Number of hash tokens for deduplication: {num_hash_tokens}")
    LOGGER.info(f"Using {num_producer_threads} producer thread(s) and {num_consumer_threads} consumer thread(s)")
    
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
            "empty": 0,
            "duplicate": 0,
            "large": 0,
            "serialize_error": 0
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
                    page_size=100  # Smaller batch size for better parallelism
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
            retry_mode="adaptive"
        )
        local_s3_client = get_s3_client(config=local_s3_config)
        
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
                    parsed_docs = parse_object(local_s3_client, "data.kl3m.ai", s3_obj.key, max_size=max_size_bytes)
                    
                    # If no successful docs were parsed, skip to next object
                    successful_docs = [doc for doc in parsed_docs if doc.success]
                    if not successful_docs:
                        with stats_lock:
                            stats["objects_skipped"] += 1
                            stats["objects_processed"] += 1
                        LOGGER.info(f"No successful documents parsed from {s3_obj.key}, skipping")
                        s3_obj.status = "no_successful_docs"
                        object_queue.task_done()
                        continue
                    
                    # Update parsing stats
                    with stats_lock:
                        stats["documents_parsed"] += len(successful_docs)
                    
                    # STEP 2: Process each document fully (convert to parquet and score)
                    for doc in successful_docs:
                        # Skip if stop event is set
                        if stop_event.is_set():
                            break
                            
                        doc_dict = doc.to_json_dict()
                        
                        # Check if document has tokens
                        tokens = doc_dict.get("tokens", [])
                        if not tokens:
                            with stats_lock:
                                stats["documents_excluded"]["empty"] += 1
                            LOGGER.info(f"Empty document (no tokens), skipping: {doc_dict.get('identifier', 'unknown')}")
                            continue
                        
                        # Check document size
                        if len(json.dumps(doc_dict).encode('utf-8')) > max_size_bytes:
                            with stats_lock:
                                stats["documents_excluded"]["large"] += 1
                            LOGGER.info(f"Document too large, skipping: {doc_dict.get('identifier', 'unknown')}")
                            continue
                        
                        # STEP 3: Convert to parquet format
                        try:
                            # We don't need to save the parquet bytes, just verify the document can be serialized
                            parquet_bytes = serialize_document(doc_dict)
                            if parquet_bytes is None:
                                with stats_lock:
                                    stats["documents_excluded"]["serialize_error"] += 1
                                LOGGER.warning(f"Parquet serialization failed for {doc_dict.get('identifier', 'unknown')}")
                                continue
                            
                            # Update conversion stats
                            with stats_lock:
                                stats["documents_converted"] += 1
                                
                        except Exception as e:
                            with stats_lock:
                                stats["documents_excluded"]["serialize_error"] += 1
                            LOGGER.error(f"Error serializing document {doc_dict.get('identifier', 'unknown')}: {e}")
                            continue
                        
                        # STEP 4: Score the document
                        try:
                            metrics = get_metrics(doc_dict)
                            score = metrics.get("score", float('inf'))
                            
                            # Update scoring stats
                            with stats_lock:
                                stats["documents_scored"] += 1
                            
                            # Log details about the score for debugging
                            identifier = doc_dict.get("identifier", "unknown")
                            LOGGER.debug(f"Document {identifier} scored {score:.2f} (threshold: {score_threshold})")
                            if "metrics" in metrics:
                                for metric_name, metric_value in metrics.get("metrics", {}).items():
                                    LOGGER.debug(f"  - {metric_name}: {metric_value}")
                                
                        except Exception as e:
                            with stats_lock:
                                stats["documents_excluded"]["score"] += 1
                            LOGGER.error(f"Error scoring document {doc_dict.get('identifier', 'unknown')}: {e}")
                            continue
                        
                        # Check the score against threshold
                        # If all_documents flag is true, we'll set a very high threshold effectively including all documents
                        effective_threshold = float('inf') if all_documents else score_threshold
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
                                    LOGGER.info(f"Duplicate document based on token hash, skipping: {identifier} ({dataset})")
                                    continue
                                
                                # Add to seen hashes
                                seen_hashes.add(token_hash)
                            
                            # Track inclusion
                            with stats_lock:
                                stats["documents_included"] += 1
                            
                            # Create processed document and put in scored queue
                            scored_doc = {
                                "identifier": doc_dict.get("identifier"),
                                "dataset": doc_dict.get("dataset"),
                                "mime_type": doc_dict.get("mime_type"),
                                "score": score,
                                "tokens": tokens
                            }
                            scored_doc_queue.put(scored_doc)
                        else:
                            with stats_lock:
                                stats["documents_excluded"]["score"] += 1
                            
                            # Log that the document was excluded due to score
                            identifier = doc_dict.get("identifier", "unknown")
                            dataset = doc_dict.get("dataset", "unknown")
                            mime_type = doc_dict.get("mime_type", "unknown")
                            LOGGER.info(f"Document score {score:.2f} > threshold {score_threshold}, skipping: {identifier} ({dataset}, {mime_type})")
                    
                    # Mark object as successfully processed
                    s3_obj.status = "processed"
                    
                except Exception as e:
                    with stats_lock:
                        stats["objects_failed"] += 1
                    s3_obj.status = "failed"
                    s3_obj.error_message = str(e)
                    error_type = type(e).__name__
                    LOGGER.error(f"Error ({error_type}) processing {s3_obj.key}: {e}")
                
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
                if (current_stats["objects_processed"] > last_counts["objects"] or 
                    current_stats["documents_included"] > last_counts["included"]):
                    
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
                    excluded_breakdown = ", ".join([
                        f"{key}={val}" 
                        for key, val in current_stats["documents_excluded"].items() 
                        if val > 0
                    ])
                    
                    result_queue.put(
                        f"Progress: {current_stats['objects_processed']}/{current_stats['total_objects']} objects | "
                        f"Parsed: {current_stats['documents_parsed']} docs | "
                        f"Included: {current_stats['documents_included']} | "
                        f"Excluded: {excluded_total} ({excluded_breakdown}) | "
                        f"Queue: {obj_queue_size}/{scored_queue_size}"
                    )
                
                # Break if both queues are empty and no active threads
                if object_queue.empty() and scored_doc_queue.empty() and all_tasks_done.is_set():
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
                    if (not producer_thread.is_alive() and 
                        all(not t.is_alive() for t in consumer_threads) and
                        scored_doc_queue.empty()):
                        all_tasks_done.set()
                        break
                    continue
        except Exception as e:
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
                if (not producer_thread.is_alive() and 
                    all(not t.is_alive() for t in consumer_threads) and
                    scored_doc_queue.empty()):
                    break
        
        # Start the reporter thread
        reporter_thread = threading.Thread(target=collection_reporter)
        reporter_thread.daemon = True
        reporter_thread.start()
        
        # Collect documents until we have enough or processing is complete
        try:
            while True:
                # Check if we should stop collecting
                if (limit and len(documents) >= limit):
                    console.print(f"[cyan]Reached limit of {limit} documents[/cyan]")
                    break
                
                # Check if all processing is complete and queue is empty
                if (not producer_thread.is_alive() and 
                    all(not t.is_alive() for t in consumer_threads) and
                    scored_doc_queue.empty()):
                    break
                
                # Get document from queue with timeout
                try:
                    doc = scored_doc_queue.get(timeout=0.5)
                    documents.append(doc)
                    scored_doc_queue.task_done()
                except queue.Empty:
                    continue
                except Exception as e:
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
        # Wait a bit for initial documents to be processed
        while scored_doc_queue.empty() and producer_thread.is_alive():
            time.sleep(0.5)
        
        # Collect documents first to avoid pickling issues
        collected_documents = collect_documents()
        
        # Check if we have any documents to upload
        if not collected_documents:
            console.print("[bold red]No documents available for upload. Check logs for details.[/bold red]")
            stop_event.set()
        else:
            console.print(f"[cyan]Collected {len(collected_documents)} documents for upload[/cyan]")
            
            # Create and upload the dataset with our clean generator
            console.print(f"[cyan]Creating HuggingFace dataset...[/cyan]")
            dataset = Dataset.from_generator(yield_scored_documents)
            console.print(f"[cyan]Pushing dataset to HuggingFace as {output_name}...[/cyan]")
            dataset.push_to_hub(output_name)
            
            # Wait for threads to complete
            stop_event.set()
            producer_thread.join(timeout=5.0)
            for t in consumer_threads:
                t.join(timeout=5.0)
            monitor_thread.join(timeout=5.0)
            result_handler_thread.join(timeout=5.0)
            
            # Calculate final statistics
            with stats_lock:
                final_stats = stats.copy()
                
            # Log final statistics
            excluded_total = sum(final_stats["documents_excluded"].values())
            
            console.print(f"[green]Dataset {dataset_id} processing complete:[/green]")
            console.print(f"[green]- Objects processed: {final_stats['objects_processed']}/{final_stats['total_objects']}[/green]")
            console.print(f"[green]- Documents parsed: {final_stats['documents_parsed']}[/green]")
            
            if final_stats['documents_parsed'] > 0:  # Avoid division by zero
                included_pct = round(final_stats['documents_included']/final_stats['documents_parsed']*100, 1)
                excluded_pct = round(excluded_total/final_stats['documents_parsed']*100, 1)
                
                console.print(f"[green]- Documents included: {final_stats['documents_included']} ({included_pct}%)[/green]")
                console.print(f"[green]- Documents excluded: {excluded_total} ({excluded_pct}%)[/green]")
                console.print(f"[green]  - Score threshold: {final_stats['documents_excluded']['score']}[/green]")
                console.print(f"[green]  - Empty documents: {final_stats['documents_excluded']['empty']}[/green]")
                console.print(f"[green]  - Large documents: {final_stats['documents_excluded']['large']}[/green]")
                console.print(f"[green]  - Serialization errors: {final_stats['documents_excluded']['serialize_error']}[/green]")
                console.print(f"[green]  - Duplicates: {final_stats['documents_excluded']['duplicate']}[/green]")
            else:
                console.print(f"[green]- Documents included: {final_stats['documents_included']}[/green]")
                console.print(f"[green]- Documents excluded: {excluded_total}[/green]")
            
            console.print(f"[bold green]Upload completed! Dataset available at: https://huggingface.co/datasets/{output_name}[/bold green]")
    
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
    parser.add_argument(
        "dataset_id", 
        help="Dataset ID to process"
    )
    parser.add_argument(
        "output_name", 
        help="HuggingFace dataset name to create"
    )
    
    # Processing options
    parser.add_argument(
        "--key-prefix", 
        help="Optional prefix for filtering keys"
    )
    parser.add_argument(
        "--score-threshold", 
        type=float, 
        default=10.0,
        help="Quality score threshold (default: 10.0)"
    )
    parser.add_argument(
        "--all-documents",
        action="store_true",
        help="Include all documents regardless of quality score"
    )
    parser.add_argument(
        "--limit", 
        type=int, 
        help="Maximum number of samples to upload"
    )
    parser.add_argument(
        "--clobber", 
        action="store_true", 
        help="Overwrite existing files"
    )
    parser.add_argument(
        "--max-size", 
        type=int, 
        default=8,
        help="Maximum file size in MB to process"
    )
    parser.add_argument(
        "--num-hash-tokens",
        type=int,
        default=1024,
        help="Number of tokens to hash for deduplication"
    )
    
    # Threading options
    parser.add_argument(
        "--producer-threads",
        type=int,
        default=4,
        help="Number of threads for retrieving S3 objects (default: 4)"
    )
    parser.add_argument(
        "--consumer-threads",
        type=int,
        default=4,
        help="Number of threads for processing documents (default: 4)"
    )
    parser.add_argument(
        "--queue-size",
        type=int,
        default=200,
        help="Size of queue for communication between threads (default: 200)"
    )
    
    args = parser.parse_args()
    
    # Print multithreading info
    print(f"Using {args.producer_threads} producer thread(s) and {args.consumer_threads} consumer thread(s)")
    
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
        all_documents=args.all_documents
    )


if __name__ == "__main__":
    main()