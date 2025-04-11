"""
Fast parquet to jsonl.gz export for KL3M datasets.

This module provides a highly optimized approach to exporting parquet data to jsonl.gz
using parallel processing, queue-based streaming, and efficient compression.
"""

# Standard imports
import argparse
import gzip
import json
import math
import os
import queue
import random
import threading
import time
import multiprocessing
import psutil
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Set, Tuple

# Packages
import boto3
import botocore.config
import pyarrow
import pyarrow.parquet
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, SpinnerColumn

# Project imports
from kl3m_data.logger import LOGGER
from kl3m_data.pipeline.s3.dataset import DatasetPipeline
from kl3m_data.utils.s3_utils import S3Stage, get_s3_client, list_common_prefixes, get_object_bytes
from kl3m_data.utils.parquet_utils import deserialize_document_bytes

# Constants - base values for medium-sized machines
QUEUE_SIZE = 5000  # Size of the queue for producer-consumer pattern
CHUNK_SIZE = 2000  # Number of documents to process in each chunk
FLUSH_THRESHOLD = 50000  # Number of bytes to buffer before writing to disk
S3_BATCH_SIZE = 100  # Number of S3 keys to process in a single batch
S3_MAX_RETRIES = 5  # Maximum number of S3 operation retries
S3_MAX_POOL_CONNECTIONS = 200  # Maximum number of connections in the S3 connection pool
S3_KEYS_PER_LIST_PAGE = 1000  # Number of keys to retrieve in each S3 list operation

def detect_system_resources():
    """
    Detect system resources and return optimized parameter values.
    
    Returns:
        Dict containing optimized parameters for the current system
    """
    try:
        # Get CPU count
        cpu_count = multiprocessing.cpu_count()
        
        # Get available memory
        memory_gb = psutil.virtual_memory().total / (1024 ** 3)
        
        # Get available disk space
        disk_gb = psutil.disk_usage('/').free / (1024 ** 3)
        
        # Determine if we're on a large server
        is_large_server = cpu_count >= 16 and memory_gb >= 32
        is_very_large_server = cpu_count >= 32 and memory_gb >= 64
        
        # Calculate optimal settings
        if is_very_large_server:
            # Settings for very large servers
            max_workers = min(64, cpu_count)
            queue_size = 15000
            pool_connections = 400
            batch_size = 200
            parallel_downloads = 100
        elif is_large_server:
            # Settings for large servers
            max_workers = min(32, cpu_count)
            queue_size = 10000
            pool_connections = 200
            batch_size = 100
            parallel_downloads = 50
        else:
            # Settings for medium servers
            max_workers = min(16, max(4, cpu_count - 2))
            queue_size = 5000
            pool_connections = 100
            batch_size = 50
            parallel_downloads = 20
        
        # Return optimized parameters
        return {
            "max_workers": max_workers,
            "queue_size": queue_size,
            "pool_connections": pool_connections,
            "batch_size": batch_size,
            "parallel_downloads": parallel_downloads,
            "system_info": {
                "cpu_count": cpu_count,
                "memory_gb": round(memory_gb, 1),
                "disk_gb": round(disk_gb, 1)
            }
        }
    except Exception as e:
        # If resource detection fails, return default values
        LOGGER.warning(f"Error detecting system resources: {e}. Using default values.")
        return {
            "max_workers": 16,
            "queue_size": 5000,
            "pool_connections": 100,
            "batch_size": 50,
            "parallel_downloads": 20,
            "system_info": {
                "cpu_count": "unknown",
                "memory_gb": "unknown",
                "disk_gb": "unknown"
            }
        }


def get_optimized_s3_client(max_workers: int = 10) -> boto3.client:
    """
    Get an S3 client optimized for concurrent operations.

    Args:
        max_workers: Number of worker threads to account for

    Returns:
        Optimized boto3 S3 client
    """
    # Configure the client with a generous connection pool and retry settings
    # For large servers, scale pool connections based on worker count but with higher minimum
    pool_connections = min(max(max_workers * 4, 50), S3_MAX_POOL_CONNECTIONS)
    
    config = botocore.config.Config(
        max_pool_connections=pool_connections,
        retries={
            'max_attempts': S3_MAX_RETRIES,
            'mode': 'adaptive'  # Use adaptive retry mode for exponential backoff
        },
        connect_timeout=10,  # Increased timeout for establishing connections
        read_timeout=60,     # Increased timeout for read operations
        tcp_keepalive=True   # Enable TCP keepalive for persistent connections
    )
    
    # Create an S3 client with the optimized configuration
    return boto3.client('s3', config=config)


def list_s3_keys_in_batches(
    s3_client: boto3.client,
    bucket: str,
    prefix: str,
    batch_size: int = S3_KEYS_PER_LIST_PAGE
) -> List[str]:
    """
    List S3 keys in batches using paginated requests.

    Args:
        s3_client: S3 client
        bucket: S3 bucket name
        prefix: S3 key prefix
        batch_size: Number of keys to retrieve in each request

    Returns:
        List of all S3 keys under the prefix
    """
    all_keys = []
    continuation_token = None
    
    # Ensure prefix ends with a slash if not empty
    if prefix and not prefix.endswith('/'):
        prefix += '/'
    
    while True:
        # Prepare the list objects request
        list_kwargs = {
            'Bucket': bucket,
            'Prefix': prefix,
            'MaxKeys': batch_size
        }
        
        # Add continuation token if we have one
        if continuation_token:
            list_kwargs['ContinuationToken'] = continuation_token
        
        try:
            # Execute the request with exponential backoff retry
            response = s3_client.list_objects_v2(**list_kwargs)
            
            # Process the contents
            if 'Contents' in response:
                all_keys.extend([obj['Key'] for obj in response['Contents']])
            
            # Check if there are more keys to fetch
            if not response.get('IsTruncated'):
                break
                
            # Get the continuation token for the next request
            continuation_token = response.get('NextContinuationToken')
            
        except Exception as e:
            LOGGER.error(f"Error listing S3 objects: {e}")
            # Implement a brief retry delay to avoid overwhelming S3
            time.sleep(random.uniform(0.5, 2.0))
            
    return all_keys


def get_s3_objects_in_batch(
    s3_client: boto3.client,
    bucket: str,
    keys: List[str],
    max_parallel_downloads: int = None
) -> Dict[str, bytes]:
    """
    Get multiple S3 objects in parallel using thread pool.

    Args:
        s3_client: S3 client
        bucket: S3 bucket name
        keys: List of S3 keys to retrieve
        max_parallel_downloads: Optional maximum number of parallel downloads

    Returns:
        Dictionary mapping keys to their contents
    """
    results = {}
    
    def get_single_object(key: str) -> Tuple[str, Optional[bytes]]:
        """Helper function to get a single object with retries"""
        retry_count = 0
        while retry_count < S3_MAX_RETRIES:
            try:
                content = get_object_bytes(s3_client, bucket, key)
                return key, content
            except Exception as e:
                retry_count += 1
                if retry_count >= S3_MAX_RETRIES:
                    LOGGER.error(f"Failed to get object {key} after {S3_MAX_RETRIES} attempts: {e}")
                    return key, None
                # Exponential backoff with jitter
                delay = min(2 ** retry_count, 10) * (0.5 + random.random())
                time.sleep(delay)
        
        return key, None
    
    # Use ThreadPoolExecutor to fetch objects in parallel
    # If not specified, detect from system resources or use default
    if max_parallel_downloads is None:
        # Try to get from system resources
        try:
            resources = detect_system_resources()
            max_parallel_downloads = resources["parallel_downloads"]
        except Exception:
            # Fallback to default
            max_parallel_downloads = 50
    
    # Limit by actual number of keys
    max_parallel_downloads = min(len(keys), max_parallel_downloads)
    
    with ThreadPoolExecutor(max_workers=max_parallel_downloads) as executor:
        futures = {executor.submit(get_single_object, key): key for key in keys}
        
        for future in futures:
            try:
                key, content = future.result()
                if content is not None:
                    results[key] = content
            except Exception as e:
                LOGGER.error(f"Error in get_s3_objects_in_batch: {e}")
    
    return results


def extract_tokens_and_metadata(
    doc_data: Dict,
    include_metrics: bool = False,
    format_type: str = "tokens"
) -> Dict:
    """
    Extract tokens and metadata from a document.

    Args:
        doc_data: Document data from parquet
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
        # Default to tokens format
        result["tokens"] = tokens

    # Add metrics if requested
    if include_metrics:
        from kl3m_data.metrics.quality_metrics import get_metrics
        metrics_result = get_metrics(result)
        result["score"] = metrics_result.get("score", 0.0)
        result["metrics"] = metrics_result.get("metrics", {})
    
    return result


class ParquetExporter:
    """
    High-performance exporter that processes parquet files in parallel and streams
    results to a compressed jsonl.gz file.
    """
    
    def __init__(
        self,
        dataset_id: str,
        output_path: str,
        key_prefix: Optional[str] = None,
        max_workers: int = 32,  # Increased default for large servers
        max_documents: Optional[int] = None,
        deduplicate: bool = True,
        include_metrics: bool = False,
        format_type: str = "tokens",
        compression_level: int = 6,
        system_resources: Optional[Dict] = None
    ):
        """
        Initialize the ParquetExporter.
        
        Args:
            dataset_id: Dataset ID to export
            output_path: Path to output JSONL.gz file
            key_prefix: Optional key prefix to filter objects
            max_workers: Maximum number of worker threads for parallel processing (default 32 for large servers)
            max_documents: Maximum number of documents to export
            deduplicate: Whether to deduplicate documents based on first 1024 tokens
            include_metrics: Whether to include detailed metrics in the output
            format_type: Output format type, either "tokens" for token IDs or "text" for decoded text
            compression_level: GZIP compression level (1-9, 9 is highest)
            system_resources: Optional dict with system-specific configurations
        """
        self.dataset_id = dataset_id
        self.output_path = output_path
        self.key_prefix = key_prefix
        self.max_workers = max_workers
        self.max_documents = max_documents
        self.deduplicate = deduplicate
        self.include_metrics = include_metrics
        self.format_type = format_type
        self.compression_level = compression_level
        
        # Get system resources if not provided
        if system_resources is None:
            system_resources = detect_system_resources()
            LOGGER.info(f"Using auto-detected system resources: {system_resources['system_info']}")
        self.system_resources = system_resources
            
        # Initialize statistics
        self.processed_count = 0
        self.skipped_count = 0
        self.exported_count = 0
        self.error_count = 0
        self.batch_count = 0
        
        # Set for deduplication
        self.seen_hashes = set()
        
        # Thread synchronization
        self.stats_lock = threading.Lock()
        self.should_stop = threading.Event()
        
        # Initialize optimized S3 client with system-specific connection pool
        pool_connections = system_resources.get("pool_connections", S3_MAX_POOL_CONNECTIONS)
        self.s3_client = get_optimized_s3_client(max_workers)
        
        # Initialize pipeline with optimized client
        self.pipeline = DatasetPipeline(dataset_id, key_prefix=key_prefix, s3_client=self.s3_client)
        
        # Data queue for communication between producers and consumer
        # Use system-specific queue size if available
        queue_size = system_resources.get("queue_size", QUEUE_SIZE)
        # Ensure queue size is appropriate for worker count as a backup
        queue_size = min(max(queue_size, max_workers * 100), 10000)
        self.result_queue = queue.Queue(maxsize=queue_size)
        
        # Initialize console
        self.console = Console()
        self.console.print(f"Initialized exporter with {max_workers} workers and queue size {queue_size}")
    
    def collect_keys(self) -> List[str]:
        """
        Collect all S3 keys to process using optimized batched listing.
        
        Returns:
            List of S3 keys to process
        """
        self.console.print(f"Collecting keys for {self.dataset_id} using optimized S3 listing...")
        
        # Get prefix for the parquet stage
        prefix = f"{S3Stage.PARQUET.value}/{self.dataset_id}/"
        if self.key_prefix:
            prefix += f"{self.key_prefix.strip('/')}/"
        
        # Use optimized batched listing
        return list_s3_keys_in_batches(
            self.s3_client, 
            "data.kl3m.ai", 
            prefix, 
            batch_size=S3_KEYS_PER_LIST_PAGE
        )
    
    def process_batch(self, key_batch: List[str], progress_task_id=None, progress_obj=None):
        """
        Process a batch of parquet files and put results in the queue.
        
        Args:
            key_batch: Batch of S3 keys to process
            progress_task_id: ID of progress bar task to update
            progress_obj: Progress bar object
        """
        try:
            # Track local stats for this batch
            batch_size = len(key_batch)
            processed_in_batch = 0
            
            # Update batch counter
            with self.stats_lock:
                self.batch_count += 1
                batch_num = self.batch_count
            
            # Log to progress bar if available, otherwise to logger
            if progress_obj and progress_task_id is not None:
                desc = f"Exporting {self.dataset_id} (Batch {batch_num}/{self.batch_count} - {batch_size} keys)"
                progress_obj.update(progress_task_id, description=desc.ljust(80))  # Fixed width
            else:
                LOGGER.info(f"Starting batch {batch_num} with {batch_size} keys")
            
            # Get all objects in the batch at once using system-specific parallel downloads
            parallel_downloads = self.system_resources.get("parallel_downloads") if self.system_resources else None
            object_batch = get_s3_objects_in_batch(
                self.s3_client, 
                "data.kl3m.ai", 
                key_batch,
                max_parallel_downloads=parallel_downloads
            )
            
            # Log success rate to progress bar if available
            success_rate = len(object_batch) / batch_size * 100 if batch_size > 0 else 0
            if progress_obj and progress_task_id is not None:
                retrieval_status = f"Retrieved {len(object_batch)}/{batch_size} objects ({success_rate:.1f}%)"
                desc = f"Exporting {self.dataset_id} (Batch {batch_num}) - {retrieval_status}"
                progress_obj.update(progress_task_id, description=desc.ljust(80))  # Fixed width
            else:
                LOGGER.info(f"Batch {batch_num}: Retrieved {len(object_batch)}/{batch_size} objects ({success_rate:.1f}%)")
            
            # Process each object
            for key, object_bytes in object_batch.items():
                if self.should_stop.is_set():
                    break
                
                try:
                    # Deserialize parquet bytes
                    try:
                        doc_data = deserialize_document_bytes(object_bytes)
                        
                        # Add dataset info from key if not in document
                        if "dataset" not in doc_data:
                            doc_data["dataset"] = self.dataset_id
                    except Exception as e:
                        LOGGER.error(f"Error parsing parquet data for {key}: {e}")
                        with self.stats_lock:
                            self.error_count += 1
                        continue
                    
                    # Extract tokens and metadata
                    result = extract_tokens_and_metadata(
                        doc_data,
                        include_metrics=self.include_metrics,
                        format_type=self.format_type,
                    )
                    
                    # Skip documents with no tokens or text
                    if (
                        self.format_type == "tokens" and not result.get("tokens")
                        or self.format_type == "text" and not result.get("text")
                    ):
                        with self.stats_lock:
                            self.skipped_count += 1
                        continue
                    
                    # Deduplicate if requested
                    if self.deduplicate:
                        if self.format_type == "tokens" and result.get("tokens"):
                            # Use first 1024 tokens for deduplication
                            tokens_hash = hash(tuple(result["tokens"][:1024]))
                            with self.stats_lock:
                                if tokens_hash in self.seen_hashes:
                                    self.skipped_count += 1
                                    continue
                                self.seen_hashes.add(tokens_hash)
                        elif self.format_type == "text" and result.get("text"):
                            # Use first 1000 characters for deduplication with text format
                            text_hash = hash(result["text"][:1000])
                            with self.stats_lock:
                                if text_hash in self.seen_hashes:
                                    self.skipped_count += 1
                                    continue
                                self.seen_hashes.add(text_hash)
                    
                    # Put the result in the queue with backpressure handling
                    while True:
                        try:
                            self.result_queue.put(result, timeout=0.5)
                            break
                        except queue.Full:
                            if self.should_stop.is_set():
                                break
                            # Queue is full, wait briefly before retrying
                            time.sleep(0.1)
                    
                    # Update stats
                    with self.stats_lock:
                        self.processed_count += 1
                        processed_in_batch += 1
                        
                        # Check if we've reached the maximum number of documents
                        if self.max_documents and self.processed_count >= self.max_documents:
                            self.should_stop.set()
                    
                except Exception as e:
                    LOGGER.error(f"Error processing {key}: {e}")
                    with self.stats_lock:
                        self.error_count += 1
            
            # Log batch completion and update progress if provided
            if progress_obj and progress_task_id is not None:
                completion_status = f"Processed {processed_in_batch}/{batch_size} objects"
                desc = f"Exporting {self.dataset_id} (Completed Batch {batch_num}) - {completion_status}"
                progress_obj.update(
                    progress_task_id, 
                    description=desc.ljust(80),  # Fixed width
                    advance=batch_size
                )
            else:
                LOGGER.info(f"Completed batch {batch_num}: Processed {processed_in_batch}/{batch_size} objects")
                
        except Exception as e:
            LOGGER.error(f"Batch processing error: {e}")
        finally:
            # Signal that this producer is done
            self.result_queue.put(None)
    
    def consumer(self, total_producers: int, progress=None):
        """
        Consume results from the queue and write them to the output file.
        
        Args:
            total_producers: Total number of producer threads
            progress: Optional progress bar object
        """
        # Create a status task in the progress bar if available
        status_task = None
        if progress:
            status_task = progress.add_task("[cyan]Status:", total=None, completed=0)
        
        # Open the output file for writing with requested compression level
        with gzip.open(self.output_path, 'wt', encoding='utf-8', compresslevel=self.compression_level) as f:
            buffer = []
            buffer_size = 0
            producers_done = 0
            last_flush_time = time.time()
            last_report_time = time.time()
            start_time = time.time()
            
            while producers_done < total_producers:
                try:
                    # Get result from queue with timeout to periodically check should_stop
                    result = self.result_queue.get(timeout=0.1)
                    
                    # None signals a producer is done
                    if result is None:
                        producers_done += 1
                        if status_task and progress:
                            progress.update(status_task, description=f"[cyan]Status: {producers_done}/{total_producers} producers done")
                        continue
                    
                    # Convert result to JSON string
                    json_str = json.dumps(result) + '\n'
                    
                    # Add to buffer
                    buffer.append(json_str)
                    buffer_size += len(json_str)
                    
                    # Update stats
                    with self.stats_lock:
                        self.exported_count += 1
                    
                    # Flush if buffer is full, reached max documents, or time-based flush
                    current_time = time.time()
                    should_flush = (
                        buffer_size >= FLUSH_THRESHOLD or 
                        (self.max_documents and self.exported_count >= self.max_documents) or
                        (current_time - last_flush_time >= 5.0)  # Flush every 5 seconds maximum
                    )
                    
                    if should_flush and buffer:
                        f.writelines(buffer)
                        buffer = []
                        buffer_size = 0
                        last_flush_time = current_time
                        
                        # Check if we've reached the maximum number of documents
                        if self.max_documents and self.exported_count >= self.max_documents:
                            self.should_stop.set()
                            break
                    
                    # Periodically report statistics (every 2 seconds)
                    if current_time - last_report_time >= 2.0:
                        # Calculate rates
                        elapsed = current_time - start_time
                        total_rate = self.exported_count / max(0.1, elapsed)
                        recent_rate = self.exported_count / (current_time - last_report_time + 0.01)
                        
                        # Update progress status with fixed width to prevent UI jumping
                        if status_task and progress:
                            status_description = (
                                f"[cyan]Status: Exported: {self.exported_count:,} docs | " 
                                f"Rate: {recent_rate:.1f} docs/sec | "
                                f"Avg: {total_rate:.1f} docs/sec | "
                                f"Queue: {self.result_queue.qsize():>5}/{self.result_queue.maxsize:<5} | "
                                f"Producers: {producers_done}/{total_producers}"
                            ).ljust(80)  # Pad to fixed width
                            progress.update(status_task, description=status_description)
                        
                        last_report_time = current_time
                    
                except queue.Empty:
                    # Check if we should stop
                    if self.should_stop.is_set():
                        break
                    
                    # Flush on timeout if buffer has content to ensure regular progress
                    current_time = time.time()
                    if buffer and current_time - last_flush_time >= 5.0:
                        f.writelines(buffer)
                        buffer = []
                        buffer_size = 0
                        last_flush_time = current_time
                    
                    # Update status periodically even when queue is empty
                    if status_task and progress and current_time - last_report_time >= 2.0:
                        elapsed = current_time - start_time
                        total_rate = self.exported_count / max(0.1, elapsed)
                        status_description = (
                            f"[cyan]Status: Exported: {self.exported_count:,} docs | " 
                            f"Avg: {total_rate:.1f} docs/sec | "
                            f"Queue: {self.result_queue.qsize():>5}/{self.result_queue.maxsize:<5} | "
                            f"Producers: {producers_done}/{total_producers}"
                        ).ljust(80)  # Pad to fixed width
                        progress.update(status_task, description=status_description)
                        last_report_time = current_time
                    
                    continue
                    
                except Exception as e:
                    LOGGER.error(f"Consumer error: {e}")
                    if status_task and progress:
                        progress.update(status_task, description=f"[red]Error: {e}")
                        time.sleep(0.5)  # Brief pause to make error visible
            
            # Flush any remaining items in buffer
            if buffer:
                f.writelines(buffer)
    
    def export(self):
        """
        Export parquet files to jsonl.gz format using optimized S3 operations.
        
        Returns:
            int: Number of exported documents
        """
        # Collect all keys to process using optimized S3 listing
        keys = self.collect_keys()
        
        if not keys:
            self.console.print(f"No parquet files found for dataset {self.dataset_id}")
            return 0
        
        total_keys = len(keys)
        self.console.print(f"Found {total_keys} parquet files to process")
        
        # Set up progress bar
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description:<80}"),  # Fixed width of 80 characters
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("({task.completed}/{task.total})"),
            TimeElapsedColumn(),
        ) as progress:
            # Create a progress task for the main progress bar
            task_id = progress.add_task(
                description=f"Exporting {self.dataset_id}", total=total_keys
            )
            
            # Organize keys into batches for better S3 performance
            # Use system-specific batch size if available
            system_batch_size = self.system_resources.get("batch_size", S3_BATCH_SIZE) if self.system_resources else S3_BATCH_SIZE
            # For large servers, optimize batch size based on total keys and worker count
            # Larger batches improve throughput when processing large datasets
            batch_size = min(system_batch_size, max(20, total_keys // (self.max_workers * 2)))
            key_batches = [keys[i:i + batch_size] for i in range(0, len(keys), batch_size)]
            
            # Update progress description with batch information
            desc = f"Exporting {self.dataset_id} - {len(key_batches)} batches of {batch_size} keys each"
            progress.update(task_id, description=desc.ljust(80))
            
            # Start consumer thread
            consumer_thread = threading.Thread(
                target=self.consumer, 
                args=(len(key_batches), progress)
            )
            consumer_thread.daemon = True
            consumer_thread.start()
            
            # Use multiple threads to process batches in parallel
            # Reserve a few threads for the consumer and system operations
            # A good rule for large servers is to use ~80% of available workers for processing
            workers = min(max(1, self.max_workers - 4), len(key_batches))
            
            # Start worker threads for batch processing
            with ThreadPoolExecutor(max_workers=workers) as executor:
                # Submit all batches for processing
                futures = []
                for batch in key_batches:
                    futures.append(
                        executor.submit(
                            self.process_batch, 
                            batch, 
                            task_id, 
                            progress
                        )
                    )
                
                # Wait for all batch processors to complete
                for future in futures:
                    future.result()
            
            # Wait for consumer to finish
            consumer_thread.join()
            
            # Update progress to completion with final statistics in the description
            final_stats = (
                f"Export complete - Total: {total_keys} | "
                f"Processed: {self.processed_count} | "
                f"Exported: {self.exported_count} | "
                f"Skipped: {self.skipped_count} | "
                f"Errors: {self.error_count}"
            ).ljust(80)  # Fixed width
            progress.update(task_id, completed=total_keys, description=final_stats)
            
            # Add a small delay to ensure progress bar updates are visible
            time.sleep(0.5)
        
        # Print minimal completion message (detailed stats already in progress bar)
        self.console.print(f"Export complete: {self.output_path}")
        
        return self.exported_count


def main():
    """Command-line interface for parquet to jsonl.gz export."""
    parser = argparse.ArgumentParser(
        description="Fast export of parquet data to jsonl.gz"
    )
    
    parser.add_argument(
        "dataset_id", 
        help="Dataset ID to export"
    )
    
    parser.add_argument(
        "output_path", 
        help="Path to output JSONL.gz file"
    )
    
    parser.add_argument(
        "--key-prefix", 
        help="Optional key prefix to filter objects"
    )
    
    parser.add_argument(
        "--auto-configure", 
        action="store_true",
        help="Automatically detect system resources and configure optimal settings"
    )
    
    parser.add_argument(
        "--max-workers", 
        type=int, 
        default=32,
        help="Maximum number of worker threads (default: 32 for large servers)"
    )
    
    parser.add_argument(
        "--max-documents", 
        type=int, 
        help="Maximum number of documents to export"
    )
    
    parser.add_argument(
        "--no-deduplicate", 
        action="store_true",
        help="Disable deduplication"
    )
    
    parser.add_argument(
        "--include-metrics", 
        action="store_true",
        help="Include quality metrics in the output"
    )
    
    parser.add_argument(
        "--format", 
        choices=["tokens", "text"], 
        default="tokens",
        help="Output format type (default: tokens)"
    )
    
    parser.add_argument(
        "--compression-level", 
        type=int, 
        choices=range(1, 10), 
        default=6,
        help="GZIP compression level 1-9, higher is smaller but slower (default: 6)"
    )
    
    args = parser.parse_args()
    console = Console()
    
    # If auto-configure is enabled, detect system resources and set optimal parameters
    max_workers = args.max_workers
    system_resources = None
    
    if args.auto_configure:
        system_resources = detect_system_resources()
        max_workers = system_resources["max_workers"]
        
        # Log the detected system resources and configuration
        console.print("[bold green]Auto-configuration enabled[/bold green]")
        console.print(f"Detected system resources:")
        console.print(f"  CPU cores: {system_resources['system_info']['cpu_count']}")
        console.print(f"  Memory: {system_resources['system_info']['memory_gb']} GB")
        console.print(f"  Free disk space: {system_resources['system_info']['disk_gb']} GB")
        console.print(f"Optimal configuration:")
        console.print(f"  Worker threads: {system_resources['max_workers']}")
        console.print(f"  Queue size: {system_resources['queue_size']}")
        console.print(f"  Pool connections: {system_resources['pool_connections']}")
        console.print(f"  Batch size: {system_resources['batch_size']}")
        console.print(f"  Parallel downloads: {system_resources['parallel_downloads']}")
    
    # Create the exporter
    exporter = ParquetExporter(
        dataset_id=args.dataset_id,
        output_path=args.output_path,
        key_prefix=args.key_prefix,
        max_workers=max_workers,
        max_documents=args.max_documents,
        deduplicate=not args.no_deduplicate,
        include_metrics=args.include_metrics,
        format_type=args.format,
        compression_level=args.compression_level,
        system_resources=system_resources,
    )
    
    # Run the export
    start_time = time.time()
    doc_count = exporter.export()
    elapsed_time = time.time() - start_time
    
    # Print concise summary with timing info
    console = Console()
    console.print(f"[bold green]Export complete: {doc_count:,} documents in {elapsed_time:.2f} seconds ({doc_count / elapsed_time:.2f} docs/second)[/bold green]")


if __name__ == "__main__":
    main()