"""
CLI for end-to-end data processing: parse, convert, score, and upload to Hugging Face.
"""

# imports
import argparse
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, Optional, Dict, Generator

# packages
from datasets import Dataset
from huggingface_hub import hf_api
from huggingface_hub.errors import RepositoryNotFoundError
from rich import print as rprint
from rich.console import Console
from rich.panel import Panel

# project imports
from kl3m_data.logger import LOGGER
from kl3m_data.metrics.quality_metrics import get_metrics
from kl3m_data.cli.parsers import get_output_key
from kl3m_data.cli.parquet import get_sample_batch
from kl3m_data.utils.s3_utils import (
    get_s3_client,
    check_object_exists,
    iter_prefix,
    iter_prefix_shard,
    put_object_bytes,
    get_object_bytes
)
from kl3m_data.parsers.parser import parse_object
from kl3m_data.utils.parquet_utils import serialize_document


def process_and_upload(
    dataset_id: str,
    output_name: str,
    key_prefix: Optional[str] = None,
    score_threshold: float = 10.0,
    limit: Optional[int] = None,
    clobber: bool = False,
    max_size: int = 8,
    num_hash_tokens: int = 1024,
) -> None:
    """
    End-to-end process: parse, convert, score and upload to Hugging Face.

    Args:
        dataset_id: Dataset ID to process
        output_name: HuggingFace dataset name to create
        key_prefix: Optional prefix for filtering keys
        score_threshold: Quality score threshold
        limit: Maximum number of samples to upload
        clobber: Whether to overwrite existing files
        max_size: Maximum file size in MB to process
        num_hash_tokens: Number of tokens to hash for deduplication
    """
    console = Console()
    s3_client = get_s3_client()
    
    # Check if output dataset already exists
    try:
        if hf_api.dataset_info(output_name) is not None and not clobber:
            raise ValueError(
                f"Output dataset {output_name} already exists. Choose a different name or use --clobber to overwrite."
            )
    except RepositoryNotFoundError:
        # Need to create for the first time
        pass
    
    # 1. Parse documents
    console.print(Panel(f"[bold blue]Step 1/3: Parsing documents from {dataset_id}...[/bold blue]"))
    
    # Use direct function call instead of running parse_serial
    # This avoids nested progress bars
    bytes_to_mb = 1024 * 1024
    max_size_bytes = max_size * bytes_to_mb
    
    # Manual implementation to avoid nested progress bars
    # Get dataset paths
    if dataset_id:
        dataset_paths = [f"documents/{dataset_id}/"]
    else:
        dataset_paths = ["documents/"]
        
    # Process dataset paths
    good, bad, new = 0, 0, 0
    objects_processed = 0
    
    for dataset_path in dataset_paths:
        console.print(f"Processing {dataset_path}")
        
        # Get iterator based on shard prefix and key prefix
        if key_prefix:
            full_dataset_path = dataset_path.rstrip("/") + "/" + key_prefix
        else:
            full_dataset_path = dataset_path
        
        # Get objects iterator with optimized parameters
        console.print(f"Listing objects with prefix: {full_dataset_path}")
        objects = iter_prefix(
            s3_client, 
            "data.kl3m.ai", 
            full_dataset_path,
            page_size=1000  # Get more objects per request
        )
        
        # Process objects
        for object_key in objects:
            objects_processed += 1
            
            # Print status update periodically
            if objects_processed % 100 == 0:
                console.print(f"Progress: {objects_processed} objects, good={good}, bad={bad}, new={new}")
            
            # Get output key and check if it exists
            output_key = get_output_key(object_key)
            if not clobber:
                if check_object_exists(s3_client, "data.kl3m.ai", output_key):
                    LOGGER.info("Output key already exists: %s", output_key)
                    good += 1
                    continue
                    
            # Parse the object
            try:
                parsed_docs = parse_object(s3_client, "data.kl3m.ai", object_key, max_size=max_size_bytes)
                output_docs = []
                for doc in parsed_docs:
                    if doc.success:
                        good += 1
                        new += 1
                        output_docs.append(doc.to_json_dict())
                    else:
                        bad += 1
                
                if len(output_docs) > 0:
                    # Use optimized put_object_bytes with explicit retries
                    put_object_bytes(
                        s3_client,
                        "data.kl3m.ai",
                        output_key,
                        json.dumps({"documents": output_docs}).encode("utf-8"),
                        retry_count=3,
                        retry_delay=1.0
                    )
            
            except Exception as e:
                LOGGER.error("Error processing object key=%s: %s", object_key, e)
                bad += 1
                
    console.print(f"[green]Parsing completed: good={good} bad={bad} new={new}[/green]")
    
    # 2. Convert to parquet
    console.print(Panel(f"[bold blue]Step 2/3: Converting documents to parquet...[/bold blue]"))
    
    # Setup paths
    representation_path = f"representations/{dataset_id}/"
    parquet_path = f"parquet/{dataset_id}/"
    
    # Convert files
    converted = 0
    skipped = 0
    failed = 0
    
    # Use optimized iterator
    path_iterator = iter_prefix(
        s3_client, 
        "data.kl3m.ai", 
        representation_path,
        page_size=1000  # Get more objects per request
    )
    
    # Process conversions with less verbosity
    i = 0
    update_interval = 25  # Show updates less frequently
    
    for object_key in path_iterator:
        i += 1
        
        # Print status updates periodically
        if i % update_interval == 0:
            console.print(f"Conversion progress: {i} objects, converted={converted}, skipped={skipped}, failed={failed}")
        
        # Get parquet key
        parquet_key = object_key.replace(representation_path, parquet_path)[
            : -len(".json")
        ]
        
        # Check if already exists with improved check_object_exists
        if not clobber:
            if check_object_exists(s3_client, "data.kl3m.ai", parquet_key, retry_count=2):
                skipped += 1
                continue
                
        # Get representation data with improved get_object_bytes
        try:
            representation_buffer = get_object_bytes(
                s3_client, 
                "data.kl3m.ai", 
                object_key,
                retry_count=3,
                retry_delay=0.5
            )
            
            if representation_buffer is None or len(representation_buffer) > max_size_bytes:
                skipped += 1
                continue
                
            representation_data = json.loads(representation_buffer)
            documents = representation_data.get("documents", [])
            
            if len(documents) < 1:
                skipped += 1
                continue
                
            # Convert to parquet
            parquet_bytes = serialize_document(documents[0])
            if parquet_bytes is None:
                failed += 1
                continue
                
            # Upload parquet file with improved put_object_bytes
            put_object_bytes(
                s3_client, 
                "data.kl3m.ai", 
                parquet_key, 
                parquet_bytes,
                retry_count=3,
                retry_delay=1.0
            )
            
            converted += 1
            
        except Exception as e:
            failed += 1
            # Only log every few errors to reduce output volume
            if i % update_interval == 0:
                console.print(f"Error converting object: {e}")
    
    console.print(f"[green]Conversion completed: converted={converted} skipped={skipped} failed={failed}[/green]")
    
    # 3. Score and upload to HuggingFace
    console.print(Panel(f"[bold blue]Step 3/3: Scoring and uploading to HuggingFace as {output_name}...[/bold blue]"))
    
    # Define generator function for Dataset creation with scoring
    def yield_scored_documents() -> Generator[Dict[str, Any], None, None]:
        """Yield documents that pass quality filtering with optimized performance."""
        count = 0
        stats = {
            "included": 0,
            "excluded_score": 0,
            "excluded_empty": 0,
            "excluded_duplicate": 0,
        }
        seen_hashes = set()
        
        # Progress update interval (higher = less verbose)
        update_interval = 500
        
        console.print(f"Starting document processing with score threshold {score_threshold}")
        
        # Use optimized batch size and batch processing
        for doc in get_sample_batch(
            datasets=dataset_id,
            limit=limit,
            shuffle=True,
            batch_size=200  # Process more documents per batch
        ):
            count += 1
            
            # Skip empty documents
            tokens = doc.get("tokens", [])
            if len(tokens) == 0:
                stats["excluded_empty"] += 1
                continue
            
            # Apply quality metrics
            metrics = get_metrics(doc)
            score = metrics.get("score", float('inf'))
            
            # Only include documents with score below threshold
            if score <= score_threshold:
                # Optimize hashing by using a smaller portion of tokens for very large docs
                hash_size = min(num_hash_tokens, len(tokens))
                tokens_to_hash = tokens[0:hash_size]
                
                # Optimize hash creation
                token_hash = hashlib.blake2b(
                    ",".join(map(str, tokens_to_hash)).encode(),
                    digest_size=16,
                ).hexdigest()
                
                # Skip duplicates
                if token_hash in seen_hashes:
                    stats["excluded_duplicate"] += 1
                    continue
                
                # Add to seen hashes
                seen_hashes.add(token_hash)
                stats["included"] += 1
                
                # Yield document with only the required fields (no extra metadata)
                yield {
                    "identifier": doc.get("identifier"),
                    "dataset": doc.get("dataset"),
                    "mime_type": doc.get("mime_type"),
                    "score": score,
                    "tokens": tokens
                }
            else:
                stats["excluded_score"] += 1
            
            # Update progress periodically (less frequently for better performance)
            if count % update_interval == 0:
                # Update on progress and stats
                total_excluded = sum(v for k, v in stats.items() if k != 'included')
                
                console.print(f"Documents processed: {count}, included: {stats['included']} ({round(stats['included']/count*100, 1)}%), "
                             f"excluded: {total_excluded} ({round(total_excluded/count*100, 1)}%)")
                
                # If we have a limit, show progress percentage
                if limit:
                    progress_pct = min(100, round(count / limit * 100, 1))
                    console.print(f"Progress: {progress_pct}% ({count}/{limit})")
        
        # Log final statistics
        total_excluded = sum(v for k, v in stats.items() if k != 'included')
        console.print(f"[green]Dataset {dataset_id} processing complete:[/green]")
        console.print(f"[green]- Total processed: {count}[/green]")
        if count > 0:  # Avoid division by zero
            console.print(f"[green]- Included: {stats['included']} ({round(stats['included']/count*100, 1)}%)[/green]")
            console.print(f"[green]- Excluded: {total_excluded} ({round(total_excluded/count*100, 1)}%)[/green]")
        else:
            console.print(f"[green]- Included: {stats['included']}[/green]")
            console.print(f"[green]- Excluded: {total_excluded}[/green]")
        console.print(f"[green]  - Score threshold: {stats['excluded_score']}[/green]")
        console.print(f"[green]  - Empty documents: {stats['excluded_empty']}[/green]")
        console.print(f"[green]  - Duplicates: {stats['excluded_duplicate']}[/green]")
    
    # Create and upload dataset
    dataset = Dataset.from_generator(yield_scored_documents)
    dataset.push_to_hub(output_name)
    
    console.print(f"[bold green]Upload completed! Dataset available at: https://huggingface.co/datasets/{output_name}[/bold green]")


def main() -> None:
    """
    Main entry point for end-to-end HuggingFace data processing.
    """
    parser = argparse.ArgumentParser(
        description="Parse, convert, score and upload documents to HuggingFace"
    )
    
    parser.add_argument(
        "dataset_id", 
        help="Dataset ID to process"
    )
    parser.add_argument(
        "output_name", 
        help="HuggingFace dataset name to create"
    )
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
    
    args = parser.parse_args()
    
    process_and_upload(
        dataset_id=args.dataset_id,
        output_name=args.output_name,
        key_prefix=args.key_prefix,
        score_threshold=args.score_threshold,
        limit=args.limit,
        clobber=args.clobber,
        max_size=args.max_size,
        num_hash_tokens=args.num_hash_tokens
    )


if __name__ == "__main__":
    main()