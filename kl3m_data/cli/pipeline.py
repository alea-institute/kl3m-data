"""
CLI for the KL3M Data pipeline functionality.
"""

# imports
import argparse
import json
import time
from typing import Any, Optional, List

# packages
from rich.console import Console
from rich.table import Table
from rich.progress import Progress

# project imports
from kl3m_data.logger import LOGGER
from kl3m_data.pipeline.s3.dataset import DatasetPipeline
from kl3m_data.pipeline.s3.hf import (
    export_to_jsonl,
    push_to_huggingface,
    list_dataset_subfolders,
    get_subfolder_status,
)
from kl3m_data.utils.s3_utils import get_s3_client, list_dataset_ids, S3Stage


def status_command(
    dataset_id: str, key_prefix: Optional[str] = None, csv_path: Optional[str] = None
) -> None:
    """
    Show the status of a dataset in the pipeline.

    Args:
        dataset_id (str): Dataset ID
        key_prefix (Optional[str]): Optional key prefix to filter objects
        csv_path (Optional[str]): Optional path to save results as CSV
    """
    # Initialize the pipeline
    pipeline = DatasetPipeline(dataset_id, key_prefix=key_prefix)

    # Get document counts
    counts = pipeline.get_document_counts()

    # Calculate missing documents
    missing_representation = pipeline.get_missing_documents(
        S3Stage.DOCUMENTS, S3Stage.REPRESENTATIONS
    )
    missing_parquet = pipeline.get_missing_documents(
        S3Stage.REPRESENTATIONS, S3Stage.PARQUET
    )

    # Create a table for display
    console = Console()
    table = Table(title=f"Pipeline Status: {dataset_id}")

    # Add columns
    table.add_column("Stage", style="cyan")
    table.add_column("Count", style="green")
    table.add_column("Missing", style="red")

    # Add rows
    table.add_row(S3Stage.DOCUMENTS.value, str(counts.get(S3Stage.DOCUMENTS, 0)), "")
    table.add_row(
        S3Stage.REPRESENTATIONS.value,
        str(counts.get(S3Stage.REPRESENTATIONS, 0)),
        str(len(missing_representation)),
    )
    table.add_row(
        S3Stage.PARQUET.value,
        str(counts.get(S3Stage.PARQUET, 0)),
        str(len(missing_parquet)),
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
                        len(missing_representation),
                    ]
                )
                csvwriter.writerow(
                    [
                        S3Stage.PARQUET.value,
                        counts.get(S3Stage.PARQUET, 0),
                        len(missing_parquet),
                    ]
                )

            console.print(f"[green]CSV output written to {csv_path}[/green]")
        except Exception as e:
            console.print(f"[red]Error writing CSV: {e}[/red]")


def list_datasets_command() -> None:
    """
    List all datasets in the pipeline.
    """
    # Get S3 client
    s3_client = get_s3_client()

    # Get datasets from each stage
    document_datasets = set(
        list_dataset_ids(s3_client, "data.kl3m.ai", S3Stage.DOCUMENTS)
    )
    representation_datasets = set(
        list_dataset_ids(s3_client, "data.kl3m.ai", S3Stage.REPRESENTATIONS)
    )
    parquet_datasets = set(list_dataset_ids(s3_client, "data.kl3m.ai", S3Stage.PARQUET))

    # Get all unique dataset IDs
    all_datasets = document_datasets.union(representation_datasets).union(
        parquet_datasets
    )

    # Create a table for display
    console = Console()
    table = Table(title="Available Datasets")

    # Add columns
    table.add_column("Dataset ID", style="cyan")
    table.add_column("Documents", style="green")
    table.add_column("Representations", style="green")
    table.add_column("Parquet", style="green")

    # Add rows
    for dataset_id in sorted(all_datasets):
        table.add_row(
            dataset_id,
            "✓" if dataset_id in document_datasets else "✗",
            "✓" if dataset_id in representation_datasets else "✗",
            "✓" if dataset_id in parquet_datasets else "✗",
        )

    # Display the table
    console.print(table)


def sublist_command(dataset_id: str, source_stage: Optional[str] = None) -> None:
    """
    List subfolders (top-level prefixes) inside a dataset across all stages or a specific stage.

    Args:
        dataset_id (str): Dataset ID
        source_stage (Optional[str]): Source stage (documents, representations, or parquet)
                                     If None, lists all stages
    """
    # Convert stage string to enum if provided
    stage = None
    if source_stage:
        stage = S3Stage[source_stage.upper()]

    # List the subfolders
    list_dataset_subfolders(dataset_id=dataset_id, source_stage=stage)


def substatus_command(
    dataset_id: str, subfolder: str, csv_path: Optional[str] = None
) -> None:
    """
    Get the status (document counts and missing documents) for a specific subfolder.

    Args:
        dataset_id (str): Dataset ID
        subfolder (str): Subfolder within the dataset to check
        csv_path (Optional[str]): Optional path to save results as CSV
    """
    # Get status for the subfolder
    get_subfolder_status(dataset_id=dataset_id, subfolder=subfolder, csv_path=csv_path)


def process_command(
    dataset_id: str,
    source_stage: str,
    target_stage: str,
    key_prefix: Optional[str] = None,
    max_workers: int = 10,
    max_size: Optional[int] = None,
    clobber: bool = False,
) -> None:
    """
    Process documents from one stage to another.

    Args:
        dataset_id (str): Dataset ID
        source_stage (str): Source stage
        target_stage (str): Target stage
        key_prefix (Optional[str]): Optional key prefix to filter objects
        max_workers (int): Maximum number of worker threads
        max_size (Optional[int]): Maximum file size in bytes
        clobber (bool): Whether to overwrite existing files
    """
    # Convert stage strings to enums
    source = S3Stage[source_stage.upper()]
    target = S3Stage[target_stage.upper()]

    # Initialize the pipeline
    pipeline = DatasetPipeline(dataset_id, key_prefix=key_prefix)

    # Process the stage
    console = Console()
    start_time = time.time()
    with console.status(
        f"Processing {dataset_id}: {source.value} -> {target.value}..."
    ):
        processed, errors = pipeline.process_stage(
            source, target, max_workers, max_size, clobber
        )

    # Calculate processing time
    duration = time.time() - start_time

    # Display results
    console.print(
        f"[green]Processed:[/green] {processed} documents in {duration:.2f} seconds"
    )
    if processed > 0 and duration > 0:
        console.print(f"[blue]Rate:[/blue] {processed / duration:.2f} docs/sec")
    if errors > 0:
        console.print(f"[red]Errors:[/red] {errors} documents")


def process_missing_command(
    dataset_id: str,
    source_stage: str,
    target_stage: str,
    key_prefix: Optional[str] = None,
    max_workers: int = 10,
    max_size: Optional[int] = None,
) -> None:
    """
    Efficiently process only documents that exist in source_stage but not in target_stage.

    Args:
        dataset_id (str): Dataset ID
        source_stage (str): Source stage
        target_stage (str): Target stage
        key_prefix (Optional[str]): Optional key prefix to filter objects
        max_workers (int): Maximum number of worker threads
        max_size (Optional[int]): Maximum file size in bytes
    """
    # Convert stage strings to enums
    source = S3Stage[source_stage.upper()]
    target = S3Stage[target_stage.upper()]

    # Initialize the pipeline
    pipeline = DatasetPipeline(dataset_id, key_prefix=key_prefix)

    # First, get the list of missing documents
    console = Console()
    console.print(
        f"Finding missing documents for {dataset_id}: {source.value} -> {target.value}..."
    )
    missing_docs = pipeline.get_missing_documents(source, target)
    console.print(f"Found {len(missing_docs)} documents to process")

    if not missing_docs:
        console.print("[green]All documents are already processed![/green]")
        return

    # Process the missing documents
    start_time = time.time()
    with console.status(f"Processing {len(missing_docs)} missing documents..."):
        processed, errors = pipeline.process_stage_missing_only(
            source, target, max_workers, max_size
        )

    # Calculate processing time
    duration = time.time() - start_time

    # Display results
    console.print(
        f"[green]Processed:[/green] {processed} documents in {duration:.2f} seconds"
    )
    if processed > 0 and duration > 0:
        console.print(f"[blue]Rate:[/blue] {processed / duration:.2f} docs/sec")
    if errors > 0:
        console.print(f"[red]Errors:[/red] {errors} documents")


def process_all_command(
    dataset_id: str,
    key_prefix: Optional[str] = None,
    max_workers: int = 10,
    max_size: Optional[int] = None,
    clobber: bool = False,
) -> None:
    """
    Process all stages of the pipeline for a dataset (documents → representations → parquet).

    Args:
        dataset_id (str): Dataset ID
        key_prefix (Optional[str]): Optional key prefix to filter objects
        max_workers (int): Maximum number of worker threads
        max_size (Optional[int]): Maximum file size in bytes
        clobber (bool): Whether to overwrite existing files
    """
    # Initialize the pipeline
    pipeline = DatasetPipeline(dataset_id, key_prefix=key_prefix)
    console = Console()

    # Stage 1: Documents → Representations
    console.print(f"[bold]Stage 1:[/bold] Processing documents → representations")
    start_time = time.time()
    with console.status(f"Processing {dataset_id}: documents → representations..."):
        processed_1, errors_1 = pipeline.process_stage(
            S3Stage.DOCUMENTS, S3Stage.REPRESENTATIONS, max_workers, max_size, clobber
        )

    duration_1 = time.time() - start_time
    console.print(
        f"[green]Processed:[/green] {processed_1} documents in {duration_1:.2f} seconds"
    )
    if processed_1 > 0 and duration_1 > 0:
        console.print(f"[blue]Rate:[/blue] {processed_1 / duration_1:.2f} docs/sec")
    if errors_1 > 0:
        console.print(f"[red]Errors:[/red] {errors_1} documents")

    # Stage 2: Representations → Parquet
    console.print(f"\n[bold]Stage 2:[/bold] Processing representations → parquet")
    start_time = time.time()
    with console.status(f"Processing {dataset_id}: representations → parquet..."):
        processed_2, errors_2 = pipeline.process_stage(
            S3Stage.REPRESENTATIONS, S3Stage.PARQUET, max_workers, max_size, clobber
        )

    duration_2 = time.time() - start_time
    console.print(
        f"[green]Processed:[/green] {processed_2} documents in {duration_2:.2f} seconds"
    )
    if processed_2 > 0 and duration_2 > 0:
        console.print(f"[blue]Rate:[/blue] {processed_2 / duration_2:.2f} docs/sec")
    if errors_2 > 0:
        console.print(f"[red]Errors:[/red] {errors_2} documents")

    # Build index (optional if any documents were processed)
    if processed_1 > 0 or processed_2 > 0:
        console.print(f"\n[bold]Building index[/bold] for {dataset_id}")
        with console.status(f"Building index..."):
            success = pipeline.build_index()

        if success:
            console.print(f"[green]Successfully built index for {dataset_id}[/green]")
        else:
            console.print(f"[red]Failed to build index for {dataset_id}[/red]")

    # Summary
    total_processed = processed_1 + processed_2
    total_errors = errors_1 + errors_2
    total_duration = duration_1 + duration_2

    console.print(f"\n[bold]Summary:[/bold]")
    console.print(
        f"Total processed: {total_processed} documents in {total_duration:.2f} seconds"
    )
    if total_processed > 0 and total_duration > 0:
        console.print(f"Overall rate: {total_processed / total_duration:.2f} docs/sec")
    if total_errors > 0:
        console.print(f"Total errors: {total_errors} documents")


def process_all_missing_command(
    dataset_id: str,
    key_prefix: Optional[str] = None,
    max_workers: int = 10,
    max_size: Optional[int] = None,
) -> None:
    """
    Efficiently process only missing documents through all stages of the pipeline.

    Args:
        dataset_id (str): Dataset ID
        key_prefix (Optional[str]): Optional key prefix to filter objects
        max_workers (int): Maximum number of worker threads
        max_size (Optional[int]): Maximum file size in bytes
    """
    # Initialize the pipeline
    pipeline = DatasetPipeline(dataset_id, key_prefix=key_prefix)
    console = Console()

    # Stage 1: Documents → Representations (missing only)
    console.print(
        f"[bold]Stage 1:[/bold] Processing missing documents → representations"
    )
    console.print(f"Finding missing documents...")
    missing_docs_1 = pipeline.get_missing_documents(
        S3Stage.DOCUMENTS, S3Stage.REPRESENTATIONS
    )
    console.print(f"Found {len(missing_docs_1)} documents to process")

    processed_1, errors_1, duration_1 = 0, 0, 0
    if missing_docs_1:
        start_time = time.time()
        with console.status(f"Processing {len(missing_docs_1)} missing documents..."):
            processed_1, errors_1 = pipeline.process_stage_missing_only(
                S3Stage.DOCUMENTS, S3Stage.REPRESENTATIONS, max_workers, max_size
            )

        duration_1 = time.time() - start_time
        console.print(
            f"[green]Processed:[/green] {processed_1} documents in {duration_1:.2f} seconds"
        )
        if processed_1 > 0 and duration_1 > 0:
            console.print(f"[blue]Rate:[/blue] {processed_1 / duration_1:.2f} docs/sec")
        if errors_1 > 0:
            console.print(f"[red]Errors:[/red] {errors_1} documents")
    else:
        console.print("[green]No missing documents to process![/green]")

    # Stage 2: Representations → Parquet (missing only)
    console.print(
        f"\n[bold]Stage 2:[/bold] Processing missing representations → parquet"
    )
    console.print(f"Finding missing documents...")
    missing_docs_2 = pipeline.get_missing_documents(
        S3Stage.REPRESENTATIONS, S3Stage.PARQUET
    )
    console.print(f"Found {len(missing_docs_2)} documents to process")

    processed_2, errors_2, duration_2 = 0, 0, 0
    if missing_docs_2:
        start_time = time.time()
        with console.status(f"Processing {len(missing_docs_2)} missing documents..."):
            processed_2, errors_2 = pipeline.process_stage_missing_only(
                S3Stage.REPRESENTATIONS, S3Stage.PARQUET, max_workers, max_size
            )

        duration_2 = time.time() - start_time
        console.print(
            f"[green]Processed:[/green] {processed_2} documents in {duration_2:.2f} seconds"
        )
        if processed_2 > 0 and duration_2 > 0:
            console.print(f"[blue]Rate:[/blue] {processed_2 / duration_2:.2f} docs/sec")
        if errors_2 > 0:
            console.print(f"[red]Errors:[/red] {errors_2} documents")
    else:
        console.print("[green]No missing documents to process![/green]")

    # Build index (optional if any documents were processed)
    if processed_1 > 0 or processed_2 > 0:
        console.print(f"\n[bold]Building index[/bold] for {dataset_id}")
        with console.status(f"Building index..."):
            success = pipeline.build_index()

        if success:
            console.print(f"[green]Successfully built index for {dataset_id}[/green]")
        else:
            console.print(f"[red]Failed to build index for {dataset_id}[/red]")

    # Summary
    total_processed = processed_1 + processed_2
    total_errors = errors_1 + errors_2
    total_duration = duration_1 + duration_2

    console.print(f"\n[bold]Summary:[/bold]")
    console.print(
        f"Total processed: {total_processed} documents in {total_duration:.2f} seconds"
    )
    if total_processed > 0 and total_duration > 0:
        console.print(f"Overall rate: {total_processed / total_duration:.2f} docs/sec")
    if total_errors > 0:
        console.print(f"Total errors: {total_errors} documents")


def build_index_command(dataset_id: str, key_prefix: Optional[str] = None) -> None:
    """
    Build an index for a dataset.

    Args:
        dataset_id (str): Dataset ID
        key_prefix (Optional[str]): Optional key prefix to filter objects
    """
    # Initialize the pipeline
    pipeline = DatasetPipeline(dataset_id, key_prefix=key_prefix)

    # Build the index
    console = Console()
    with console.status(f"Building index for {dataset_id}..."):
        success = pipeline.build_index()

    # Display results
    if success:
        console.print(f"[green]Successfully built index for {dataset_id}[/green]")
    else:
        console.print(f"[red]Failed to build index for {dataset_id}[/red]")


def export_jsonl_command(
    dataset_id: str,
    output_path: str,
    source_stage: str,
    key_prefix: Optional[str] = None,
    max_workers: int = 10,
    max_documents: Optional[int] = None,
    score_threshold: Optional[float] = None,
    deduplicate: bool = True,
    include_metrics: bool = False,
) -> None:
    """
    Export dataset documents to a JSONL file.

    Args:
        dataset_id: Dataset ID to export
        output_path: Path to output JSONL file
        source_stage: Which pipeline stage to export from (representations or parquet)
        key_prefix: Optional key prefix to filter objects
        max_workers: Maximum number of worker threads for parallel processing
        max_documents: Maximum number of documents to export
        score_threshold: Optional quality score threshold
        deduplicate: Whether to deduplicate documents based on tokens
        include_metrics: Whether to include detailed metrics in the output
    """
    # Convert source stage string to enum
    source = S3Stage[source_stage.upper()]

    # Export to JSONL
    export_to_jsonl(
        dataset_id=dataset_id,
        output_path=output_path,
        source_stage=source,
        key_prefix=key_prefix,
        max_workers=max_workers,
        max_documents=max_documents,
        score_threshold=score_threshold,
        deduplicate=deduplicate,
        include_metrics=include_metrics,
    )


def push_to_hf_command(
    dataset_id: str,
    output_name: str,
    key_prefix: Optional[str] = None,
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
    Export dataset documents to Hugging Face.

    Args:
        dataset_id: Dataset ID to export
        output_name: Name of the Hugging Face dataset to create/update
        key_prefix: Optional key prefix to filter objects
        max_workers: Maximum number of worker threads for parallel processing
        max_documents: Maximum number of documents to export
        score_threshold: Optional quality score threshold
        deduplicate: Whether to deduplicate documents based on tokens
        include_metrics: Whether to include detailed metrics in the output
        use_temp_file: Whether to use a temporary file (more reliable)
        format_type: Output format type, either "tokens" for token IDs or "text" for decoded text
        temp_file_path: Optional custom path for the temporary file. If None, uses system temp directory.
    """
    # Always use PARQUET stage
    source = S3Stage.PARQUET

    # Push to Hugging Face
    push_to_huggingface(
        dataset_id=dataset_id,
        output_name=output_name,
        source_stage=source,
        key_prefix=key_prefix,
        max_workers=max_workers,
        max_documents=max_documents,
        score_threshold=score_threshold,
        deduplicate=deduplicate,
        include_metrics=include_metrics,
        use_temp_file=use_temp_file,
        format_type=format_type,
        temp_file_path=temp_file_path,
    )


def main() -> None:
    """
    Main entry point for the KL3M Data pipeline CLI.
    """
    parser = argparse.ArgumentParser(description="kl3m data pipeline CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # List datasets command
    list_parser = subparsers.add_parser("list", help="List all datasets")

    # Sublist command (list subfolders inside a dataset)
    sublist_parser = subparsers.add_parser(
        "sublist", help="List subfolders inside a dataset"
    )
    sublist_parser.add_argument("dataset_id", help="Dataset ID")
    sublist_parser.add_argument(
        "--source-stage",
        choices=["documents", "representations", "parquet"],
        help="Source stage to list from (if not specified, lists all stages)",
    )

    # Substatus command (show status for a specific subfolder)
    substatus_parser = subparsers.add_parser(
        "substatus", help="Show status for a specific subfolder in a dataset"
    )
    substatus_parser.add_argument("dataset_id", help="Dataset ID")
    substatus_parser.add_argument("subfolder", help="Subfolder within the dataset")
    substatus_parser.add_argument(
        "--csv", dest="csv_path", help="Path to save results as CSV file"
    )

    # Status command
    status_parser = subparsers.add_parser("status", help="Show dataset status")
    status_parser.add_argument("dataset_id", help="Dataset ID")
    status_parser.add_argument(
        "--key-prefix", help="Key prefix to filter objects within the dataset"
    )
    status_parser.add_argument(
        "--csv", dest="csv_path", help="Path to save results as CSV file"
    )

    # Process command
    process_parser = subparsers.add_parser(
        "process", help="Process documents from one stage to another"
    )
    process_parser.add_argument("dataset_id", help="Dataset ID")
    process_parser.add_argument(
        "--key-prefix", help="Key prefix to filter objects within the dataset"
    )
    process_parser.add_argument(
        "source_stage",
        choices=["documents", "representations", "parquet"],
        help="Source stage",
    )
    process_parser.add_argument(
        "target_stage",
        choices=["documents", "representations", "parquet"],
        help="Target stage",
    )
    process_parser.add_argument(
        "--max-workers",
        type=int,
        default=10,
        help="Maximum number of worker threads",
    )
    process_parser.add_argument(
        "--max-size",
        type=int,
        help="Maximum file size in bytes",
    )
    process_parser.add_argument(
        "--clobber",
        action="store_true",
        help="Overwrite existing files",
    )

    # Process Missing command
    process_missing_parser = subparsers.add_parser(
        "process-missing",
        help="Efficiently process only documents missing from target stage",
    )
    process_missing_parser.add_argument("dataset_id", help="Dataset ID")
    process_missing_parser.add_argument(
        "--key-prefix", help="Key prefix to filter objects within the dataset"
    )
    process_missing_parser.add_argument(
        "source_stage",
        choices=["documents", "representations", "parquet"],
        help="Source stage",
    )
    process_missing_parser.add_argument(
        "target_stage",
        choices=["documents", "representations", "parquet"],
        help="Target stage",
    )
    process_missing_parser.add_argument(
        "--max-workers",
        type=int,
        default=10,
        help="Maximum number of worker threads",
    )
    process_missing_parser.add_argument(
        "--max-size",
        type=int,
        help="Maximum file size in bytes",
    )

    # Process All command
    process_all_parser = subparsers.add_parser(
        "process-all",
        help="Process through all stages (documents→representations→parquet)",
    )
    process_all_parser.add_argument("dataset_id", help="Dataset ID")
    process_all_parser.add_argument(
        "--key-prefix", help="Key prefix to filter objects within the dataset"
    )
    process_all_parser.add_argument(
        "--max-workers",
        type=int,
        default=10,
        help="Maximum number of worker threads",
    )
    process_all_parser.add_argument(
        "--max-size",
        type=int,
        help="Maximum file size in bytes",
    )
    process_all_parser.add_argument(
        "--clobber",
        action="store_true",
        help="Overwrite existing files",
    )

    # Process All Missing command
    process_all_missing_parser = subparsers.add_parser(
        "process-all-missing", help="Process only missing documents through all stages"
    )
    process_all_missing_parser.add_argument("dataset_id", help="Dataset ID")
    process_all_missing_parser.add_argument(
        "--key-prefix", help="Key prefix to filter objects within the dataset"
    )
    process_all_missing_parser.add_argument(
        "--max-workers",
        type=int,
        default=10,
        help="Maximum number of worker threads",
    )
    process_all_missing_parser.add_argument(
        "--max-size",
        type=int,
        help="Maximum file size in bytes",
    )

    # Build index command
    index_parser = subparsers.add_parser(
        "build-index", help="Build an index for a dataset"
    )
    index_parser.add_argument("dataset_id", help="Dataset ID")
    index_parser.add_argument(
        "--key-prefix", help="Key prefix to filter objects within the dataset"
    )

    # Export JSONL command
    export_parser = subparsers.add_parser(
        "export-jsonl", help="Export dataset to JSONL file"
    )
    export_parser.add_argument("dataset_id", help="Dataset ID")
    export_parser.add_argument("output_path", help="Path to output JSONL file")
    export_parser.add_argument(
        "source_stage",
        choices=["representations", "parquet"],
        help="Source stage to export from",
    )
    export_parser.add_argument(
        "--key-prefix", help="Key prefix to filter objects within the dataset"
    )
    export_parser.add_argument(
        "--max-workers",
        type=int,
        default=10,
        help="Maximum number of worker threads",
    )
    export_parser.add_argument(
        "--max-documents",
        type=int,
        help="Maximum number of documents to export",
    )
    export_parser.add_argument(
        "--score-threshold",
        type=float,
        help="Quality score threshold (lower is better)",
    )
    export_parser.add_argument(
        "--no-deduplicate",
        action="store_true",
        help="Disable deduplication based on tokens",
    )
    export_parser.add_argument(
        "--include-metrics",
        action="store_true",
        help="Include detailed metrics in output",
    )

    # Push to Hugging Face command
    hf_parser = subparsers.add_parser(
        "push-to-hf", help="Export dataset to Hugging Face"
    )
    hf_parser.add_argument("dataset_id", help="Dataset ID")
    hf_parser.add_argument(
        "output_name", help="Name of the Hugging Face dataset to create"
    )
    hf_parser.add_argument(
        "--key-prefix", help="Key prefix to filter objects within the dataset"
    )
    hf_parser.add_argument(
        "--max-workers",
        type=int,
        default=10,
        help="Maximum number of worker threads",
    )
    hf_parser.add_argument(
        "--max-documents",
        type=int,
        help="Maximum number of documents to export",
    )
    hf_parser.add_argument(
        "--score-threshold",
        type=float,
        help="Quality score threshold (lower is better)",
    )
    hf_parser.add_argument(
        "--no-deduplicate",
        action="store_true",
        help="Disable deduplication based on tokens",
    )
    hf_parser.add_argument(
        "--include-metrics",
        action="store_true",
        help="Include detailed metrics in output",
    )
    hf_parser.add_argument(
        "--direct-streaming",
        action="store_true",
        help="Use direct streaming instead of temporary file",
    )
    hf_parser.add_argument(
        "--format-type",
        choices=["tokens", "text"],
        default="tokens",
        help="Output format type: 'tokens' for token IDs or 'text' for decoded text",
    )
    hf_parser.add_argument(
        "--temp-file-path",
        help="Custom path for the temporary file. If not provided, uses system temp directory",
    )

    # Parse arguments
    args = parser.parse_args()

    # Execute command
    if args.command == "list":
        list_datasets_command()
    elif args.command == "sublist":
        sublist_command(args.dataset_id, args.source_stage)
    elif args.command == "substatus":
        substatus_command(args.dataset_id, args.subfolder, args.csv_path)
    elif args.command == "status":
        status_command(args.dataset_id, args.key_prefix, args.csv_path)
    elif args.command == "process":
        process_command(
            args.dataset_id,
            args.source_stage,
            args.target_stage,
            args.key_prefix,
            args.max_workers,
            args.max_size,
            args.clobber,
        )
    elif args.command == "process-missing":
        process_missing_command(
            args.dataset_id,
            args.source_stage,
            args.target_stage,
            args.key_prefix,
            args.max_workers,
            args.max_size,
        )
    elif args.command == "process-all":
        process_all_command(
            args.dataset_id,
            args.key_prefix,
            args.max_workers,
            args.max_size,
            args.clobber,
        )
    elif args.command == "process-all-missing":
        process_all_missing_command(
            args.dataset_id,
            args.key_prefix,
            args.max_workers,
            args.max_size,
        )
    elif args.command == "build-index":
        build_index_command(args.dataset_id, args.key_prefix)
    elif args.command == "export-jsonl":
        export_jsonl_command(
            args.dataset_id,
            args.output_path,
            args.source_stage,
            args.key_prefix,
            args.max_workers,
            args.max_documents,
            args.score_threshold,
            not args.no_deduplicate,
            args.include_metrics,
        )
    elif args.command == "push-to-hf":
        push_to_hf_command(
            args.dataset_id,
            args.output_name,
            args.key_prefix,
            args.max_workers,
            args.max_documents,
            args.score_threshold,
            not args.no_deduplicate,
            args.include_metrics,
            not args.direct_streaming,
            args.format_type,
            args.temp_file_path,
        )


if __name__ == "__main__":
    main()
