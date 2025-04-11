"""
S3 Dataset Pipeline utilities for working with documents through the pipeline stages.
"""

# imports
import json
import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Generator, List, Optional, Set, Tuple, Union, Any

# packages
import boto3

# project imports
from kl3m_data.logger import LOGGER
from kl3m_data.parsers.parser import parse_object
from kl3m_data.utils.parquet_utils import serialize_document
from kl3m_data.utils.s3_utils import (
    S3Stage,
    check_object_exists,
    get_document_key,
    get_index_key,
    get_object_bytes,
    get_parquet_key,
    get_representation_key,
    get_s3_client,
    get_stage_prefix,
    iter_prefix,
    list_dataset_ids,
    put_object_bytes,
)


class DatasetDocument:
    """
    A document in a dataset with support for all pipeline stages.
    """

    def __init__(
        self,
        document_key: str,
        dataset_id: str,
        s3_client: Optional[boto3.client] = None,
        bucket: str = "data.kl3m.ai",
        key_prefix: Optional[str] = None,
    ):
        """
        Initialize a dataset document.

        Args:
            document_key (str): The document key (can be from any stage)
            dataset_id (str): The dataset ID
            s3_client (boto3.client): The S3 client
            bucket (str): The S3 bucket name
            key_prefix (Optional[str]): Optional key prefix for filtering
        """
        self.dataset_id = dataset_id
        self.bucket = bucket
        self.s3_client = s3_client or get_s3_client()
        self.key_prefix = key_prefix

        # Extract the document identifier from the key (last part of the path)
        path_parts = document_key.split("/")
        self.doc_id = (
            path_parts[-1].split(".")[0] if len(path_parts) > 1 else document_key
        )

        # Extract the document path after stage and dataset_id
        # This will include any key_prefix and the document name
        stage_and_dataset = False
        for stage in [
            S3Stage.DOCUMENTS.value,
            S3Stage.REPRESENTATIONS.value,
            S3Stage.PARQUET.value,
        ]:
            stage_dataset_prefix = f"{stage}/{dataset_id}/"
            if document_key.startswith(stage_dataset_prefix):
                # Extract everything after stage/dataset_id/
                self.doc_path = document_key[len(stage_dataset_prefix) :]
                stage_and_dataset = True
                break

        if not stage_and_dataset:
            # If we couldn't extract from a known stage, use the full key
            self.doc_path = document_key

        # Normalize keys for all stages using the document path
        # Combination of stage + dataset_id + doc_path
        self.document_key = f"{S3Stage.DOCUMENTS.value}/{dataset_id}/{self.doc_path}"
        self.representation_key = (
            f"{S3Stage.REPRESENTATIONS.value}/{dataset_id}/{self.doc_path}"
        )

        # For parquet key, remove .json extension if present
        if self.doc_path.endswith(".json"):
            parquet_doc_path = self.doc_path[:-5]  # Remove .json
        else:
            parquet_doc_path = self.doc_path

        self.parquet_key = f"{S3Stage.PARQUET.value}/{dataset_id}/{parquet_doc_path}"

        # Log the keys for debugging
        LOGGER.debug(f"Document key: {self.document_key}")
        LOGGER.debug(f"Representation key: {self.representation_key}")
        LOGGER.debug(f"Parquet key: {self.parquet_key}")

    def exists_in_stage(self, stage: S3Stage) -> bool:
        """
        Check if the document exists in a specific stage.

        Args:
            stage (S3Stage): The stage to check

        Returns:
            bool: Whether the document exists in the stage
        """
        # Use the appropriate key for the stage
        if stage == S3Stage.DOCUMENTS:
            key_to_check = self.document_key
        elif stage == S3Stage.REPRESENTATIONS:
            key_to_check = self.representation_key
        elif stage == S3Stage.PARQUET:
            key_to_check = self.parquet_key
        else:
            raise ValueError(f"Invalid stage: {stage}")

        # Log key being checked for debugging
        LOGGER.debug(f"Checking existence of key: {key_to_check}")

        # Check if the object exists
        result = check_object_exists(self.s3_client, self.bucket, key_to_check)

        # Log the result
        if result:
            LOGGER.debug(f"Key exists: {key_to_check}")
        else:
            LOGGER.debug(f"Key does not exist: {key_to_check}")

        return result

    def get_document_data(self) -> Optional[Dict]:
        """
        Get the document data from Stage 1.

        Returns:
            Optional[Dict]: The document data or None if not found
        """
        # Get document bytes
        doc_bytes = get_object_bytes(self.s3_client, self.bucket, self.document_key)
        if not doc_bytes:
            return None

        # Parse document data
        try:
            return json.loads(doc_bytes)
        except Exception as e:
            LOGGER.error("Error parsing document data: %s", e)
            return None

    def get_representation_data(self) -> Optional[Dict]:
        """
        Get the representation data from Stage 2.

        Returns:
            Optional[Dict]: The representation data or None if not found
        """
        # Get representation bytes
        rep_bytes = get_object_bytes(
            self.s3_client, self.bucket, self.representation_key
        )
        if not rep_bytes:
            return None

        # Parse representation data
        try:
            return json.loads(rep_bytes)
        except Exception as e:
            LOGGER.error("Error parsing representation data: %s", e)
            return None

    def get_parquet_data(self) -> Optional[bytes]:
        """
        Get the parquet data from Stage 3.

        Returns:
            Optional[bytes]: The parquet data or None if not found
        """
        return get_object_bytes(self.s3_client, self.bucket, self.parquet_key)

    def process_to_representations(self, max_size: Optional[int] = None) -> bool:
        """
        Process the document to Stage 2 (representations).

        Args:
            max_size (Optional[int]): Maximum file size in bytes to process

        Returns:
            bool: Whether the processing was successful
        """
        try:
            # Parse the object
            parsed_docs = parse_object(
                self.s3_client, self.bucket, self.document_key, max_size=max_size
            )

            # Check for successful parsing
            if not parsed_docs:
                LOGGER.warning(f"No documents parsed for '{self.doc_id}'")
                return False

            # Convert documents to output format
            output_docs = []
            for doc in parsed_docs:
                if doc.success:
                    output_docs.append(doc.to_json_dict())
                else:
                    LOGGER.error(f"Error parsing document '{self.doc_id}': {doc.error}")

            # Check if we have any documents to output
            if not output_docs:
                LOGGER.warning(f"No successful documents for '{self.doc_id}'")
                return False

            # Upload the representations
            result = put_object_bytes(
                self.s3_client,
                self.bucket,
                self.representation_key,
                json.dumps({"documents": output_docs}).encode("utf-8"),
            )

            return result

        except Exception as e:
            LOGGER.error(f"Error processing to representations: {e}")
            return False

    def process_to_parquet(self) -> bool:
        """
        Process the document from Stage 2 to Stage 3 (parquet).

        Returns:
            bool: Whether the processing was successful
        """
        try:
            # Get representation data
            rep_data = self.get_representation_data()
            if not rep_data:
                LOGGER.warning(f"No representation data found for '{self.doc_id}'")
                return False

            # Check if we have documents
            documents = rep_data.get("documents", [])
            if not documents:
                LOGGER.warning(
                    f"No documents found in representation data for '{self.doc_id}'"
                )
                return False

            # Convert to parquet (use first document)
            parquet_bytes = serialize_document(documents[0])
            if not parquet_bytes:
                LOGGER.error(f"Failed to serialize document '{self.doc_id}' to parquet")
                return False

            # Upload the parquet data
            result = put_object_bytes(
                self.s3_client,
                self.bucket,
                self.parquet_key,
                parquet_bytes,
            )

            return result

        except Exception as e:
            LOGGER.error(f"Error processing to parquet: {e}")
            return False


class DatasetPipeline:
    """
    A pipeline for processing datasets through all stages.
    """

    def __init__(
        self,
        dataset_id: str,
        key_prefix: Optional[str] = None,
        s3_client: Optional[boto3.client] = None,
        bucket: str = "data.kl3m.ai",
    ):
        """
        Initialize a dataset pipeline.

        Args:
            dataset_id (str): The dataset ID
            key_prefix (Optional[str]): Optional key prefix to filter objects within the dataset
            s3_client (boto3.client): The S3 client
            bucket (str): The S3 bucket name
        """
        self.dataset_id = dataset_id
        self.key_prefix = key_prefix
        self.bucket = bucket
        self.s3_client = s3_client or get_s3_client()

        # Configure stage prefixes during initialization
        self.prefixes = {}
        for stage in [S3Stage.DOCUMENTS, S3Stage.REPRESENTATIONS, S3Stage.PARQUET]:
            # Start with the basic stage/dataset_id/ prefix
            prefix = f"{stage.value}/{dataset_id}/"

            # Add key_prefix if specified
            if key_prefix:
                # Make sure key_prefix doesn't start with / but does end with /
                clean_prefix = key_prefix.strip("/")
                if clean_prefix:
                    prefix = f"{prefix}{clean_prefix}/"

            # Store the complete prefix for this stage
            self.prefixes[stage] = prefix

        # Log initialization
        prefix_info = f" with key_prefix '{key_prefix}'" if key_prefix else ""
        LOGGER.info(f"Initialized pipeline for dataset '{dataset_id}'{prefix_info}")

    def iter_stage_keys(self, stage: S3Stage) -> Generator[str, None, None]:
        """
        Iterate over all keys in a stage for this dataset.

        Args:
            stage (S3Stage): The stage to iterate over

        Yields:
            str: The S3 keys in the stage
        """
        # Get the appropriate prefix for this stage
        prefix = self.prefixes.get(stage)
        if not prefix:
            # Fallback to computed prefix if not preconfigured
            prefix = get_stage_prefix(stage, self.dataset_id)
            if self.key_prefix:
                prefix = f"{prefix}{self.key_prefix.strip('/')}/"

        # Count keys for logging
        key_count = 0

        # Iterate over all keys with the prefix
        for key in iter_prefix(self.s3_client, self.bucket, prefix):
            key_count += 1
            if key_count % 1000 == 0:
                LOGGER.info(
                    f"Found {key_count} keys in {stage.value}/{self.dataset_id}"
                )
            yield key

        # Log the final count
        LOGGER.info(f"Found {key_count} total keys in {stage.value}/{self.dataset_id}")

    def iter_documents(
        self, stage: S3Stage = S3Stage.DOCUMENTS
    ) -> Generator[DatasetDocument, None, None]:
        """
        Iterate over all documents in a stage for this dataset.

        Args:
            stage (S3Stage): The stage to iterate over

        Yields:
            DatasetDocument: The dataset documents
        """
        for key in self.iter_stage_keys(stage):
            # Create DatasetDocument with the key_prefix from the pipeline
            yield DatasetDocument(
                document_key=key,
                dataset_id=self.dataset_id,
                s3_client=self.s3_client,
                bucket=self.bucket,
                key_prefix=self.key_prefix,
            )

    def get_document_counts(self) -> Dict[S3Stage, int]:
        """
        Get the number of documents in each stage.

        Returns:
            Dict[S3Stage, int]: The number of documents in each stage
        """
        LOGGER.info(f"Getting document counts for dataset '{self.dataset_id}'")
        counts = {}
        for stage in [S3Stage.DOCUMENTS, S3Stage.REPRESENTATIONS, S3Stage.PARQUET]:
            count = sum(1 for _ in self.iter_stage_keys(stage))
            counts[stage] = count
            LOGGER.info(f"Found {count} documents in stage '{stage.value}'")

        return counts

    def get_missing_documents(
        self, source_stage: S3Stage, target_stage: S3Stage
    ) -> Set[str]:
        """
        Get the set of documents that exist in source_stage but not in target_stage.

        Args:
            source_stage (S3Stage): The source stage
            target_stage (S3Stage): The target stage

        Returns:
            Set[str]: The set of document keys that are missing from the target stage
        """
        LOGGER.info(
            f"Finding documents in '{source_stage.value}' missing from '{target_stage.value}'"
        )

        # Get the normalized source and target prefixes
        source_prefix = self.prefixes.get(source_stage)
        target_prefix = self.prefixes.get(target_stage)

        # Collect all keys from the source stage
        source_keys = set()
        for key in self.iter_stage_keys(source_stage):
            # Extract the document-specific part of the key (after prefix removal)
            if key.startswith(source_prefix):
                # Get just the document path part (after prefix)
                doc_path = key[len(source_prefix) :]

                source_keys.add(doc_path)

        # Collect all keys from the target stage
        target_keys = set()
        for key in self.iter_stage_keys(target_stage):
            # Extract the document-specific part of the key (after prefix removal)
            if key.startswith(target_prefix):
                # Get just the document path part (after prefix)
                doc_path = key[len(target_prefix) :]

                # Add JSON if parquet stage
                if target_stage == S3Stage.PARQUET:
                    doc_path += ".json"

                target_keys.add(doc_path)

        # Find document paths in source but not in target
        missing_paths = source_keys - target_keys

        # Format the result as full document keys with dataset ID
        if self.key_prefix:
            # Add the key prefix to the missing paths
            result = {
                f"{self.dataset_id}/{self.key_prefix.strip('/')}/{path}"
                for path in missing_paths
            }
        else:
            result = {f"{self.dataset_id}/{path}" for path in missing_paths}

        LOGGER.info(
            f"Found {len(result)} documents in '{source_stage.value}' missing from '{target_stage.value}'"
        )
        return result

    def process_stage(
        self,
        source_stage: S3Stage,
        target_stage: S3Stage,
        max_workers: int = 10,
        max_size: Optional[int] = None,
        clobber: bool = False,
    ) -> Tuple[int, int]:
        """
        Process all documents from source_stage to target_stage in parallel.

        Args:
            source_stage (S3Stage): The source stage
            target_stage (S3Stage): The target stage
            max_workers (int): The maximum number of worker threads
            max_size (Optional[int]): Maximum file size in bytes to process
            clobber (bool): Whether to overwrite existing files

        Returns:
            Tuple[int, int]: (processed_count, error_count)
        """
        if clobber:
            # Use the clobber version that processes all documents
            return self.process_stage_with_clobber(
                source_stage, target_stage, max_workers, max_size
            )
        else:
            # Use the optimized version that only processes missing documents
            return self.process_stage_missing_only(
                source_stage, target_stage, max_workers, max_size
            )

    def process_stage_with_clobber(
        self,
        source_stage: S3Stage,
        target_stage: S3Stage,
        max_workers: int = 10,
        max_size: Optional[int] = None,
    ) -> Tuple[int, int]:
        """
        Process all documents from source_stage to target_stage in parallel,
        overwriting any existing documents in the target stage.

        Args:
            source_stage (S3Stage): The source stage
            target_stage (S3Stage): The target stage
            max_workers (int): The maximum number of worker threads
            max_size (Optional[int]): Maximum file size in bytes to process

        Returns:
            Tuple[int, int]: (processed_count, error_count)
        """
        LOGGER.info(
            f"Processing ALL documents {source_stage.value} -> {target_stage.value} for dataset '{self.dataset_id}'"
        )
        LOGGER.info(
            f"Parameters: max_workers={max_workers}, max_size={max_size}, clobber=True"
        )

        start_time = datetime.datetime.now()
        LOGGER.info(f"Process started at {start_time.isoformat()}")

        # Validate stage transition and get the appropriate process function
        process_fn = self._get_process_function(source_stage, target_stage, max_size)

        # Find all documents to process
        LOGGER.info(f"Scanning for ALL documents in source stage: {source_stage.value}")
        task_keys = []
        total_count = 0

        for key in self.iter_stage_keys(source_stage):
            total_count += 1
            if total_count % 1000 == 0:
                LOGGER.info(f"Scanned {total_count} documents so far...")
            task_keys.append(key)

        LOGGER.info(
            f"Found {len(task_keys)} total documents to process with clobber=True"
        )

        # Process the documents
        return self._process_documents(
            task_keys, process_fn, max_workers, start_time, total_count
        )

    def process_stage_missing_only(
        self,
        source_stage: S3Stage,
        target_stage: S3Stage,
        max_workers: int = 10,
        max_size: Optional[int] = None,
    ) -> Tuple[int, int]:
        """
        Process only documents that exist in source_stage but not in target_stage.
        This is more efficient when processing large datasets where most documents
        have already been processed.

        Args:
            source_stage (S3Stage): The source stage
            target_stage (S3Stage): The target stage
            max_workers (int): The maximum number of worker threads
            max_size (Optional[int]): Maximum file size in bytes to process

        Returns:
            Tuple[int, int]: (processed_count, error_count)
        """
        LOGGER.info(
            f"Processing MISSING documents {source_stage.value} -> {target_stage.value} for dataset '{self.dataset_id}'"
        )
        LOGGER.info(
            f"Parameters: max_workers={max_workers}, max_size={max_size}, clobber=False"
        )

        start_time = datetime.datetime.now()
        LOGGER.info(f"Process started at {start_time.isoformat()}")

        # Validate stage transition and get the appropriate process function
        process_fn = self._get_process_function(source_stage, target_stage, max_size)

        # Use get_missing_documents to efficiently find only documents that need processing
        LOGGER.info(
            f"Finding missing documents between {source_stage.value} and {target_stage.value}..."
        )
        missing_docs = self.get_missing_documents(source_stage, target_stage)
        total_count = len(missing_docs)
        LOGGER.info(f"Found {total_count} missing documents to process")

        if not missing_docs:
            LOGGER.info(f"No missing documents to process, returning early")
            return 0, 0

        # Convert document paths to full keys
        task_keys = []
        for doc_path in missing_docs:
            # Format the full key including stage and dataset_id
            stage_key = f"{source_stage.value}/{doc_path}"
            task_keys.append(stage_key)

        # Process the documents
        return self._process_documents(
            task_keys, process_fn, max_workers, start_time, total_count
        )

    def _get_process_function(
        self,
        source_stage: S3Stage,
        target_stage: S3Stage,
        max_size: Optional[int] = None,
    ) -> callable:
        """
        Get the appropriate processing function for the stage transition.

        Args:
            source_stage (S3Stage): The source stage
            target_stage (S3Stage): The target stage
            max_size (Optional[int]): Maximum file size in bytes to process

        Returns:
            callable: The processing function to use
        """
        if (
            source_stage == S3Stage.DOCUMENTS
            and target_stage == S3Stage.REPRESENTATIONS
        ):
            LOGGER.info(f"Processing documents to representations")
            return lambda doc: doc.process_to_representations(max_size)
        elif (
            source_stage == S3Stage.REPRESENTATIONS and target_stage == S3Stage.PARQUET
        ):
            LOGGER.info(f"Processing representations to parquet")
            return lambda doc: doc.process_to_parquet()
        else:
            LOGGER.error(
                f"Invalid stage transition requested: {source_stage} -> {target_stage}"
            )
            raise ValueError(
                f"Invalid stage transition: {source_stage} -> {target_stage}"
            )

    def _process_documents(
        self,
        task_keys: List[str],
        process_fn: callable,
        max_workers: int,
        start_time: datetime.datetime,
        total_count: int,
    ) -> Tuple[int, int]:
        """
        Process a list of document keys in parallel.

        Args:
            task_keys (List[str]): The list of document keys to process
            process_fn (callable): The processing function to apply to each document
            max_workers (int): The maximum number of worker threads
            start_time (datetime.datetime): The start time of the processing
            total_count (int): The total number of documents (for reporting)

        Returns:
            Tuple[int, int]: (processed_count, error_count)
        """
        if not task_keys:
            LOGGER.info(f"No documents to process, returning early")
            return 0, 0

        processed_count = 0
        error_count = 0

        # Process in parallel
        LOGGER.info(
            f"Starting parallel processing with {max_workers} workers for {len(task_keys)} documents"
        )
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_key = {}
            for i, key in enumerate(task_keys):
                doc = DatasetDocument(
                    document_key=key,
                    dataset_id=self.dataset_id,
                    s3_client=self.s3_client,
                    bucket=self.bucket,
                    key_prefix=self.key_prefix,
                )
                future = executor.submit(process_fn, doc)
                future_to_key[future] = key

                if (i + 1) % 1000 == 0 or i + 1 == len(task_keys):
                    LOGGER.info(f"Submitted {i + 1}/{len(task_keys)} tasks to executor")

            # Process results as they complete
            completed_count = 0
            for future in future_to_key:
                key = future_to_key[future]
                doc_id = key.split("/")[-1]  # Extract document ID from key for logging
                try:
                    LOGGER.debug(f"Processing document: {doc_id}")
                    success = future.result()
                    if success:
                        processed_count += 1
                        LOGGER.debug(f"Successfully processed document: {doc_id}")
                    else:
                        error_count += 1
                        LOGGER.warning(f"Failed to process document: {doc_id}")
                except Exception as e:
                    error_count += 1
                    LOGGER.error(f"Exception processing {doc_id}: {e}")

                completed_count += 1
                if completed_count % 100 == 0 or completed_count == len(future_to_key):
                    progress_pct = (completed_count / len(future_to_key)) * 100
                    LOGGER.info(
                        f"Progress: {completed_count}/{len(future_to_key)} documents ({progress_pct:.1f}%) - Success: {processed_count}, Errors: {error_count}"
                    )

        end_time = datetime.datetime.now()
        duration = (end_time - start_time).total_seconds()
        docs_per_second = processed_count / duration if duration > 0 else 0

        LOGGER.info(
            f"Processing complete in {duration:.2f} seconds ({docs_per_second:.2f} docs/sec)"
        )
        LOGGER.info(
            f"Results: Processed: {processed_count}, Errors: {error_count}, Total scanned: {total_count}"
        )
        return processed_count, error_count

    def build_index(self) -> bool:
        """
        Build an index of all representation objects.

        Returns:
            bool: Whether the operation was successful
        """
        try:
            # Get all representation keys
            all_keys = list(self.iter_stage_keys(S3Stage.REPRESENTATIONS))

            # Generate the index path
            index_path = get_index_key(self.dataset_id)

            # Add key_prefix to index path if specified
            if self.key_prefix:
                # Modify the index path to include key_prefix info
                if index_path.endswith(".json.gz"):
                    clean_prefix = self.key_prefix.strip("/").replace("/", "-")
                    index_path = index_path.replace(
                        ".json.gz", f"-{clean_prefix}.json.gz"
                    )

            # Create index content with metadata
            index_data = {
                "objects": all_keys,
                "metadata": {
                    "dataset_id": self.dataset_id,
                    "key_prefix": self.key_prefix,
                    "count": len(all_keys),
                    "created_at": datetime.datetime.now().isoformat(),
                },
            }

            # Upload the index
            compressed_data = json.dumps(index_data).encode("utf-8")
            result = put_object_bytes(
                self.s3_client, self.bucket, index_path, compressed_data
            )

            if result:
                LOGGER.info(
                    f"Successfully built index with {len(all_keys)} objects at {index_path}"
                )
            else:
                LOGGER.error(f"Failed to upload index to {index_path}")

            return result

        except Exception as e:
            LOGGER.error(f"Error building index: {e}")
            return False


if __name__ == "__main__":
    # Test with a specific dataset and key_prefix
    dp = DatasetPipeline("dotgov", key_prefix="acl.gov")

    # process
    dp.process_stage(S3Stage.DOCUMENTS, S3Stage.REPRESENTATIONS)
