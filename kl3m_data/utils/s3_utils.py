"""
S3 utilities
"""

# imports
import datetime
import hashlib
import hmac
import os
import time
from enum import Enum
from pathlib import Path
from typing import Generator, Optional, List
from urllib.parse import quote

# packages
import boto3
import botocore.config
import httpx

# project
from kl3m_data.config import KL3MDataConfig
from kl3m_data.logger import LOGGER

# timeouts
DEFAULT_TIMEOUT = 600

# S3 storage stages
class S3Stage(Enum):
    """
    Enum for S3 storage stages
    """
    DOCUMENTS = "documents"
    REPRESENTATIONS = "representations"
    PARQUET = "parquet"
    INDEX = "index"


def get_s3_config(
    pool_size: Optional[int] = 25,  # Larger connection pool for better parallelism
    connect_timeout: Optional[int] = 10,  # Shorter connect timeout
    read_timeout: Optional[int] = 60,  # Longer read timeout
    retry_count: Optional[int] = 3,  # More retries
    retry_mode: Optional[str] = "adaptive",  # Adaptive retries
    region_name: Optional[str] = None,
) -> botocore.config.Config:
    """
    Get an optimized S3 configuration object with the specified parameters.

    Args:
        pool_size (int): Number of connections in the pool.
        connect_timeout (int): Connection timeout in seconds.
        read_timeout (int): Read timeout in seconds.
        retry_count (int): Number of retries.
        retry_mode (str): Retry mode ('standard', 'adaptive', or 'legacy').
        region_name (str): AWS region name.

    Returns:
        botocore.config.Config: An optimized S3 configuration object.
    """
    # Create a new configuration with optimized defaults
    config_dict = {
        "max_pool_connections": pool_size,
        "connect_timeout": connect_timeout,
        "read_timeout": read_timeout,
        "retries": {
            "max_attempts": retry_count,
            "mode": retry_mode,
        },
    }
    
    if region_name is not None:
        config_dict["region_name"] = region_name
        
    # Create config from dictionary
    config = botocore.config.Config(**config_dict)

    # Log configuration details
    LOGGER.info(
        "S3 configured with region=%s, pool_size=%s, connect_timeout=%s, read_timeout=%s, retry_count=%s, retry_mode=%s",
        region_name,
        pool_size,
        connect_timeout,
        read_timeout,
        retry_count,
        retry_mode,
    )

    return config


def get_s3_client(
    config: Optional[botocore.config.Config] = None,
) -> boto3.client:
    """
    Get an S3 client with the specified configuration, relying on standard
    boto environment variables for credentials.

    Args:
        config (botocore.config.Config): S3 configuration object.

    Returns:
        boto3.client: An S3 client.
    """
    # get default if not provided
    if config is None:
        config = get_s3_config()

    # create the S3 client
    client = boto3.client(
        "s3",
        config=config,
    )

    return client


def put_object_bytes(
    client: boto3.client,
    bucket: str,
    key: str,
    data: str | bytes,
    retry_count: int = 3,
    retry_delay: float = 1.0,
) -> bool:
    """
    Put an object into an S3 bucket with improved retry logic.

    Args:
        client (boto3.client): S3 client.
        bucket (str): Bucket name.
        key (str): Object key.
        data (str | bytes): Object data.
        retry_count (int): Number of retries on failure.
        retry_delay (float): Base delay between retries in seconds.

    Returns:
        bool: Whether the operation succeeded.
    """
    # encode the data if it is a string
    if isinstance(data, str):
        data = data.encode("utf-8")

    # put the object into the bucket with exponential backoff retry
    for attempt in range(retry_count + 1):
        try:
            # put the object
            client.put_object(
                Bucket=bucket,
                Key=key,
                Body=data,
            )
            
            if attempt > 0:
                LOGGER.info("Put object %s/%s (%d bytes) after %d retries", 
                           bucket, key, len(data), attempt)
            else:
                LOGGER.info("Put object %s/%s (%d bytes)", bucket, key, len(data))
                
            return True
            
        except Exception as e:  # pylint: disable=broad-except
            if attempt < retry_count:
                # Calculate exponential backoff delay
                backoff_delay = retry_delay * (2 ** attempt)
                LOGGER.warning("Error putting object %s/%s: %s. Retrying in %.1f seconds... (%d/%d)", 
                              bucket, key, e, backoff_delay, attempt + 1, retry_count)
                time.sleep(backoff_delay)
            else:
                LOGGER.error("Error putting object %s/%s after %d retries: %s", 
                            bucket, key, retry_count, e)
                return False
    
    return False


def put_object_path(
    client: boto3.client, bucket: str, key: str, path: str | Path
) -> bool:
    """
    Put an object into an S3 bucket.

    Args:
        client (boto3.client): S3 client.
        bucket (str): Bucket name.
        key (str): Object key.
        path (str | Path): Path to the object.

    Returns:
        None
    """
    # read the object data
    try:
        with open(path, "rb") as input_file:
            data = input_file.read()
    except Exception as e:  # pylint: disable=broad-except
        LOGGER.error("Error reading object data: %s", e)
        return False

    # put the object into the bucket
    return put_object_bytes(client, bucket, key, data)


def get_object_bytes(
    client: boto3.client,
    bucket: str,
    key: str,
    retry_count: int = 3,
    retry_delay: float = 1.0,
) -> Optional[bytes]:
    """
    Get an object from an S3 bucket with improved retry logic.

    Args:
        client (boto3.client): S3 client.
        bucket (str): Bucket name.
        key (str): Object key.
        retry_count (int): Number of retries on failure.
        retry_delay (float): Delay between retries in seconds.

    Returns:
        bytes: Object data.
    """
    # Implement exponential backoff retry
    for attempt in range(retry_count + 1):
        try:
            response = client.get_object(
                Bucket=bucket,
                Key=key,
            )
            
            # Stream and read content in chunks if needed
            data = response["Body"].read()
            
            if attempt > 0:
                LOGGER.info("Got object %s://%s after %d retries (%d bytes)", 
                           bucket, key, attempt, len(data))
            else:
                LOGGER.info("Got object %s://%s (%d bytes)", bucket, key, len(data))
                
            return data
            
        except client.exceptions.NoSuchKey:
            # Don't retry if the key doesn't exist
            LOGGER.error("Object %s://%s does not exist", bucket, key)
            return None
            
        except Exception as e:  # pylint: disable=broad-except
            if attempt < retry_count:
                # Calculate exponential backoff delay
                backoff_delay = retry_delay * (2 ** attempt)
                LOGGER.warning("Error getting object %s://%s: %s. Retrying in %.1f seconds... (%d/%d)", 
                              bucket, key, e, backoff_delay, attempt + 1, retry_count)
                time.sleep(backoff_delay)
            else:
                LOGGER.error("Error getting object %s://%s after %d retries: %s", 
                            bucket, key, retry_count, e)
                return None
                
    return None


def get_object(
    client: boto3.client,
    bucket: str,
    key: str,
    retry_count: int = 3,
    retry_delay: float = 1.0,
) -> Optional[str]:
    """
    Get an object from an S3 bucket and return it as a string with improved retry logic.

    Args:
        client (boto3.client): S3 client.
        bucket (str): Bucket name.
        key (str): Object key.
        retry_count (int): Number of retries on failure.
        retry_delay (float): Delay between retries in seconds.

    Returns:
        str: Object data as a UTF-8 string, or None if an error occurs.
    """
    # Get the object as bytes
    data_bytes = get_object_bytes(client, bucket, key, retry_count, retry_delay)
    
    # Convert to string if we got data
    if data_bytes is not None:
        try:
            return data_bytes.decode('utf-8')
        except UnicodeDecodeError as e:
            LOGGER.error("Error decoding object %s://%s: %s", bucket, key, e)
            return None
    
    return None


def check_object_exists(
    client: boto3.client,
    bucket: str,
    key: str,
    retry_count: int = 2,
    retry_delay: float = 0.5,
) -> bool:
    """
    Check if an object exists in an S3 bucket with improved retry logic.

    Args:
        client (boto3.client): S3 client.
        bucket (str): Bucket name.
        key (str): Object key.
        retry_count (int): Number of retries on failure.
        retry_delay (float): Delay between retries in seconds.

    Returns:
        bool: Whether the object exists.
    """
    # check if the object exists with retries
    for attempt in range(retry_count + 1):
        try:
            client.head_object(
                Bucket=bucket,
                Key=key,
            )
            return True
            
        except client.exceptions.ClientError as e:
            # If the error is 404, the object doesn't exist
            if e.response['Error']['Code'] == '404':
                return False
                
            # For other client errors, retry if we have attempts left
            if attempt < retry_count:
                backoff_delay = retry_delay * (2 ** attempt)
                LOGGER.warning("Error checking if object %s/%s exists: %s. Retrying in %.1f seconds... (%d/%d)",
                             bucket, key, e, backoff_delay, attempt + 1, retry_count)
                time.sleep(backoff_delay)
            else:
                LOGGER.error("Error checking if object %s/%s exists after %d retries: %s",
                           bucket, key, retry_count, e)
                return False
                
        except Exception as e:  # pylint: disable=broad-except
            if attempt < retry_count:
                backoff_delay = retry_delay * (2 ** attempt)
                LOGGER.warning("Error checking if object %s/%s exists: %s. Retrying in %.1f seconds... (%d/%d)",
                             bucket, key, e, backoff_delay, attempt + 1, retry_count)
                time.sleep(backoff_delay)
            else:
                LOGGER.error("Error checking if object %s/%s exists after %d retries: %s",
                           bucket, key, retry_count, e)
                return False
    
    return False


def check_prefix_exists(
    client: boto3.client,
    bucket: str,
    prefix: str,
) -> bool:
    """
    Check if a prefix exists in an S3 bucket.

    Args:
        client (boto3.client): S3 client.
        bucket (str): Bucket name.
        prefix (str): Prefix.

    Returns:
        bool: Whether the prefix exists.
    """
    # check if the prefix exists
    try:
        response = client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
        )
        return "Contents" in response
    except Exception as e:  # pylint: disable=broad-except
        LOGGER.error("Error checking prefix: %s", e)
        return False


def list_common_prefixes(
    client: boto3.client,
    bucket: str,
    prefix: str,
) -> list[str]:
    """
    List the common prefixes, i.e., "folders", with a prefix in an S3 bucket.

    Args:
        client (boto3.client): S3 client.
        bucket (str): Bucket name.
        prefix (str): Prefix.

    Returns:
        list[str]: Object keys.
    """
    # get the objects with the prefix
    try:
        response = client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter="/",
        )
        if "CommonPrefixes" in response:
            return [obj["Prefix"] for obj in response["CommonPrefixes"]]
    except Exception as e:  # pylint: disable=broad-except
        LOGGER.error("Error listing prefix: %s", e)
    return []


def iter_prefix(
    client: boto3.client,
    bucket: str,
    prefix: str,
    page_size: int = 1000,
    max_items: Optional[int] = None,
) -> Generator[str, None, None]:
    """
    Iterate over objects with a prefix in an S3 bucket with optimized pagination.

    Args:
        client (boto3.client): S3 client.
        bucket (str): Bucket name.
        prefix (str): Prefix.
        page_size (int): Number of keys to retrieve per request.
        max_items (Optional[int]): Maximum number of items to retrieve.

    Yields:
        str: Object key.
    """
    # get the objects with the prefix
    try:
        list_paginator = client.get_paginator("list_objects_v2")
        pagination_config = {
            "PageSize": page_size,  # Retrieve more keys per request
        }
        
        if max_items:
            pagination_config["MaxItems"] = max_items
            
        list_results = list_paginator.paginate(
            Bucket=bucket, 
            Prefix=prefix,
            PaginationConfig=pagination_config
        )
        
        for results in list_results:
            if "Contents" in results:
                # Process all keys in one batch for better performance
                for obj in results["Contents"]:
                    yield obj["Key"]
    except Exception as e:  # pylint: disable=broad-except
        LOGGER.error("Error listing prefix: %s", e)


def iter_prefix_shard(
    client: boto3.client, 
    bucket: str, 
    prefix: str, 
    shard: str,
    page_size: int = 1000,
    max_items: Optional[int] = None
) -> Generator[str, None, None]:
    """
    Iterate over objects with a prefix in an S3 bucket, filtered by shard.

    Args:
        client (boto3.client): S3 client.
        bucket (str): Bucket name.
        prefix (str): Prefix.
        shard (str): Shard prefix to match.
        page_size (int): Number of keys to retrieve per request.
        max_items (Optional[int]): Maximum number of items to retrieve.

    Yields:
        str: Object key.
    """
    # get the objects with the prefix
    try:
        list_paginator = client.get_paginator("list_objects_v2")
        pagination_config = {
            "PageSize": page_size,  # Retrieve more keys per request
        }
        
        if max_items:
            pagination_config["MaxItems"] = max_items
            
        list_results = list_paginator.paginate(
            Bucket=bucket, 
            Prefix=prefix,
            PaginationConfig=pagination_config
        )
        
        for results in list_results:
            if "Contents" in results:
                # Pre-calculate hashes for the entire batch
                filtered_keys = []
                for obj in results["Contents"]:
                    key_hash = hashlib.blake2b(obj["Key"].encode()).hexdigest().lower()
                    if key_hash.startswith(shard):
                        filtered_keys.append(obj["Key"])
                
                # Yield filtered keys
                for key in filtered_keys:
                    yield key
    except Exception as e:  # pylint: disable=broad-except
        LOGGER.error("Error listing prefix: %s", e)


class AwsAuth(httpx.Auth):
    """AWS Signature Version 4 authentication for httpx."""

    def __init__(self, access_id, secret_key, region, service):
        """
        Initialize the AWS authentication.

        Args:
            access_id (str): AWS access key ID.
            secret_key (str): AWS secret access key.
            region (str): AWS region.
            service (str): AWS service.

        Returns:
            None
        """
        self.access_id = access_id
        self.secret_key = secret_key
        self.region = region
        self.service = service

    def get_datestamp(self) -> tuple[str, str]:
        """
        Get the datestamp.

        Returns:
            str: The datestamp.
        """
        date_value = datetime.datetime.now(tz=datetime.timezone.utc)
        return date_value.strftime("%Y%m%dT%H%M%SZ"), date_value.strftime("%Y%m%d")

    def auth_flow(self, request) -> Generator[httpx.Request, None, None]:
        """
        Authenticate the request.

        Args:
            request (httpx.Request): The request.

        Returns:
            Iterator[httpx.Request]:
        """
        # make sure the path is properly encoded too
        method = request.method.upper()
        host = request.url.host
        path = request.url.path
        if request.url.query:
            path += f"?{request.url.query}"

        # ensure the path is encoded like the request
        path = quote(path, safe="/")

        # get utc datetime strings
        datetime_string, date_string = self.get_datestamp()

        # hash the content
        content_hash = hashlib.sha256(request.content or b"").hexdigest()

        # set up the canonical request
        canonical_headers = f"host:{host}\nx-amz-content-sha256:{content_hash}\nx-amz-date:{datetime_string}\n"
        signed_headers = "host;x-amz-content-sha256;x-amz-date"
        canonical_request = (
            f"{method}\n{path}\n\n{canonical_headers}\n{signed_headers}\n{content_hash}"
        )
        credential_scope = f"{date_string}/{self.region}/{self.service}/aws4_request"
        string_to_sign = f"AWS4-HMAC-SHA256\n{datetime_string}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"

        def sign(key, msg):
            return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

        k_date = sign(("AWS4" + self.secret_key).encode("utf-8"), date_string)
        k_region = sign(k_date, self.region)
        k_service = sign(k_region, self.service)
        k_signing = sign(k_service, "aws4_request")

        signature = hmac.new(
            k_signing, string_to_sign.encode("utf-8"), hashlib.sha256
        ).hexdigest()

        authorization_header = (
            f"AWS4-HMAC-SHA256 Credential={self.access_id}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        )

        request.headers["x-amz-date"] = datetime_string
        request.headers["x-amz-content-sha256"] = content_hash
        request.headers["Authorization"] = authorization_header
        yield request


async def get_object_bytes_async(
    bucket: str,
    key: str,
    aws_region: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    verify_ssl: bool = True,
) -> Optional[bytes]:
    """
    Get an object from an S3 bucket asynchronously using httpx.

    Args:
        bucket (str): Bucket name.
        key (str): Object key.
        region (str): AWS region (default: "us-east-1").
        aws_access_key_id (Optional[str]): AWS access key ID.
        aws_secret_access_key (Optional[str]): AWS secret access key.
        verify_ssl (bool): Whether to verify SSL certificates (default: True).

    Returns:
        Optional[bytes]: Object data or None if an error occurs.
    """
    if aws_access_key_id is None:
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    if aws_secret_access_key is None:
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if aws_region is None:
        aws_region = os.getenv(
            "AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        )

    # Use the correct S3 endpoint format
    encoded_key = quote(key, safe="")
    object_url = f"https://s3.{aws_region}.amazonaws.com/{bucket}/{encoded_key}"

    async with httpx.AsyncClient(verify=verify_ssl) as client:
        try:
            response = await client.get(
                object_url,
                auth=AwsAuth(
                    aws_access_key_id, aws_secret_access_key, aws_region, "s3"
                ),
                timeout=DEFAULT_TIMEOUT,
            )
            response.raise_for_status()
            data = response.content
            LOGGER.info("Got object %s://%s (%d)", bucket, key, len(data))
            return data
        except httpx.HTTPStatusError as e:
            LOGGER.error("HTTP error getting object: %s", e)
            return None
        except Exception as e:  # pylint: disable=broad-except
            LOGGER.error("Error getting object: %s", e)
            return None


def get_stage_prefix(stage: S3Stage, dataset_id: Optional[str] = None) -> str:
    """
    Get the S3 prefix for a specific stage and optional dataset ID.

    Args:
        stage (S3Stage): The storage stage
        dataset_id (Optional[str]): The dataset ID (optional)

    Returns:
        str: The S3 prefix
    """
    if stage == S3Stage.INDEX:
        return f"{stage.value}/"
    elif dataset_id:
        return f"{stage.value}/{dataset_id}/"
    else:
        return f"{stage.value}/"


def convert_key_to_stage(key: str, target_stage: S3Stage) -> str:
    """
    Convert an S3 key from its current stage to the target stage.

    Args:
        key (str): The S3 key to convert
        target_stage (S3Stage): The target storage stage

    Returns:
        str: The converted S3 key
    """
    LOGGER.debug(f"Converting key '{key}' to stage '{target_stage.value}'")
    
    # Split the key into components
    components = key.split("/")
    
    if len(components) < 2:
        raise ValueError(f"Invalid key format: {key}")
    
    # Save the original stage
    original_stage = components[0]
    
    # Replace the first component with the target stage
    components[0] = target_stage.value
    
    # For parquet, we need to handle extension differently
    if target_stage == S3Stage.PARQUET:
        # Always remove the .json extension when converting to parquet
        if key.endswith(".json"):
            result = "/".join(components)[:-len(".json")]
            LOGGER.debug(f"Converted '{key}' from '{original_stage}' to '{target_stage.value}': '{result}' (removed .json)")
            return result
        else:
            # Key doesn't end with .json - e.g., when converting from parquet to parquet
            result = "/".join(components)
            LOGGER.debug(f"Converted '{key}' from '{original_stage}' to '{target_stage.value}': '{result}' (no extension change)")
            return result
    
    # For document and representation stages, we need to ensure proper extension
    elif target_stage in [S3Stage.DOCUMENTS, S3Stage.REPRESENTATIONS]:
        # If converting from parquet (no extension) to documents/representations, add .json
        if original_stage == S3Stage.PARQUET.value and not key.endswith(".json"):
            result = "/".join(components) + ".json"
            LOGGER.debug(f"Converted '{key}' from '{original_stage}' to '{target_stage.value}': '{result}' (added .json)")
            return result
    
    # Default case - just change the stage prefix
    result = "/".join(components)
    LOGGER.debug(f"Converted '{key}' from '{original_stage}' to '{target_stage.value}': '{result}' (stage prefix only)")
    return result


def get_document_key(key: str) -> str:
    """
    Convert any key to a document key.

    Args:
        key (str): The S3 key to convert

    Returns:
        str: The document key
    """
    return convert_key_to_stage(key, S3Stage.DOCUMENTS)


def get_representation_key(key: str) -> str:
    """
    Convert any key to a representation key.

    Args:
        key (str): The S3 key to convert

    Returns:
        str: The representation key
    """
    return convert_key_to_stage(key, S3Stage.REPRESENTATIONS)


def get_parquet_key(key: str) -> str:
    """
    Convert any key to a parquet key (removes .json extension if present).

    Args:
        key (str): The S3 key to convert

    Returns:
        str: The parquet key
    """
    return convert_key_to_stage(key, S3Stage.PARQUET)


def get_index_key(dataset_id: str) -> str:
    """
    Get the index key for a dataset.

    Args:
        dataset_id (str): The dataset ID

    Returns:
        str: The index key
    """
    return f"{S3Stage.INDEX.value}/{dataset_id}.json.gz"


def check_stage_exists(
    client: boto3.client,
    bucket: str,
    key: str,
    stage: S3Stage,
    retry_count: int = 2,
    retry_delay: float = 0.5,
) -> bool:
    """
    Check if an object exists in a specific S3 stage.

    Args:
        client (boto3.client): S3 client
        bucket (str): Bucket name
        key (str): The original key (from any stage)
        stage (S3Stage): The stage to check
        retry_count (int): Number of retries on failure
        retry_delay (float): Delay between retries in seconds

    Returns:
        bool: Whether the object exists in the specified stage
    """
    # Convert the key to the target stage
    stage_key = convert_key_to_stage(key, stage)
    
    # Log the check
    LOGGER.debug(f"Checking if object exists in {stage.value} stage: {stage_key}")
    
    # Check if the object exists
    result = check_object_exists(client, bucket, stage_key, retry_count, retry_delay)
    
    # Log the result
    if result:
        LOGGER.debug(f"Object exists in {stage.value} stage: {stage_key}")
    else:
        LOGGER.debug(f"Object does NOT exist in {stage.value} stage: {stage_key}")
        
    return result


def list_dataset_ids(
    client: boto3.client,
    bucket: str,
    stage: S3Stage = S3Stage.DOCUMENTS,
) -> List[str]:
    """
    List all dataset IDs available in a specific stage.

    Args:
        client (boto3.client): S3 client
        bucket (str): Bucket name
        stage (S3Stage): The stage to list datasets from

    Returns:
        List[str]: List of dataset IDs
    """
    # Get the prefix for the stage
    prefix = get_stage_prefix(stage)
    
    # List common prefixes
    common_prefixes = list_common_prefixes(client, bucket, prefix)
    
    # Extract dataset IDs from prefixes
    dataset_ids = []
    for prefix in common_prefixes:
        # Extract dataset ID from prefix (format: "stage/dataset_id/")
        parts = prefix.rstrip("/").split("/")
        if len(parts) >= 2:
            dataset_ids.append(parts[1])
    
    return dataset_ids
