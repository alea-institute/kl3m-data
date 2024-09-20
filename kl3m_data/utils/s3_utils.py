"""
S3 utilities
"""

# imports
import time
from pathlib import Path
from typing import Optional

# packages
import boto3
import botocore.config

from kl3m_data.config import KL3MDataConfig

# project
from kl3m_data.logger import LOGGER


def get_s3_config(
    pool_size: Optional[int] = None,
    connect_timeout: Optional[int] = None,
    read_timeout: Optional[int] = None,
    retry_count: Optional[int] = None,
) -> botocore.config.Config:
    """
    Get an S3 configuration object with the specified parameters.

    Args:
        pool_size (int): Number of connections in the pool.
        connect_timeout (int): Connection timeout in seconds.
        read_timeout (int): Read timeout in seconds.
        retry_count (int): Number of retries.

    Returns:
        botocore.config.Config: An S3 configuration object.
    """
    # get the default configuration
    config = botocore.config.Config()

    # update the configuration with the specified parameters
    if pool_size is not None:
        config.max_pool_connections = pool_size
    if connect_timeout is not None:
        config.connect_timeout = connect_timeout
    if read_timeout is not None:
        config.read_timeout = read_timeout
    if retry_count is not None:
        config.retries = {
            "max_attempts": retry_count,
            "mode": "standard",
        }

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
) -> bool:
    """
    Put an object into an S3 bucket.

    Args:
        client (boto3.client): S3 client.
        bucket (str): Bucket name.
        key (str): Object key.
        data (str | bytes): Object data.

    Returns:
        None
    """
    # encode the data if it is a string
    if isinstance(data, str):
        data = data.encode("utf-8")

    # put the object into the bucket
    try:
        for _ in range(KL3MDataConfig.default_s3_retry_count):
            try:
                # put the object
                client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=data,
                )
                LOGGER.info("Put object %s/%s (%d)", bucket, key, len(data))
                return True
            except Exception as e:  # pylint: disable=broad-except
                LOGGER.error("Error putting object: %s", e)
                time.sleep(1)
    except Exception as e:  # pylint: disable=broad-except
        LOGGER.error("Error putting object: %s", e)
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
) -> Optional[bytes]:
    """
    Get an object from an S3 bucket.

    Args:
        client (boto3.client): S3 client.
        bucket (str): Bucket name.
        key (str): Object key.

    Returns:
        bytes: Object data.
    """
    # get the object from the bucket
    try:
        response = client.get_object(
            Bucket=bucket,
            Key=key,
        )
        data = response["Body"].read()
        LOGGER.info("Got object %s://%s (%d)", bucket, key, len(data))
        return data
    except Exception as e:  # pylint: disable=broad-except
        LOGGER.error("Error getting object: %s", e)
        return None


def check_object_exists(
    client: boto3.client,
    bucket: str,
    key: str,
) -> bool:
    """
    Check if an object exists in an S3 bucket.

    Args:
        client (boto3.client): S3 client.
        bucket (str): Bucket name.
        key (str): Object key.

    Returns:
        bool: Whether the object exists.
    """
    # check if the object exists
    try:
        client.head_object(
            Bucket=bucket,
            Key=key,
        )
        return True
    except Exception as e:  # pylint: disable=broad-except
        LOGGER.error("Error checking object: %s", e)
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
