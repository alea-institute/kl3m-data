"""
S3 utilities
"""

# imports
import datetime
import hashlib
import hmac
import os
import time
from pathlib import Path
from typing import Generator, Optional
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


def get_s3_config(
    pool_size: Optional[int] = None,
    connect_timeout: Optional[int] = None,
    read_timeout: Optional[int] = None,
    retry_count: Optional[int] = None,
    region_name: Optional[str] = None,
) -> botocore.config.Config:
    """
    Get an S3 configuration object with the specified parameters.

    Args:
        pool_size (int): Number of connections in the pool.
        connect_timeout (int): Connection timeout in seconds.
        read_timeout (int): Read timeout in seconds.
        retry_count (int): Number of retries.
        region_name (str): AWS region name.

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
    if region_name is not None:
        config.region_name = region_name

    # log all details
    LOGGER.info(
        "S3 configured with region=%s, pool_size=%s, connect_timeout=%s, read_timeout=%s, retry_count=%s",
        region_name,
        pool_size,
        connect_timeout,
        read_timeout,
        retry_count,
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
) -> Generator[str, None, None]:
    """
    Iterate over objects with a prefix in an S3 bucket.

    Args:
        client (boto3.client): S3 client.
        bucket (str): Bucket name.
        prefix (str): Prefix.

    Yields:
        str: Object key.
    """
    # get the objects with the prefix
    try:
        list_paginator = client.get_paginator("list_objects_v2")
        list_results = list_paginator.paginate(Bucket=bucket, Prefix=prefix)
        for results in list_results:
            if "Contents" in results:
                for obj in results["Contents"]:
                    yield obj["Key"]
    except Exception as e:  # pylint: disable=broad-except
        LOGGER.error("Error listing prefix: %s", e)


def iter_prefix_shard(
    client: boto3.client, bucket: str, prefix: str, shard: str
) -> Generator[str, None, None]:
    """
    Iterate over objects with a prefix in an S3 bucket.

    Args:
        client (boto3.client): S3 client.
        bucket (str): Bucket name.
        prefix (str): Prefix.
        shard (str): Shard prefix to mathc.

    Yields:
        str: Object key.
    """
    # get the objects with the prefix
    try:
        list_paginator = client.get_paginator("list_objects_v2")
        list_results = list_paginator.paginate(Bucket=bucket, Prefix=prefix)
        for results in list_results:
            if "Contents" in results:
                for obj in results["Contents"]:
                    key_hash = hashlib.blake2b(obj["Key"].encode()).hexdigest().lower()
                    if key_hash.startswith(shard):
                        yield obj["Key"]
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
