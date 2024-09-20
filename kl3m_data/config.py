"""
Configuration module for the kl3m_data package.
"""

# imports
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

# packages


# pylint: disable=too-many-instance-attributes
@dataclass
class KL3MDataConfig:
    """
    Configuration class for the KL3MData package.
    """

    # dataclass fields with default values
    # dns
    default_dns_timeout: int = 10
    # http(x) configuration
    default_http_timeout: int = 10
    user_agent: str = (
        "kl3m-data/0.1.0 (https://kl3m.ai; https://aleainstitute.ai; "
        "hello@aleainstitute.ai; https://github.com/alea-institute/kl3m-data)"
    )
    default_httpx_limit_keepalive: int = 8
    default_httpx_limit_connections: int = 64
    default_httpx_network_timeout: int = 10
    default_httpx_connect_timeout: int = 10
    default_httpx_read_timeout: int = 10
    default_httpx_write_timeout: int = 10

    # aws/s3 configuration
    default_s3_bucket: str = "data.kl3m.ai"
    default_s3_region: str = "us-east-2"
    default_s3_pool_size: int = 8
    default_s3_connect_timeout: int = 10
    default_s3_read_timeout: int = 10
    default_s3_retry_count: int = 3

    @property
    def aws_access_key(self) -> Optional[str]:
        """
        Get the S3 access key from the environment.

        Returns:
            str: The S3 access key.
        """
        return os.getenv("AWS_ACCESS_KEY_ID", None)

    @property
    def aws_secret_key(self) -> Optional[str]:
        """
        Get the S3 secret key from the environment.

        Returns:
            str: The S3 secret key.
        """
        return os.getenv("AWS_SECRET_ACCESS_KEY", None)

    @property
    def aws_region(self) -> Optional[str]:
        """
        Get the S3 session token from the environment.

        Returns:
            str: The S3 session token.
        """
        return os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", None))

    @staticmethod
    def from_json(file_path: Path):
        """
        Load a KL3MDataConfig object from a JSON file.

        Args:
            file_path (Path): Path to the JSON file.

        Returns:
            KL3MDataConfig: A KL3MDataConfig object.
        """
        with file_path.open("rt", encoding="utf-8") as input_file:
            config_data = json.load(input_file)
            return KL3MDataConfig(**config_data)

    def to_json(self, file_path: Optional[Path] = None) -> Optional[str]:
        """
        Save the KL3MDataConfig object to a JSON file.

        Args:
            file_path (Path): Path to the JSON file.

        Returns:
            Optional[str]: A JSON string.
        """
        if file_path:
            with file_path.open("wt", encoding="utf-8") as output_file:
                json.dump(asdict(self), output_file, indent=4)
                return None
        else:
            return json.dumps(asdict(self), indent=4)


# load the default configuration
CONFIG = KL3MDataConfig.from_json(Path(__file__).parent.parent / "config.json")
