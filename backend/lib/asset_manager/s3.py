import asyncio
import os
from typing import Any, Optional

import boto3
from botocore.config import Config
from mypy_boto3_s3 import S3Client

from backend.lib.utils import none_throws

from .base import AssetManager


class S3AssetManager(AssetManager):
    def __init__(
        self,
        bucket_name: Optional[str] = None,
        region_name: Optional[str] = None,
    ):
        """Initializes the S3 client and target bucket.

        Args:
            bucket_name: Target S3 bucket.
            region_name: Optional AWS region (default uses env config).

        """
        self.bucket_name = bucket_name or none_throws(
            os.getenv("AWS_S3_DEFAULT_BUCKET_NAME"),
        )
        self.region_name = region_name or none_throws(
            os.getenv("AWS_S3_DEFAULT_BUCKET_REGION"),
        )
        self.s3: S3Client = boto3.client(  # pyright: ignore[reportUnknownMemberType]
            "s3",
            region_name=self.region_name,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

    async def upload_file(
        self,
        file_path: str,
        key: str,
        public: bool = False,
        content_type: Optional[str] = None,
    ) -> None:
        """Uploads a file from local disk to the configured S3 bucket asynchronously.

        Args:
            file_path: Local path to the file.
            key: The S3 object key.
            public: Whether to make the file publicly accessible.
            content_type: Optional MIME type.

        """
        extra_args: dict[str, Any] = {}
        if content_type:
            extra_args["ContentType"] = content_type
        if public:
            extra_args["ACL"] = "public-read"

        await asyncio.to_thread(
            self.s3.upload_file,
            Filename=file_path,
            Bucket=self.bucket_name,
            Key=key,
            ExtraArgs=extra_args,
        )

    async def generate_signed_url(self, key: str, expires_in: int = 3600) -> str:
        """Asynchronously generate a signed URL for an S3 object.

        Args:
            key: S3 object key (e.g., "uploads/uuid.png").
            expires_in: Time in seconds before the URL expires.

        Returns:
            A signed URL as a string.

        """
        return await asyncio.to_thread(
            self.s3.generate_presigned_url,
            ClientMethod="get_object",
            Params={"Bucket": self.bucket_name, "Key": key},
            ExpiresIn=expires_in,
        )
