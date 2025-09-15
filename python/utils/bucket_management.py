"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file LICENSE.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""


from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
import boto3
import os
from typing import Callable, Optional
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import BlobProperties
from arcticdb.util.logger import get_logger


logger = get_logger()


def s3_client() -> BaseClient:
    """Create a boto S3 client to Amazon AWS S3 store"""
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("ARCTICDB_REAL_S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("ARCTICDB_REAL_S3_SECRET_KEY"),
    )


def gcp_client() -> BaseClient:
    """Returns a boto client to GCP stoage"""
    session = boto3.session.Session()
    return session.client(
        service_name='s3',
        aws_access_key_id=os.getenv("ARCTICDB_REAL_GCP_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("ARCTICDB_REAL_GCP_SECRET_KEY"),
        endpoint_url=os.getenv("ARCTICDB_REAL_GCP_ENDPOINT")
    )


def azure_client() -> BlobServiceClient:
    """Creates and returns a BlobServiceClient using the provided connection string."""
    connection_string=os.getenv("ARCTICDB_REAL_AZURE_CONNECTION_STRING")
    return BlobServiceClient.from_connection_string(connection_string)


def list_bucket(
    client: BaseClient,
    bucket_name: str,
    handler: Callable[[dict], None],
    cutoff_date: Optional[datetime] = None
) -> None:
    """
    Lists objects in a bucket that were last modified before a given date,
    and applies a handler function to each.

    Parameters:
        client: boto3 S3-compatible client (e.g., for GCS via HMAC).
        bucket_name: Name of the bucket.
        handler : Function to apply to each qualifying object.
        cutoff_date (Optional): Only include objects older than this date.
                                    Defaults to current UTC time.
    """
    if cutoff_date is None:
        cutoff_date = datetime.now(timezone.utc)

    paginator = client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name):
        for obj in page.get('Contents', []):
            if obj['LastModified'] < cutoff_date:
                handler(obj)
            

def delete_gcp_bucket(
    client: BaseClient,
    bucket_name: str,
    cutoff_date: Optional[datetime] = None,
    max_workers: int = 50
) -> None:
    """
    Deletes objects in a GCS bucket that were last modified before a given date,
    using parallel deletion via HMAC credentials.

    Parameters:
        bucket_name (str): Name of the GCS bucket.
        cutoff_date (Optional[datetime]): Only delete objects older than this date.
                                          Defaults to current UTC time.
        max_workers (int): Number of parallel threads for deletion.
    """
    keys_to_delete: list[str] = []

    def collect_key(obj: dict) -> None:
        keys_to_delete.append(obj['Key'])

    list_bucket(client, bucket_name, collect_key, cutoff_date)
    logger.info(f"Found {len(keys_to_delete)} objects to delete before {cutoff_date or datetime.now(timezone.utc)}")

    def delete_key(key: str) -> None:
        client.delete_object(Bucket=bucket_name, Key=key)
        logger.info(f"Deleted: {key}")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(delete_key, keys_to_delete)


def get_gcp_bucket_size(
        client: BaseClient, 
        bucket_name: str,
        cutoff_date: Optional[datetime] = None,
) -> int:
    """Returns the size of specified GCP bucket
    
    Parameters:
        client: boto3 S3-compatible client (e.g., for GCS via HMAC).
        bucket_name: Name of the bucket.
        cutoff_date (Optional): Only include objects older than this date.
                                    Defaults to current UTC time.
    """

    def calc_size_handler():
        total = {'size': 0}  # Mutable container to hold state

        def handler(obj):
            total['size'] += obj['Size']

        return handler, total
    
    handler, total = calc_size_handler()
    list_bucket(client, bucket_name, handler, cutoff_date)
    total_size = total['size']
    return total_size        


def list_azure_container(
    client: BlobServiceClient,
    container_name: str,
    handler: Callable[[BlobProperties], None],
    cutoff_date: Optional[datetime] = None
) -> None:
    """
    Lists blobs in a container that were last modified before a given date,
    and applies a handler function to each.

    Parameters:
        client : Authenticated BlobServiceClient.
        container_name : Name of the container.
        handler : Function to apply to each qualifying blob.
        cutoff_date (Optional[datetime]): Only include blobs older than this date.
                                          Defaults to current UTC time.
    """
    if cutoff_date is None:
        cutoff_date = datetime.now(timezone.utc)

    container_client = client.get_container_client(container_name)
    for blob in container_client.list_blobs():
        if blob.last_modified and blob.last_modified < cutoff_date:
            handler(blob)


def get_azure_container_size(
    blob_service_client: BlobServiceClient,
    container_name: str,
    cutoff_date: Optional[datetime] = None
) -> int:
    """Calculates the total size of all blobs in a container."""
    total_size = 0

    def size_accumulator(blob: BlobProperties) -> None:
        nonlocal total_size
        total_size += blob.size

    list_azure_container(blob_service_client, container_name, size_accumulator, cutoff_date)
    return total_size


def delete_azure_container33(
    blob_service_client: BlobServiceClient,
    container_name: str,
    cutoff_date: Optional[datetime] = None
) -> None:
    """Deletes all blobs in a container."""
    container_client = blob_service_client.get_container_client(container_name)

    def delete_handler(blob: BlobProperties) -> None:
        container_client.delete_blob(blob.name)
        logger.info(f"Deleted: {blob.name}")

    list_azure_container(blob_service_client, container_name, delete_handler, cutoff_date)

def delete_azure_container(
    client: BlobServiceClient,
    container_name: str,
    cutoff_date: Optional[datetime] = None,
    max_workers: int = 20
) -> None:
    """
    Deletes blobs in an Azure container that were last modified before the cutoff date.

    Parameters:
        client : Authenticated BlobServiceClient.
        container_name : Name of the container.
        cutoff_date : Only delete blobs older than this date.
                                          Defaults to current UTC time.
        max_workers : Number of parallel threads for deletion.
    """
    container_client = client.get_container_client(container_name)
    blobs_to_delete: list[str] = []

    def collect_blob(blob: BlobProperties) -> None:
        blobs_to_delete.append(blob.name)

    list_azure_container(client, container_name, collect_blob, cutoff_date)

    logger.info(f"Found {len(blobs_to_delete)} blobs to delete before {cutoff_date or datetime.now(timezone.utc)}")

    def delete_blob(blob_name: str) -> None:
        try:
            container_client.delete_blob(blob_name)
            logger.info(f"Deleted: {blob_name}")
        except Exception as e:
            logger.error(f"Failed to delete {blob_name}: {e}")


    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(delete_blob, blobs_to_delete)


def get_s3_bucket_size(
        client: BaseClient, 
        bucket_name: str,
        cutoff_date: Optional[datetime] = None
) -> int:
    """
    Calculates the total size of all objects in an S3 bucket.

    Parameters:
        client : A boto3 S3 client.
        bucket_name : Name of the S3 bucket.
        cutoff_date : Only delete blobs older than this date.
                                          Defaults to current UTC time.

    Returns:
        int: Total size in bytes.
    """
    total_size = 0

    def size_accumulator(obj: dict) -> None:
        nonlocal total_size
        total_size += obj['Size']

    list_bucket(client, bucket_name, size_accumulator, cutoff_date)
    return total_size


def delete_s3_bucket_batch(
    client: BaseClient,
    bucket_name: str,
    cutoff_date: Optional[datetime] = None,
    batch_size: int = 1000
) -> None:
    """
    Deletes objects in an S3-compatible bucket that were last modified before the cutoff date,
    using batch deletion (up to 1000 objects per request).

    Args:
        client : boto3 S3-compatible client
        bucket_name : Name of the bucket.
        cutoff_date : Only delete objects older than this date.
                                          Defaults to current UTC time.
        batch_size : Maximum number of objects per delete request (max 1000).
    """
    batch: list[dict] = []

    def delete_batch(batch):
        client.delete_objects(Bucket=bucket_name, Delete={'Objects': batch})
        logger.info(f"Deleted batch of {len(batch)} AWS S3 objects")

    def collect_keys(obj: dict) -> None:
        batch.append({'Key': obj['Key']})
        if len(batch) == batch_size:
            try:
                delete_batch(batch)
            except Exception as e:
                logger.error(f"Batch delete failed: {e}")
            batch.clear()

    list_bucket(client, bucket_name, collect_keys, cutoff_date)

    # Delete any remaining objects
    if batch:
        try:
            delete_batch(batch)
        except Exception as e:
            logger.error(f"Final batch delete failed: {e}")


"""
gcp = gcp_client()
gcp_bucket = os.getenv("ARCTICDB_REAL_GCP_BUCKET")
logger.info(f"GCP TOTAL SIZE: {get_gcp_bucket_size(gcp, gcp_bucket)}")
delete_gcp_bucket(gcp, gcp_bucket)

azure = azure_client()
azure_container = os.getenv("ARCTICDB_REAL_AZURE_CONTAINER")
logger.info(f"AZURE TOTAL SIZE: {get_azure_container_size(azure, azure_container)}")
delete_azure_container(azure, azure_container)

s3 = s3_client()
s3_bucket = os.getenv("ARCTICDB_REAL_S3_BUCKET")
cutoff = datetime(2025, 3, 1, tzinfo=timezone.utc)
one_week_later = cutoff + timedelta(days=7)
delete_s3_bucket_batch(s3, s3_bucket)
logger.info(f"AWS S3 TOTAL SIZE: {get_s3_bucket_size(s3, s3_bucket)}")

"""



