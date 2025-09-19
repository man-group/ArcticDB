"""
Copyright 2025 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file LICENSE.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

from datetime import datetime, timedelta, timezone
import os
from arcticdb.util.logger import get_logger
from .bucket_management import (
    azure_client,
    delete_azure_container,
    delete_gcp_bucket,
    delete_s3_bucket_batch,
    gcp_client,
    get_azure_container_size,
    get_gcp_bucket_size,
    get_s3_bucket_size,
    s3_client,
)


logger = get_logger()

now = datetime.now(timezone.utc)
cutoff = now - timedelta(days=28)

logger.info(f"Cleaning before: {cutoff}")

logger.info("Cleaning-up GCP storage")
gcp = gcp_client()
gcp_bucket = os.getenv("ARCTICDB_REAL_GCP_BUCKET")
logger.info(f"Before clean: GCP TOTAL SIZE: {get_gcp_bucket_size(gcp, gcp_bucket)}")
delete_gcp_bucket(gcp, gcp_bucket, cutoff)
logger.info(f"After clean: GCP TOTAL SIZE: {get_gcp_bucket_size(gcp, gcp_bucket)}")

logger.info("Cleaning-up Azure storage")
azure = azure_client()
azure_container = os.getenv("ARCTICDB_REAL_AZURE_CONTAINER")
logger.info(f"Before clean: AZURE TOTAL SIZE: {get_azure_container_size(azure, azure_container)}")
delete_azure_container(azure, azure_container, cutoff)
logger.info(f"After clean: AZURE TOTAL SIZE: {get_azure_container_size(azure, azure_container)}")

logger.info("Cleaning-up S3 storage")
s3 = s3_client()
s3_bucket = os.getenv("ARCTICDB_REAL_S3_BUCKET")
logger.info(f"Before clean: AWS S3 TOTAL SIZE: {get_s3_bucket_size(s3, s3_bucket)}")
delete_s3_bucket_batch(s3, s3_bucket)
logger.info(f"After clean: AWS S3 TOTAL SIZE: {get_s3_bucket_size(s3, s3_bucket)}")
