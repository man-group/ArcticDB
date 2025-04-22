from arcticdb.version_store._common import TimeFrame
from arcticdb.version_store._store import NativeVersionStore, VersionedItem

import logging
import os

logger = logging.getLogger(__name__)

# Since aws-sdk-cpp >= 1.11.486, it has turned on checksum integrity check `x-amz-checksum-mode: enabled`
# This feature is not supported in many s3 implementation and causes error
if os.getenv("AWS_RESPONSE_CHECKSUM_VALIDATION") == "when_supported" or os.getenv("AWS_REQUEST_CHECKSUM_CALCULATION") == "when_supported":
    logger.warning(
        "Checksum validation has been specfically enabled by user, which the endpoint may not support and causes error."
    )
else:
    os.environ["AWS_RESPONSE_CHECKSUM_VALIDATION"] = "when_required"
    os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"] = "when_required"