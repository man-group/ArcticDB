"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from arcticdb_ext.storage import (
    StorageOverride,
    S3Override,
    GCPXMLOverride,
    AWSAuthMethod,
    NativeVariantStorage,
    GCPXMLSettings as NativeGCPXMLSettings,
    S3Settings as NativeS3Settings
)
from collections import namedtuple

from arcticdb.adapters.s3_library_adapter import S3LibraryAdapter

PARSED_QUERY = namedtuple("PARSED_QUERY", ["region"])


class GCPXMLLibraryAdapter(S3LibraryAdapter):
    REGEX = r"gcpxml(s)?://(?P<endpoint>.*):(?P<bucket>[-_a-zA-Z0-9.]+)(?P<query>\?.*)?"

    @staticmethod
    def supports_uri(uri: str) -> bool:
        return uri.startswith("gcpxml://") or uri.startswith("gcpxmls://")

    def __repr__(self):
        return "GCPXML(endpoint=%s, bucket=%s)" % (self._endpoint, self._bucket)

    def native_config(self):
        native_s3_settings = NativeS3Settings(AWSAuthMethod.DISABLED, "", False)
        return NativeVariantStorage(NativeGCPXMLSettings(native_s3_settings))

    def get_storage_override(self) -> StorageOverride:
        storage_override = StorageOverride()
        s3_override = self._get_s3_override()
        gcpxml_override = GCPXMLOverride(s3_override)
        storage_override.set_gcpxml_override(gcpxml_override)
        return storage_override

    def get_masking_override(self) -> StorageOverride:
        storage_override = StorageOverride()
        gcpxml_override = GCPXMLOverride(S3Override())
        storage_override.set_gcpxml_override(gcpxml_override)
        return storage_override
