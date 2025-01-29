"""
Copyright 2023 Man Group Operations Limited

Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.

As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""

import logging
import sys
import time
import psutil
import pytz
import math
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List
from enum import Enum

from arcticdb_ext import get_config_int
from arcticdb_ext.exceptions import InternalException, SortingException, UserInputException
from arcticdb_ext.storage import NoDataFoundException
from arcticdb.exceptions import ArcticDbNotYetImplemented
from arcticdb.adapters.mongo_library_adapter import MongoLibraryAdapter
from arcticdb.arctic import Arctic
from arcticdb.options import LibraryOptions
from arcticdb import QueryBuilder
from arcticdb.storage_fixtures.api import StorageFixture, ArcticUriFields, StorageFixtureFactory
from arcticdb.storage_fixtures.mongo import MongoDatabase
from arcticdb.util.test import assert_frame_equal
from arcticdb.storage_fixtures.s3 import S3Bucket
from arcticdb.version_store.library import (
    WritePayload,
    ArcticUnsupportedDataTypeException,
    ReadRequest,
    StagedDataFinalizeMethod,
)

from ...util.mark import AZURE_TESTS_MARK, MONGO_TESTS_MARK, REAL_S3_TESTS_MARK, SLOW_TESTS_MARK, SSL_TESTS_MARK, SSL_TEST_SUPPORTED

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class ParameterDisplayStatus(Enum):
    NOT_SHOW = 1
    DISABLE = 2
    ENABLE = 3

parameter_display_status = [ParameterDisplayStatus.NOT_SHOW, ParameterDisplayStatus.DISABLE, ParameterDisplayStatus.ENABLE]
no_ssl_parameter_display_status = [ParameterDisplayStatus.NOT_SHOW, ParameterDisplayStatus.DISABLE]

class DefaultSetting:
    def __init__(self, factory):
        self.cafile = factory.client_cert_file
        self.capath = factory.client_cert_dir

def edit_connection_string(uri, delimiter, storage, ssl_setting, client_cert_file, client_cert_dir):
    # Clear default setting in the uri
    if SSL_TEST_SUPPORTED:
        uri = storage.replace_uri_field(uri, ArcticUriFields.CA_PATH, "", start=1, end=3).rstrip(delimiter)
        if isinstance(storage, S3Bucket) and "&ssl=" in uri:
            uri = storage.replace_uri_field(uri, ArcticUriFields.SSL, "", start=1, end=3).rstrip(delimiter)
    # http server with ssl verification doesn't make sense but it is permitted due to historical reason
    if ssl_setting == ParameterDisplayStatus.DISABLE:
        uri += f"{delimiter}ssl=False"
    elif ssl_setting == ParameterDisplayStatus.ENABLE:
        uri += f"{delimiter}ssl=True"
    if client_cert_file == ParameterDisplayStatus.DISABLE:
        uri += f"{delimiter}CA_cert_path="
    elif client_cert_file == ParameterDisplayStatus.ENABLE:
        assert storage.factory.client_cert_file
        uri += f"{delimiter}CA_cert_path={storage.factory.client_cert_file}"
    if client_cert_dir == ParameterDisplayStatus.DISABLE:
        uri += f"{delimiter}CA_cert_dir="
    elif client_cert_dir == ParameterDisplayStatus.ENABLE:
        assert storage.factory.client_cert_dir
        uri += f"{delimiter}CA_cert_dir={storage.factory.client_cert_dir}"
    return uri


@REAL_S3_TESTS_MARK
def test_s3_sts_auth(lib_name, real_s3_sts_storage):
    import os
    logger.info(f"AWS_CONFIG_FILE: {os.getenv('AWS_CONFIG_FILE', None)}")
    logger.info(f"USERPROFILE: {os.getenv('USERPROFILE', None)}")
    library = lib_name
    logger.info(f"Library to create: {library}")
    ac = Arctic(real_s3_sts_storage.arctic_uri)
    ac.delete_library(library) # make sure we delete any previously existing library
    lib = ac.create_library(library)
    df = pd.DataFrame({'a': [1, 2, 3]})
    lib.write("sym", df)
    assert_frame_equal(lib.read("sym").data, df)
    lib = ac.get_library(library)
    assert_frame_equal(lib.read("sym").data, df)

    # Reload for testing a different codepath
    ac = Arctic(real_s3_sts_storage.arctic_uri)
    lib = ac.get_library(library)
    assert_frame_equal(lib.read("sym").data, df)
    ac.delete_library(library)


@SLOW_TESTS_MARK
@REAL_S3_TESTS_MARK
def test_s3_sts_expiry_check(lib_name, real_s3_sts_storage):
    """
    The test will obtain token at minimum expiration time of 15 minutes.
    Then will loop reading content of a symbol for 15+3 minuets minutes. If the 
    test reaches final lines then it would effectively mean that the token
    has been renewed.
    """
    symbol = "sym"
    library = lib_name
    logger.info(f"Library to create: {library}")
    # Precondition check
    min_exp_time_min = 15 # This is minimum expiry time, set at fixture level
    value = get_config_int("S3Storage.STSTokenExpiryMin")
    logger.info(f"S3Storage.STSTokenExpiryMin = {value}")
    logger.info(f"Current process id = {psutil.Process()}")
    logger.info(f"Minimum possible is {min_exp_time_min} minutes. Test will fail if bigger")
    assert min_exp_time_min >= value 

    ac = Arctic(real_s3_sts_storage.arctic_uri)
    ac.delete_library(library) # make sure we delete any previously existing library
    lib = ac.create_library(library)
    df = pd.DataFrame({'a': [1, 2, 3]})
    lib.write(symbol, df)

    now = datetime.now()
    complete_at = now + timedelta(minutes=min_exp_time_min+5)
    logger.info(f"Test will complete at {complete_at}")

    data: pd.DataFrame = lib.read(symbol).data
    assert_frame_equal(df, data)
    while (datetime.now() < complete_at):
        data: pd.DataFrame = lib.read(symbol).data
        assert_frame_equal(df, data)
        logger.info(f"sleeping 15 sec")
        time.sleep(15)
        logger.info(f"Time remaining: {complete_at - datetime.now()}")
        logger.info(f"Should complete at: {complete_at}")

    data: pd.DataFrame = lib.read(symbol).data
    assert_frame_equal(df, data)
    logger.info("Connection did not expire")
    logger.info(f"Library to remove: {library}")
    ac.delete_library(library) 

@REAL_S3_TESTS_MARK
def test_s3_sts_auth_store(real_s3_sts_version_store):
    lib = real_s3_sts_version_store
    df = pd.DataFrame({'a': [1, 2, 3]})
    lib.write("sym", df)
    assert_frame_equal(lib.read("sym").data, df)