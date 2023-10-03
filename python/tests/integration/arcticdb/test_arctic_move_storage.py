import shutil
import boto3
import pytest
import sys

from arcticdb import Arctic
from arcticdb.util.test import get_wide_dataframe
from arcticdb.util.test import assert_frame_equal
from arcticdb.exceptions import InternalException


def test_move_lmdb_library(tmpdir_factory):
    # Given - any LMDB library
    original = tmpdir_factory.mktemp("original")
    ac = Arctic(f"lmdb://{original}")
    ac.create_library("lib")
    lib = ac["lib"]

    df = get_wide_dataframe(size=100)
    lib.write("sym", df)

    # Free resources to release the filesystem lock held by Windows
    del lib
    del ac

    # When - we move the data
    dest = str(tmpdir_factory.mktemp("dest"))
    shutil.move(str(original / "_arctic_cfg"), dest)
    shutil.move(str(original / "lib"), dest)

    # Then - should be readable at new location
    ac = Arctic(f"lmdb://{dest}")
    assert ac.list_libraries() == ["lib"]
    lib = ac["lib"]
    assert lib.list_symbols() == ["sym"]
    assert_frame_equal(df, lib.read("sym").data)
    assert "dest" in lib.read("sym").host
    assert "original" not in lib.read("sym").host


def test_move_lmdb_library_map_size_reduction(tmpdir_factory):
    # Given - any LMDB library
    original = tmpdir_factory.mktemp("original")
    ac = Arctic(f"lmdb://{original}?map_size=1MB")
    ac.create_library("lib")
    lib = ac["lib"]

    df = get_wide_dataframe(size=100)
    lib.write("sym", df)

    # Free resources to release the filesystem lock held by Windows
    del lib
    del ac

    # When - we move the data
    dest = str(tmpdir_factory.mktemp("dest"))
    shutil.move(str(original / "_arctic_cfg"), dest)
    shutil.move(str(original / "lib"), dest)

    # Then - should be readable at new location as long as map size still big enough
    ac = Arctic(f"lmdb://{dest}?map_size=500KB")
    assert ac.list_libraries() == ["lib"]
    lib = ac["lib"]
    assert lib.list_symbols() == ["sym"]
    assert_frame_equal(df, lib.read("sym").data)
    assert "dest" in lib.read("sym").host
    assert "original" not in lib.read("sym").host
    lib.write("another_sym", df)

    del lib
    del ac

    # Then - read with new tiny map size
    # Current data should still be readable, expect modifications to fail
    ac = Arctic(f"lmdb://{dest}?map_size=1KB")
    assert ac.list_libraries() == ["lib"]
    lib = ac["lib"]
    assert set(lib.list_symbols()) == {"sym", "another_sym"}
    assert_frame_equal(df, lib.read("sym").data)

    # TODO #866 proper exception type for this
    with pytest.raises(InternalException) as exc_info:
        lib.write("another_sym", df)

    assert "lmdb error code -30792" in str(exc_info.value)

    # stuff should still be readable despite the error
    assert_frame_equal(df, lib.read("sym").data)


def test_move_lmdb_library_map_size_increase(tmpdir_factory):
    # Given - any LMDB library
    original = tmpdir_factory.mktemp("original")
    ac = Arctic(f"lmdb://{original}?map_size=500KB")
    ac.create_library("lib")
    lib = ac["lib"]

    for i in range(10):
        df = get_wide_dataframe(size=100)
        lib.write(f"sym_{i}", df)

    # Free resources to release the filesystem lock held by Windows
    del lib
    del ac

    # When - we move the data
    dest = str(tmpdir_factory.mktemp("dest"))
    shutil.move(str(original / "_arctic_cfg"), dest)
    shutil.move(str(original / "lib"), dest)

    # Then - should be readable at new location as long as map size made big enough
    # 20 writes of this size would fail with the old 500KB map size
    ac = Arctic(f"lmdb://{dest}?map_size=10MB")
    lib = ac["lib"]
    for i in range(20):
        df = get_wide_dataframe(size=100)
        lib.write(f"more_sym_{i}", df)
    assert len(lib.list_symbols()) == 30


def test_move_s3_library(moto_s3_endpoint_and_credentials):
    # Given - any S3 library
    endpoint, port, bucket, aws_access_key, aws_secret_key = moto_s3_endpoint_and_credentials

    original_uri = (
        endpoint.replace("http://", "s3://").rsplit(":", 1)[0]
        + ":"
        + bucket
        + "?access="
        + aws_access_key
        + "&secret="
        + aws_secret_key
        + "&port="
        + port
    )

    ac = Arctic(original_uri)
    ac.create_library("lib")
    lib = ac["lib"]

    df = get_wide_dataframe(size=100)
    lib.write("sym", df)

    # When - we move the data
    client = boto3.client(
        service_name="s3", endpoint_url=endpoint, aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key
    )
    new_bucket = f"{bucket}-dest"
    client.create_bucket(Bucket=new_bucket)

    s3 = boto3.resource(
        service_name="s3", endpoint_url=endpoint, aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key
    )
    source = s3.Bucket(bucket)
    dest = s3.Bucket(new_bucket)

    for obj in source.objects.filter():
        dest.copy({"Bucket": bucket, "Key": obj.key}, obj.key)

    # Then - should be readable at new location
    new_uri = original_uri.replace(f":{bucket}", f":{new_bucket}")
    new_ac = Arctic(new_uri)
    assert new_ac.list_libraries() == ["lib"]
    lib = new_ac["lib"]
    assert lib.list_symbols() == ["sym"]
    assert_frame_equal(df, lib.read("sym").data)
    assert "dest" in lib.read("sym").host


@pytest.mark.skipif(sys.platform == "darwin", reason="Test broken on MacOS (issue #909)")
def test_move_azure_library(azure_client_and_create_container, azurite_azure_uri, azurite_container):
    # Given - any Azure library
    original_uri = azurite_azure_uri
    ac = Arctic(original_uri)
    ac.create_library("lib")
    lib = ac["lib"]

    df = get_wide_dataframe(size=100)
    lib.write("sym", df)

    # When - we move the data
    new_container = f"{azurite_container}-new"
    client = azure_client_and_create_container
    client.create_container(name=new_container)

    container_client = client.get_container_client(container=azurite_container)
    new_container_client = client.get_container_client(container=new_container)

    for b in container_client.list_blobs():
        source = container_client.get_blob_client(b.name)
        target = new_container_client.get_blob_client(b.name)
        target.start_copy_from_url(source.url, requires_sync=True)
        props = target.get_blob_properties()
        assert props.copy.status == "success"

    # Then - should be readable at new location
    new_uri = original_uri.replace(f"Container={azurite_container};", f"Container={azurite_container}-new;")
    new_ac = Arctic(new_uri)
    assert new_ac.list_libraries() == ["lib"]
    lib = new_ac["lib"]
    assert lib.list_symbols() == ["sym"]
    assert_frame_equal(df, lib.read("sym").data)
    assert "-new" in lib.read("sym").host
