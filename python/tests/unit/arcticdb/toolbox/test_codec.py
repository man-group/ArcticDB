"""
Copyright 2023 Man Group Operations Limited
Use of this software is governed by the Business Source License 1.1 included in the file licenses/BSL.txt.
As of the Change Date specified in that file, in accordance with the Business Source License, use of this software will be governed by the Apache License, version 2.0.
"""
from arcticdb_ext.types import TypeDescriptor, Dimension, DataType
from arcticdb_ext.codec import DynamicFieldBuffer, encode_field, decode_field
from arcticc.pb2.encoding_pb2 import VariantCodec
import numpy as np

from numpy.testing import assert_equal
import pytest

from arcticdb.supported_types import sint_types, uint_types, float_types, numeric_types, int_types, Shape, get_data_type


def shapes(*array):
    return np.array(array, dtype=Shape)


def test_passthrough_enc_dim0():
    td = TypeDescriptor(DataType.INT32, Dimension.Dim0)
    s = shapes(10)
    a = np.arange(10, dtype=np.int32)

    f = DynamicFieldBuffer(td, a, s)
    codec = VariantCodec()
    res = encode_field(codec, f)

    # no shape when Dim0
    a_enc = np.array(res.buffer).view(np.uint32)
    assert_equal(a, a_enc)

    res_dec = decode_field(td, res.encoded_field, np.array(res.buffer))
    assert_equal(a, np.array(res_dec.values_buffer).view(np.uint32))


def test_passthrough_enc_dim1():
    td = TypeDescriptor(DataType.INT32, Dimension.Dim1)
    s = shapes(5, 5)
    a = np.arange(10, dtype=np.int32)

    f = DynamicFieldBuffer(td, a, s)
    codec = VariantCodec()
    res = encode_field(codec, f)

    p = Shape().itemsize * len(s)
    s_enc = np.array(res.buffer)[:p].view(Shape)
    assert_equal(s, s_enc)

    a_enc = np.array(res.buffer)[p:].view(np.uint32)
    assert_equal(a, a_enc)

    res_dec = decode_field(td, res.encoded_field, np.array(res.buffer))
    assert_equal(s, np.array(res_dec.shape_buffer).view(np.uint64))
    assert_equal(a, np.array(res_dec.values_buffer).view(np.uint32))


def test_passthrough_enc_dim2():
    td = TypeDescriptor(DataType.INT32, Dimension.Dim2)
    s = np.array([5, 5] * 4, dtype=Shape)
    a = np.arange(100, dtype=np.int32)

    f = DynamicFieldBuffer(td, a, s)
    codec = VariantCodec()
    res = encode_field(codec, f)

    p = Shape().itemsize * len(s)
    s_enc = np.array(res.buffer)[:p].view(Shape)
    assert_equal(s, s_enc)

    a_enc = np.array(res.buffer)[p:].view(np.uint32)
    assert_equal(a, a_enc)

    res_dec = decode_field(td, res.encoded_field, np.array(res.buffer))
    assert_equal(s, np.array(res_dec.shape_buffer).view(np.uint64))
    assert_equal(a, np.array(res_dec.values_buffer).view(np.uint32))


def gen_codecs():
    codecs = list()
    codecs.append(VariantCodec())

    vcm = VariantCodec()
    vcm.zstd.level = 0
    codecs.append(vcm)

    vcm = VariantCodec()
    vcm.lz4.acceleration = 0
    codecs.append(vcm)

    # vcm = VariantCodec()
    # vcm.passthrough.mark = True
    # codecs.append(vcm)

    return codecs


codecs = gen_codecs()


def gen_param_dim0(size):
    ans = []
    for c in codecs:
        for t in numeric_types:
            data_type = get_data_type(t)
            s = shapes(size)
            a = np.arange(size, dtype=t)
            ans.append((c, data_type, a, s))
    return ans


@pytest.mark.parametrize(("codec", "data_type", "array", "shape"), gen_param_dim0(10))
def test_round_trip_dim0(codec, data_type, array, shape):
    # type: (VariantCodec, np.array[Any], np.array[np.uint64])->None
    td = TypeDescriptor(data_type, Dimension.Dim0)
    f = DynamicFieldBuffer(td, array, shape)
    res = encode_field(codec, f)

    res_dec = decode_field(td, res.encoded_field, np.array(res.buffer))
    assert_equal(array, np.array(res_dec.values_buffer).view(array.dtype))


def gen_param_dim1(size):
    ans = []
    for c in codecs:
        for t in numeric_types:
            data_type = get_data_type(t)
            s = shapes(size // 2, size // 2)
            a = np.arange(size, dtype=t)
            ans.append((c, data_type, a, s))
    return ans


@pytest.mark.parametrize(("codec", "data_type", "array", "shape"), gen_param_dim1(10))
def test_round_trip_dim1(codec, data_type, array, shape):
    # type: (VariantCodec, np.array[Any], np.array[np.uint64])->None
    td = TypeDescriptor(data_type, Dimension.Dim1)
    f = DynamicFieldBuffer(td, array, shape)
    res = encode_field(codec, f)

    res_dec = decode_field(td, res.encoded_field, np.array(res.buffer))
    assert_equal(array, np.array(res_dec.values_buffer).view(array.dtype))
    assert_equal(shape, np.array(res_dec.shape_buffer).view(np.uint64))


def gen_param_dim2():
    ans = []
    for c in codecs:
        for t in numeric_types:
            data_type = get_data_type(t)
            s = np.array([5, 5] * 4, dtype=Shape)
            a = np.arange(100, dtype=t)
            ans.append((c, data_type, a, s))
    return ans


@pytest.mark.parametrize(("codec", "data_type", "array", "shape"), gen_param_dim2())
def test_round_trip_dim2(codec, data_type, array, shape):
    # type: (VariantCodec, np.array[Any], np.array[np.uint64])->None
    td = TypeDescriptor(data_type, Dimension.Dim2)
    f = DynamicFieldBuffer(td, array, shape)
    res = encode_field(codec, f)

    res_dec = decode_field(td, res.encoded_field, np.array(res.buffer))
    assert_equal(array, np.array(res_dec.values_buffer).view(array.dtype))
    assert_equal(shape, np.array(res_dec.shape_buffer).view(np.uint64))
