import pytest
import msgpack
from arcticdb._msgpack_compat import packb, padded_packb, unpackb


def test_packb():
    assert packb(None) == msgpack.packb(None, use_bin_type=True, strict_types=True)
    assert packb("a") == msgpack.packb("a", use_bin_type=True, strict_types=True)
    assert packb(b"12345") == msgpack.packb(b"12345", use_bin_type=True, strict_types=True)
    assert packb(1) == msgpack.packb(1, use_bin_type=True, strict_types=True)
    assert packb([1, 2, 3, 4]) == msgpack.packb([1, 2, 3, 4], use_bin_type=True, strict_types=True)


def test_packb_raises_on_tuple():
    with pytest.raises(TypeError):
        packb((1, 2))


def test_padded_packb_small():
    packed, nbytes = padded_packb(1)
    assert len(packed) % 8 == 0
    assert len(packed) >= nbytes
    assert len(packed) - nbytes < 8
    assert packed[:nbytes] == msgpack.packb(1, use_bin_type=True, strict_types=True)


def test_padded_packb_list():
    ints = list(range(10_000_000))
    packed, nbytes = padded_packb(ints)
    assert len(packed) % 8 == 0
    assert len(packed) >= nbytes
    assert len(packed) - nbytes < 8
    assert packed[:nbytes] == msgpack.packb(ints, use_bin_type=True, strict_types=True)


def test_padded_packb_string():
    aas = "A" * 1_000_005  # not divisible by 8
    packed, nbytes = padded_packb(aas)
    assert len(packed) % 8 == 0
    assert len(packed) >= nbytes
    assert len(packed) - nbytes < 8
    assert packed[:nbytes] == msgpack.packb(aas, use_bin_type=True, strict_types=True)


def test_padded_packb_padding():
    # padded_packb behaviour relies on 1 byte for None assumption from msgpack spec
    packed, nbytes = padded_packb(None)
    assert nbytes == 1  # 1 byte of content
    assert packed == b"\xc0\xc0\xc0\xc0\xc0\xc0\xc0\xc0"  # 7 bytes of padding, 8 total


def test_unpackb():
    # serializes without strict_types
    packed = msgpack.packb({(1, 2): "a"})
    with pytest.raises(TypeError):
        assert unpackb(packed)
