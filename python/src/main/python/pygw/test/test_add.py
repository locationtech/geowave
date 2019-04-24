import pytest

from pygw import RocksDbDs, SpatialIndex
from pygw.sft_example import Point, PointFeatureDataAdapter


# TODO: This ain't working
# @pytest.yield_fixture
# def setup_connection():
#     ds = RocksDbDs("geowave.hello", "./world")
#     yield ds
#     # teardown here
#     pass


def test_add_type():
    # given
    ds = RocksDbDs("geowave.hello", "./world")
    point = Point()
    index = SpatialIndex()
    adapter = PointFeatureDataAdapter(point)

    # when
    ds.add_type(adapter, index)

    # then
    assert len(ds.get_indices()) == 1


def test_add_existing_type():
    # given
    ds = RocksDbDs("geowave.hello", "./world")
    point = Point()
    index = SpatialIndex()
    adapter = PointFeatureDataAdapter(point)
    ds.add_type(adapter, index)

    # when
    ds.add_type(adapter, index)

    # then
    assert len(ds.get_indices()) == 1
