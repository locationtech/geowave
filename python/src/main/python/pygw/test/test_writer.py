import pytest

from pygw import RocksDbDs, SpatialIndex, config
from pygw.sft_example import Point, PointFeatureDataAdapter, PointBuilder


def test_create_writer():
    # given
    ds = RocksDbDs("geowave.hello", "./world")
    point = Point()
    adapter = PointFeatureDataAdapter(point)
    index = SpatialIndex()
    ds.add_type(adapter, index)

    # when
    writer = ds.create_writer(adapter.get_type_name())

    # then
    assert writer is not None


def test_create_writer_null():
    # given
    ds = RocksDbDs("geowave.hello", "./world")

    # when
    writer = ds.create_writer("Corgi")

    # then
    assert writer is None


def test_create_writer_null_other():
    # given
    ds = RocksDbDs("geowave.hello", "./world")
    point = Point()
    adapter = PointFeatureDataAdapter(point)
    index = SpatialIndex()
    ds.add_type(adapter, index)
    ds.create_writer(adapter.get_type_name())

    # when
    writer = ds.create_writer("Corgi")

    # then
    assert writer is None


def test_write():
    # given
    ds = RocksDbDs("geowave.hello", "./world")
    point = Point()
    builder = PointBuilder(point)
    adapter = PointFeatureDataAdapter(point)
    index = SpatialIndex()
    ds.add_type(adapter, index)
    writer = ds.create_writer(adapter.get_type_name())
    j_data = config.GATEWAY.entry_point.simpleIngest.getGriddedFeatures(builder._java_ref, 1000)

    # when
    results = None

    for data in j_data:
        results = writer._java_ref.write(data)

    writer.close()

    # then
    assert len(results.getWrittenIndexNames()) == 1
