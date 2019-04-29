import pytest

from pygw import RocksDbDs, SpatialIndex, config
from pygw.sft_example import Point, PointFeatureDataAdapter, PointBuilder


# TODO: This ain't working
# @pytest.yield_fixture
# def setup_connection():
#     ds = RocksDbDs("geowave.hello", "./world")
#     yield ds
#     # teardown here
#     pass

# Test Additions #
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


# Test Removing #
def test_remove_index():
    # given
    ds = RocksDbDs("geowave.hello", "./world")
    point = Point()
    index = SpatialIndex()
    index2 = SpatialIndex()

    adapter = PointFeatureDataAdapter(point)
    ds.add_type(adapter, index)
    ds.add_type(adapter, index2)

    # when
    ds.remove_index(index.get_name(), adapter)

    # then
    assert len(ds.get_indices()) == 1


# TODO: this test needs a different exception and different assert message
def test_remove_index_last():
    with pytest.raises(Exception) as exec:
        # given
        ds = RocksDbDs("geowave.hello", "./world")
        point = Point()
        index = SpatialIndex()
        adapter = PointFeatureDataAdapter(point)
        ds.add_type(adapter, index)

        # when
        ds.remove_index(index.get_name(), adapter)

    # then
    assert 'some error message' in str(exec.value)


def test_remove_index_non_exist():
    # given
    ds = RocksDbDs("geowave.hello", "./world")
    point = Point()
    index = SpatialIndex()
    adapter = PointFeatureDataAdapter(point)
    ds.add_type(adapter, index)

    # when
    ds.remove_index("Corgi", adapter)

    # then
    assert len(ds.get_indices()) == 1


def test_remove_type():
    # given
    ds = RocksDbDs("geowave.hello", "./world")
    point = Point()
    index = SpatialIndex()
    adapter = PointFeatureDataAdapter(point)
    ds.add_type(adapter, index)

    # when
    ds.remove_type(adapter.get_type_name())

    # then
    assert len(ds.get_indices(adapter.get_type_name())) == 0


# Test Deleting #
# TODO: delete this with querying
def test_delete():
    pass


# TODO: make this a better assert than a carbon copy of test_remove_type
def test_delete_all():
    # given
    ds = RocksDbDs("geowave.hello", "./world")
    point = Point()
    index = SpatialIndex()
    adapter = PointFeatureDataAdapter(point)
    ds.add_type(adapter, index)

    # when
    ds.delete_all()

    # then
    assert len(ds.get_indices()) == 0


# Test Copy #
def test_copy():
    # given
    ds1 = RocksDbDs("geowave.hello", "./world")
    ds2 = RocksDbDs("geowave.hello", "./santa")

    point = Point()
    adapter = PointFeatureDataAdapter(point)
    index = SpatialIndex()
    ds1.add_type(adapter, index)

    # when
    ds1.copy_to(ds2)

    # then
    assert len(ds1.get_indices()) == 1
    assert len(ds2.get_indices()) == 1


# TODO: test copy with query added

# Test Writer #
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
