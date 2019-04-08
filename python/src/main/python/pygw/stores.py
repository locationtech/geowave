from pygw.config import config
from pygw.base_models import DataStore


"""
stores.py
====================================
Data stores
"""

class RocksDbDs(DataStore):
    """RocksDB datastore.

    Args:
        gw_namespace (string): namespace of geowave
        dir (str): directory of datastore

    """
    def __init__(self, gw_namespace=None, dir="rocksdb", compact_on_write=True, batch_write_size=1000):
        if gw_namespace:
            j_rock_opts = config.MODULE__rocksdb_config.RocksDBOptions(gw_namespace)
        else:
            j_rock_opts = config.MODULE__rocksdb_config.RocksDBOptions()
        if dir != "rocksdb":
            j_rock_opts.setDirectory(dir)
        if not compact_on_write:
            j_rock_opts.setCompactOnWrite(compact_on_write)
        if batch_write_size != 1000:
            j_rock_opts.setBatchWriteSize(batch_write_size)
        j_ds = config.MODULE__core_store.DataStoreFactory.createDataStore(j_rock_opts)
        super().__init__(config.GATEWAY, j_ds)

class HBaseDs(DataStore):
    """HBaseDs datastore.

    Args:
        zookeeperh (string): zoopkeer for hbase
        hbase_namespace (str): namespace of hbase
    """
    def __init__(self, zookeeperh="example", hbase_namespace=None):
        if hbase_namespace:
            j_hbase_opts = config.MODULE__hbase_config.HBaseOptions(hbase_namespace)
        else:
            j_hbase_opts = config.MODULE__hbase_config.HBaseOptions()
        j_hbase_opts.setZookeeper(zookeeper)
        j_ds = config.MODULE__core_store.DataStoreFactory.createDataStore(j_hbase_opts)
        super().__init__(config.GATEWAY, j_ds)

class RedisDs(DataStore):
    """GeoWave RedisDB datastore.

    Args:
        gw_namespace (str): namespace of hbase
        compression (string): compression algorithm
    """
    __compression_opts = ["snappy", "lz4", "none"]
    __compression_to_j_enum = {
        # Key: [compression_type : string] => Value: [function : () -> Java Ref to RedisOptions.Compression Enum]
        "snappy": lambda : config.MODULE__redis_config.RedisOptions.Compression.SNAPPY,
        "lz4": lambda : config.MODULE__redis_config.RedisOptions.Compression.LZ4,
        "none": lambda : config.MODULE__redis_config.RedisOptions.Compression.NONE,
    }

    def  __init__(self, address, gw_namespace=None, compression="snappy"):
        if compression not in RedisDs.__compression_opts:
            raise RuntimeError("`compression` must be one of {}".format(RedisDs.__compression_opts))
        if gw_namespace:
            j_redis_opts = config.MODULE__redis_config.RedisOptions(gw_namespace)
        else:
            j_redis_opts = config.MODULE__redis_config.RedisOptions()
        j_redis_opts.setAddress(address)
        j_compression = RedisDs.__compression_to_j_enum[compression]()
        j_redis_opts.setCompression(j_compression)
        j_ds = config.MODULE__core_store.DataStoreFactory.createDataStore(j_redis_opts)
        super().__init__(config.GATEWAY, j_ds)