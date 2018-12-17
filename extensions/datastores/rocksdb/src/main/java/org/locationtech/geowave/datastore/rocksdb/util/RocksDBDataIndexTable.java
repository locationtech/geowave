package org.locationtech.geowave.datastore.rocksdb.util;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDBDataIndexTable extends AbstractRocksDBTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBDataIndexTable.class);

  public RocksDBDataIndexTable(
      final Options writeOptions,
      final Options readOptions,
      final String subDirectory,
      final short adapterId,
      final boolean visibilityEnabled) {
    super(writeOptions, readOptions, subDirectory, adapterId, visibilityEnabled);
  }

  public synchronized void add(final byte[] dataId, final GeoWaveValue value) {
    put(dataId, DataIndexUtils.serializeDataIndexValue(value, visibilityEnabled));
  }

  public synchronized CloseableIterator<GeoWaveRow> dataIndexIterator(final byte[][] dataIds) {
    final RocksDB readDb = getReadDb();
    if (readDb == null) {
      return new CloseableIterator.Empty<>();
    }
    try {
      final List<byte[]> dataIdsList = Arrays.asList(dataIds);
      final Map<byte[], byte[]> dataIdxResults = readDb.multiGet(dataIdsList);
      return new CloseableIterator.Wrapper(
          dataIdsList.stream().map(
              dataId -> DataIndexUtils.deserializeDataIndexRow(
                  dataId,
                  adapterId,
                  dataIdxResults.get(dataId),
                  visibilityEnabled)).iterator());
    } catch (final RocksDBException e) {
      LOGGER.error("Unable to get values by data ID", e);
    }
    return new CloseableIterator.Empty<>();
  }
}
