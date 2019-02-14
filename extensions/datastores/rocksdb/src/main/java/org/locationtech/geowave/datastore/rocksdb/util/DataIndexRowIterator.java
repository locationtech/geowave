package org.locationtech.geowave.datastore.rocksdb.util;

import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;

public class DataIndexRowIterator extends AbstractRocksDBIterator<GeoWaveRow> {
  private final short adapterId;
  private final boolean visibilityEnabled;

  public DataIndexRowIterator(
      final ReadOptions options,
      final RocksIterator it,
      final short adapterId,
      final boolean visiblityEnabled) {
    super(options, it);
    this.adapterId = adapterId;
    visibilityEnabled = visiblityEnabled;
  }

  @Override
  protected GeoWaveRow readRow(final byte[] key, final byte[] value) {
    return DataIndexUtils.deserializeDataIndexRow(key, adapterId, value, visibilityEnabled);
  }
}
