package org.locationtech.geowave.datastore.redis.operations;

import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.datastore.redis.config.RedisOptions.Compression;
import org.locationtech.geowave.datastore.redis.util.RedisMapWrapper;
import org.locationtech.geowave.datastore.redis.util.RedisUtils;
import org.redisson.api.RedissonClient;

public class RedisDataIndexWriter implements RowWriter {
  private final RedisMapWrapper map;

  public RedisDataIndexWriter(
      final RedissonClient client,
      final Compression compression,
      final String namespace,
      final String typeName,
      final boolean visibilityEnabled) {
    super();
    map = RedisUtils.getDataIndexMap(client, compression, namespace, typeName, visibilityEnabled);
  }

  @Override
  public void write(final GeoWaveRow[] rows) {
    for (final GeoWaveRow row : rows) {
      write(row);
    }
  }

  @Override
  public void write(final GeoWaveRow row) {
    for (final GeoWaveValue value : row.getFieldValues()) {
      // the data ID is mapped to the sort key
      map.add(row.getDataId(), value);
    }
  }

  @Override
  public void flush() {
    map.flush();
  }

  @Override
  public void close() throws Exception {
    map.close();
  }

}
