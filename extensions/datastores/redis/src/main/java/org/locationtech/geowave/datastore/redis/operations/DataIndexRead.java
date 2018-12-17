package org.locationtech.geowave.datastore.redis.operations;

import java.util.Iterator;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.datastore.redis.config.RedisOptions.Compression;
import org.locationtech.geowave.datastore.redis.util.RedisMapWrapper;
import org.locationtech.geowave.datastore.redis.util.RedisUtils;
import org.redisson.api.RedissonClient;

public class DataIndexRead {
  private final byte[][] dataIds;
  private final short adapterId;
  private final RedisMapWrapper map;

  protected DataIndexRead(
      final RedissonClient client,
      final Compression compression,
      final String namespace,
      final String typeName,
      final short adapterId,
      final byte[][] dataIds,
      final boolean visibilityEnabled) {
    map = RedisUtils.getDataIndexMap(client, compression, namespace, typeName, visibilityEnabled);
    this.adapterId = adapterId;
    this.dataIds = dataIds;
  }

  public Iterator<GeoWaveRow> results() {
    return map.getRows(dataIds, adapterId);
  }
}
