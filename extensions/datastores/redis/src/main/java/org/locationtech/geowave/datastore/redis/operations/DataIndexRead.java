/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.redis.operations;

import java.util.Iterator;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.datastore.redis.config.RedisOptions.Compression;
import org.locationtech.geowave.datastore.redis.config.RedisOptions.Serialization;
import org.locationtech.geowave.datastore.redis.util.RedisMapWrapper;
import org.locationtech.geowave.datastore.redis.util.RedisUtils;
import org.redisson.api.RedissonClient;

public class DataIndexRead {
  private final byte[][] dataIds;
  private final short adapterId;
  private final RedisMapWrapper map;

  protected DataIndexRead(
      final RedissonClient client,
      final Serialization serialization,
      final Compression compression,
      final String namespace,
      final String typeName,
      final short adapterId,
      final byte[][] dataIds,
      final boolean visibilityEnabled) {
    map =
        RedisUtils.getDataIndexMap(
            client,
            serialization,
            compression,
            namespace,
            typeName,
            visibilityEnabled);
    this.adapterId = adapterId;
    this.dataIds = dataIds;
  }

  public Iterator<GeoWaveRow> results() {
    return map.getRows(dataIds, adapterId);
  }
}
