/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.redis.operations;

import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.datastore.redis.config.RedisOptions.Compression;
import org.locationtech.geowave.datastore.redis.config.RedisOptions.Serialization;
import org.locationtech.geowave.datastore.redis.util.RedisMapWrapper;
import org.locationtech.geowave.datastore.redis.util.RedisUtils;
import org.redisson.api.RedissonClient;

public class RedisDataIndexWriter implements RowWriter {
  private final RedisMapWrapper map;

  public RedisDataIndexWriter(
      final RedissonClient client,
      final Serialization serialization,
      final Compression compression,
      final String namespace,
      final String typeName,
      final boolean visibilityEnabled) {
    super();
    map =
        RedisUtils.getDataIndexMap(
            client,
            serialization,
            compression,
            namespace,
            typeName,
            visibilityEnabled);
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
