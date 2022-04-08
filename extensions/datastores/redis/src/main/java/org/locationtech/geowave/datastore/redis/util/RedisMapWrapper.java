/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.redis.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.redisson.api.RBatch;
import org.redisson.api.RMap;
import org.redisson.api.RMapAsync;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;

public class RedisMapWrapper extends
    AbstractRedisSetWrapper<RMapAsync<byte[], byte[]>, RMap<byte[], byte[]>> {
  private final boolean visibilityEnabled;

  public RedisMapWrapper(
      final RedissonClient client,
      final String setName,
      final Codec codec,
      final boolean visibilityEnabled) {
    super(client, setName, codec);
    this.visibilityEnabled = visibilityEnabled;
  }

  public boolean remove(final byte[] dataId) {
    return getCurrentSyncCollection().remove(dataId) != null;
  }

  public void add(final byte[] dataId, final GeoWaveValue value) {
    preAdd();
    getCurrentAsyncCollection().putAsync(
        dataId,
        DataIndexUtils.serializeDataIndexValue(value, visibilityEnabled));
  }

  public void remove(final byte[][] dataIds) {
    getCurrentSyncCollection().fastRemoveAsync(dataIds);
  }


  public Iterator<GeoWaveRow> getRows(final byte[][] dataIds, final short adapterId) {
    final Map<byte[], byte[]> results =
        getCurrentSyncCollection().getAll(new HashSet<>(Arrays.asList(dataIds)));
    return Arrays.stream(dataIds).filter(dataId -> results.containsKey(dataId)).map(
        dataId -> DataIndexUtils.deserializeDataIndexRow(
            dataId,
            adapterId,
            results.get(dataId),
            visibilityEnabled)).iterator();
  }

  public Iterator<GeoWaveRow> getRows(
      final byte[] startDataId,
      final byte[] endDataId,
      final short adapterId) {
    if ((startDataId == null) && (endDataId == null)) {
      return getCurrentSyncCollection().entrySet().stream().map(
          e -> DataIndexUtils.deserializeDataIndexRow(
              e.getKey(),
              adapterId,
              e.getValue(),
              visibilityEnabled)).iterator();
    }
    // this is not a common use case, if it were a different (sorted) collection may be an
    // improvement
    final List<byte[]> list = new ArrayList<>();
    ByteArrayUtils.addAllIntermediaryByteArrays(list, new ByteArrayRange(startDataId, endDataId));
    final Map<byte[], byte[]> results = getCurrentSyncCollection().getAll(new HashSet<>(list));
    return list.stream().filter(dataId -> results.containsKey(dataId)).map(
        dataId -> DataIndexUtils.deserializeDataIndexRow(
            dataId,
            adapterId,
            results.get(dataId),
            visibilityEnabled)).iterator();
  }

  @Override
  protected RMapAsync<byte[], byte[]> initAsyncCollection(
      final RBatch batch,
      final String setName,
      final Codec codec) {
    return batch.getMap(setName, codec);
  }

  @Override
  protected RMap<byte[], byte[]> initSyncCollection(
      final RedissonClient client,
      final String setName,
      final Codec codec) {
    return client.getMap(setName, codec);
  }
}
