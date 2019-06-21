/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.adapter.merge;

import java.io.IOException;
import java.util.Map;
import org.locationtech.geowave.adapter.raster.adapter.RasterTile;
import org.locationtech.geowave.adapter.raster.adapter.ServerMergeableRasterTile;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.adapter.RowMergingDataAdapter.RowTransform;

/**
 * This class can be used by both the RasterTileCombiner and the RasterTileVisibilityCombiner to
 * execute the merge strategy
 */
public class RasterTileRowTransform<T extends Persistable> implements RowTransform<Mergeable> {
  public static final String TRANSFORM_NAME = "RasterTile";
  public static final String MERGE_STRATEGY_KEY = "MERGE_STRATEGY";
  private ServerMergeStrategy<T> mergeStrategy;
  // this priority is fairly arbitrary at the moment
  private static final int RASTER_TILE_PRIORITY = 4;

  public Mergeable transform(final short internalAdapterId, final Mergeable mergeable) {
    if ((mergeable != null) && (mergeable instanceof RasterTile)) {
      final RasterTile<T> rasterTile = (RasterTile) mergeable;
      return new ServerMergeableRasterTile<>(
          rasterTile.getDataBuffer(),
          rasterTile.getMetadata(),
          mergeStrategy,
          internalAdapterId);
    }
    return mergeable;
  }

  @Override
  public void initOptions(final Map<String, String> options) throws IOException {
    final String mergeStrategyStr = options.get(MERGE_STRATEGY_KEY);
    if (mergeStrategyStr != null) {
      final byte[] mergeStrategyBytes = ByteArrayUtils.byteArrayFromString(mergeStrategyStr);
      mergeStrategy = (ServerMergeStrategy<T>) PersistenceUtils.fromBinary(mergeStrategyBytes);
    }
  }

  @Override
  public Mergeable getRowAsMergeableObject(
      final short internalAdapterId,
      final ByteArray fieldId,
      final byte[] rowValueBinary) {
    final RasterTile mergeable = new RasterTile();

    if (mergeable != null) {
      mergeable.fromBinary(rowValueBinary);
    }
    return transform(internalAdapterId, mergeable);
  }

  @Override
  public byte[] getBinaryFromMergedObject(final Mergeable rowObject) {
    return rowObject.toBinary();
  }

  @Override
  public byte[] toBinary() {
    return new byte[] {};
  }

  @Override
  public void fromBinary(final byte[] bytes) {}

  @Override
  public String getTransformName() {
    return TRANSFORM_NAME;
  }

  @Override
  public int getBaseTransformPriority() {
    return RASTER_TILE_PRIORITY;
  }
}
