/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.adapter.merge;

import java.awt.image.SampleModel;
import java.nio.ByteBuffer;
import java.util.Map;
import org.locationtech.geowave.adapter.raster.adapter.RasterDataAdapter;
import org.locationtech.geowave.adapter.raster.adapter.RasterTile;
import org.locationtech.geowave.adapter.raster.util.SampleModelPersistenceUtils;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.opengis.coverage.grid.GridCoverage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class SingleAdapterServerMergeStrategy<T extends Persistable> implements
    ServerMergeStrategy,
    Persistable {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(SingleAdapterServerMergeStrategy.class);
  // the purpose for these maps instead of a list of samplemodel and adapter
  // ID pairs is to allow for multiple adapters to share the same sample model
  protected short internalAdapterId;
  protected SampleModel sampleModel;
  protected RasterTileMergeStrategy<T> mergeStrategy;

  public SingleAdapterServerMergeStrategy() {}

  public SingleAdapterServerMergeStrategy(
      final short internalAdapterId,
      final SampleModel sampleModel,
      final RasterTileMergeStrategy<T> mergeStrategy) {
    this.internalAdapterId = internalAdapterId;
    this.sampleModel = sampleModel;
    this.mergeStrategy = mergeStrategy;
  }

  @SuppressFBWarnings(
      value = {"DLS_DEAD_LOCAL_STORE"},
      justification = "Incorrect warning, sampleModelBinary used")
  @Override
  public byte[] toBinary() {
    final byte[] sampleModelBinary = SampleModelPersistenceUtils.getSampleModelBinary(sampleModel);

    final byte[] mergeStrategyBinary = PersistenceUtils.toBinary(mergeStrategy);

    final int byteCount =
        sampleModelBinary.length
            + VarintUtils.unsignedIntByteLength(sampleModelBinary.length)
            + VarintUtils.unsignedShortByteLength(internalAdapterId)
            + mergeStrategyBinary.length
            + VarintUtils.unsignedIntByteLength(mergeStrategyBinary.length);
    final ByteBuffer buf = ByteBuffer.allocate(byteCount);
    VarintUtils.writeUnsignedInt(sampleModelBinary.length, buf);
    buf.put(sampleModelBinary);
    VarintUtils.writeUnsignedShort(internalAdapterId, buf);
    VarintUtils.writeUnsignedInt(mergeStrategyBinary.length, buf);
    buf.put(mergeStrategyBinary);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);

    final byte[] sampleModelBinary = ByteArrayUtils.safeRead(buf, VarintUtils.readUnsignedInt(buf));
    if (sampleModelBinary.length > 0) {
      try {
        sampleModel = SampleModelPersistenceUtils.getSampleModel(sampleModelBinary);
      } catch (final Exception e) {
        LOGGER.warn("Unable to deserialize sample model", e);
      }
    } else {
      LOGGER.warn("Sample model binary is empty, unable to deserialize");
    }

    internalAdapterId = VarintUtils.readUnsignedShort(buf);

    final byte[] mergeStrategyBinary =
        ByteArrayUtils.safeRead(buf, VarintUtils.readUnsignedInt(buf));
    if (mergeStrategyBinary.length > 0) {
      try {
        mergeStrategy = (RasterTileMergeStrategy) PersistenceUtils.fromBinary(mergeStrategyBinary);

      } catch (final Exception e) {
        LOGGER.warn("Unable to deserialize merge strategy", e);
      }
    } else {
      LOGGER.warn("Merge strategy binary is empty, unable to deserialize");
    }
  }

  @Override
  public void merge(
      final RasterTile thisTile,
      final RasterTile nextTile,
      final short internalAdapterId) {
    if (mergeStrategy != null) {
      mergeStrategy.merge(thisTile, nextTile, sampleModel);
    }
  }

  public T getMetadata(
      final GridCoverage tileGridCoverage,
      final Map originalCoverageProperties,
      final RasterDataAdapter dataAdapter) {
    if (mergeStrategy != null) {
      return mergeStrategy.getMetadata(tileGridCoverage, dataAdapter);
    }
    return null;
  }
}
