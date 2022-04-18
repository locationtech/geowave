/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.filter;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRangesArray.ArrayOfArrays;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinates;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.data.IndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.query.constraints.CoordinateRangeUtils.RangeCache;
import org.locationtech.geowave.core.store.query.constraints.CoordinateRangeUtils.RangeLookupFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoordinateRangeQueryFilter implements QueryFilter {
  private static final Logger LOGGER = LoggerFactory.getLogger(CoordinateRangeQueryFilter.class);
  protected NumericIndexStrategy indexStrategy;
  protected RangeCache rangeCache;
  protected MultiDimensionalCoordinateRangesArray[] coordinateRanges;

  public CoordinateRangeQueryFilter() {}

  public CoordinateRangeQueryFilter(
      final NumericIndexStrategy indexStrategy,
      final MultiDimensionalCoordinateRangesArray[] coordinateRanges) {
    this.indexStrategy = indexStrategy;
    this.coordinateRanges = coordinateRanges;
    rangeCache = RangeLookupFactory.createMultiRangeLookup(coordinateRanges);
  }

  @Override
  public boolean accept(
      final CommonIndexModel indexModel,
      final IndexedPersistenceEncoding<?> persistenceEncoding) {
    if ((persistenceEncoding == null)
        || ((persistenceEncoding.getInsertionPartitionKey() == null)
            && (persistenceEncoding.getInsertionSortKey() == null))) {
      return false;
    }
    return inBounds(
        persistenceEncoding.getInsertionPartitionKey(),
        persistenceEncoding.getInsertionSortKey());
  }

  private boolean inBounds(final byte[] partitionKey, final byte[] sortKey) {
    final MultiDimensionalCoordinates coordinates =
        indexStrategy.getCoordinatesPerDimension(partitionKey, sortKey);
    return rangeCache.inBounds(coordinates);
  }

  @Override
  public byte[] toBinary() {
    final byte[] indexStrategyBytes = PersistenceUtils.toBinary(indexStrategy);
    final byte[] coordinateRangesBinary = new ArrayOfArrays(coordinateRanges).toBinary();

    final ByteBuffer buf =
        ByteBuffer.allocate(
            coordinateRangesBinary.length
                + indexStrategyBytes.length
                + VarintUtils.unsignedIntByteLength(indexStrategyBytes.length));

    VarintUtils.writeUnsignedInt(indexStrategyBytes.length, buf);
    buf.put(indexStrategyBytes);
    buf.put(coordinateRangesBinary);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    try {
      final int indexStrategyLength = VarintUtils.readUnsignedInt(buf);
      final byte[] indexStrategyBytes = ByteArrayUtils.safeRead(buf, indexStrategyLength);
      indexStrategy = (NumericIndexStrategy) PersistenceUtils.fromBinary(indexStrategyBytes);
      final byte[] coordRangeBytes = new byte[buf.remaining()];
      buf.get(coordRangeBytes);
      final ArrayOfArrays arrays = new ArrayOfArrays();
      arrays.fromBinary(coordRangeBytes);
      coordinateRanges = arrays.getCoordinateArrays();
      rangeCache = RangeLookupFactory.createMultiRangeLookup(coordinateRanges);
    } catch (final Exception e) {
      LOGGER.warn("Unable to read parameters", e);
    }
  }

  public NumericIndexStrategy getIndexStrategy() {
    return indexStrategy;
  }

  public MultiDimensionalCoordinateRangesArray[] getCoordinateRanges() {
    return coordinateRanges;
  }
}
