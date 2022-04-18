/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase.filters;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRangesArray.ArrayOfArrays;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinates;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.core.store.query.constraints.CoordinateRangeUtils.RangeCache;
import org.locationtech.geowave.core.store.query.constraints.CoordinateRangeUtils.RangeLookupFactory;
import org.locationtech.geowave.mapreduce.URLClassloaderUtils;

public class HBaseNumericIndexStrategyFilter extends FilterBase {
  private NumericIndexStrategy indexStrategy;
  private MultiDimensionalCoordinateRangesArray[] coordinateRanges;
  private RangeCache rangeCache;

  public HBaseNumericIndexStrategyFilter() {}

  public HBaseNumericIndexStrategyFilter(
      final NumericIndexStrategy indexStrategy,
      final MultiDimensionalCoordinateRangesArray[] coordinateRanges) {
    super();
    this.indexStrategy = indexStrategy;
    this.coordinateRanges = coordinateRanges;
    rangeCache = RangeLookupFactory.createMultiRangeLookup(coordinateRanges);
  }

  public static HBaseNumericIndexStrategyFilter parseFrom(final byte[] pbBytes)
      throws DeserializationException {
    final ByteBuffer buf = ByteBuffer.wrap(pbBytes);
    NumericIndexStrategy indexStrategy;
    MultiDimensionalCoordinateRangesArray[] coordinateRanges;
    try {
      final int indexStrategyLength = VarintUtils.readUnsignedInt(buf);
      final byte[] indexStrategyBytes = new byte[indexStrategyLength];
      buf.get(indexStrategyBytes);
      indexStrategy = (NumericIndexStrategy) URLClassloaderUtils.fromBinary(indexStrategyBytes);
      final byte[] coordRangeBytes = new byte[buf.remaining()];
      buf.get(coordRangeBytes);
      final ArrayOfArrays arrays = new ArrayOfArrays();
      arrays.fromBinary(coordRangeBytes);
      coordinateRanges = arrays.getCoordinateArrays();
    } catch (final Exception e) {
      throw new DeserializationException("Unable to read parameters", e);
    }

    return new HBaseNumericIndexStrategyFilter(indexStrategy, coordinateRanges);
  }

  @Override
  public byte[] toByteArray() throws IOException {
    final byte[] indexStrategyBytes = URLClassloaderUtils.toBinary(indexStrategy);
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
  public ReturnCode filterKeyValue(final Cell cell) throws IOException {
    if (inBounds(cell)) {
      return ReturnCode.INCLUDE;
    }
    return ReturnCode.SKIP;
  }

  private boolean inBounds(final Cell cell) {
    final GeoWaveKeyImpl cellKey =
        new GeoWaveKeyImpl(
            cell.getRowArray(),
            indexStrategy.getPartitionKeyLength(),
            cell.getRowOffset(),
            cell.getRowLength());

    final byte[] sortKey = cellKey.getSortKey();
    final byte[] partitionKey = cellKey.getPartitionKey();

    final MultiDimensionalCoordinates coordinates =
        indexStrategy.getCoordinatesPerDimension(partitionKey, sortKey);

    return rangeCache.inBounds(coordinates);
  }
}
