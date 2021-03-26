/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.binning;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.StatisticBinningStrategy;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.query.BinConstraintsImpl.ExplicitConstraints;

public class IndexBinningStrategy implements StatisticBinningStrategy {
  private Index index;
  public static final String NAME = "INDEX";

  public IndexBinningStrategy() {}

  public IndexBinningStrategy(final Index index) {
    super();
    this.index = index;
  }

  @Override
  public byte[] toBinary() {
    return PersistenceUtils.toBinary(index);
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    index = (Index) PersistenceUtils.fromBinary(bytes);
  }

  @Override
  public String getStrategyName() {
    return NAME;
  }

  @Override
  public String getDescription() {
    return "Bin based on an index";
  }

  @Override
  public Class<?>[] supportedConstraintClasses() {
    return ArrayUtils.addAll(
        StatisticBinningStrategy.super.supportedConstraintClasses(),
        MultiDimensionalNumericData.class,
        MultiDimensionalNumericData[].class);
  }

  @Override
  public ByteArrayConstraints constraints(final Object constraints) {
    if (constraints instanceof MultiDimensionalNumericData) {
      final QueryRanges range =
          index.getIndexStrategy().getQueryRanges((MultiDimensionalNumericData) constraints);
      final List<ByteArrayRange> queryRanges = range.getCompositeQueryRanges();
      return new ExplicitConstraints(queryRanges.toArray(new ByteArrayRange[queryRanges.size()]));
    } else if (constraints instanceof MultiDimensionalNumericData[]) {
      return new ExplicitConstraints(
          Arrays.stream((MultiDimensionalNumericData[]) constraints).flatMap(
              c -> index.getIndexStrategy().getQueryRanges(
                  c).getCompositeQueryRanges().stream()).toArray(ByteArrayRange[]::new));
    }
    return StatisticBinningStrategy.super.constraints(constraints);
  }

  @Override
  public <T> ByteArray[] getBins(
      final DataTypeAdapter<T> type,
      final T entry,
      final GeoWaveRow... rows) {
    final InsertionIds ids = type.encode(entry, index.getIndexModel()).getInsertionIds(index);

    return ids.getCompositeInsertionIds().stream().map(ByteArray::new).toArray(ByteArray[]::new);
  }

  @Override
  public String binToString(final ByteArray bin) {
    return bin.getHexString();
  }

  @Override
  public String getDefaultTag() {
    return index.getName();
  }

}
