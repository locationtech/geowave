/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.aggregate;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import org.junit.Test;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.BinningStrategy;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;

public class BinningAggregationOptionsTest {

  @Test
  public void testSerialization() {
    BinningAggregationOptions<?, ?> opts =
        new BinningAggregationOptions<>(new byte[0], null, null, 1234);
    assertThat(opts.baseBytes, is(new byte[0]));
    assertThat(opts.baseParamBytes, is(nullValue()));
    assertThat(opts.binningStrategy, is(nullValue()));
    assertThat(opts.maxBins, is(1234));

    byte[] serialized = PersistenceUtils.toBinary(opts);
    BinningAggregationOptions<?, ?> roundtripped =
        (BinningAggregationOptions<?, ?>) PersistenceUtils.fromBinary(serialized);

    assertThat(opts.baseBytes, is(roundtripped.baseBytes));
    assertThat(opts.baseParamBytes, is(roundtripped.baseParamBytes));
    assertThat(opts.binningStrategy, is(roundtripped.binningStrategy));
    assertThat(opts.maxBins, is(roundtripped.maxBins));

    final BinningStrategy blankStrategy = new BinningStrategy() {
      @Override
      public <T> ByteArray[] getBins(
          final DataTypeAdapter<T> type,
          final T entry,
          final GeoWaveRow... rows) {
        return new ByteArray[0];
      }

      @Override
      public byte[] toBinary() {
        return new byte[0];
      }

      @Override
      public void fromBinary(final byte[] bytes) {

      }
    };

    opts =
        new BinningAggregationOptions<>(
            new byte[] {0xC, 0xA, 0xF, 0xE, 0xB, 0xA, 0xB, 0xE},
            new byte[0],
            blankStrategy,
            Integer.MAX_VALUE);
    serialized = PersistenceUtils.toBinary(opts);
    roundtripped = (BinningAggregationOptions<?, ?>) PersistenceUtils.fromBinary(serialized);

    assertThat(opts.baseBytes, is(roundtripped.baseBytes));
    assertThat(opts.baseParamBytes, is(notNullValue()));
    assertThat(opts.binningStrategy, is(notNullValue()));
    assertThat(opts.maxBins, is(roundtripped.maxBins));
  }
}
