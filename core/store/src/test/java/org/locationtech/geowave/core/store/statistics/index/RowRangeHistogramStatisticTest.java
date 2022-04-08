/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.index;

import static org.junit.Assert.assertEquals;
import java.util.Arrays;
import org.junit.Test;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveKey;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.statistics.index.RowRangeHistogramStatistic.RowRangeHistogramValue;

public class RowRangeHistogramStatisticTest {
  static final long base = 7l;

  private GeoWaveKey genKey(final long id) {
    final InsertionIds insertionIds =
        new InsertionIds(
            Arrays.asList(
                StringUtils.stringToBinary(String.format("\12%5h", base + id) + "20030f89")));
    return GeoWaveKeyImpl.createKeys(insertionIds, new byte[] {}, (short) 0)[0];
  }

  @Test
  public void testIngest() {
    final RowRangeHistogramStatistic stats = new RowRangeHistogramStatistic("indexName");
    final RowRangeHistogramValue value = stats.createEmpty();

    for (long i = 0; i < 10000; i++) {
      final GeoWaveRow row = new GeoWaveRowImpl(genKey(i), new GeoWaveValue[] {});
      value.entryIngested(null, 1, row);
    }

    System.out.println(stats.toString());

    assertEquals(1.0, value.cdf(genKey(10000).getSortKey()), 0.00001);

    assertEquals(0.0, value.cdf(genKey(0).getSortKey()), 0.00001);

    assertEquals(0.5, value.cdf(genKey(5000).getSortKey()), 0.04);

    final RowRangeHistogramValue value2 = stats.createEmpty();

    for (long j = 10000; j < 20000; j++) {

      final GeoWaveRow row = new GeoWaveRowImpl(genKey(j), new GeoWaveValue[] {});
      value2.entryIngested(null, 1, row);
    }

    assertEquals(0.0, value2.cdf(genKey(10000).getSortKey()), 0.00001);

    value.merge(value2);

    assertEquals(0.5, value.cdf(genKey(10000).getSortKey()), 0.15);

    value2.fromBinary(value.toBinary());

    assertEquals(0.5, value2.cdf(genKey(10000).getSortKey()), 0.15);
  }
}
