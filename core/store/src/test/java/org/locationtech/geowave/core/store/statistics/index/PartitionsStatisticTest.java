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
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveKey;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.statistics.index.PartitionsStatistic.PartitionsValue;

public class PartitionsStatisticTest {
  static final long base = 7l;
  static int counter = 0;

  private GeoWaveKey genKey(final long id) {
    final InsertionIds insertionIds =
        new InsertionIds(
            new byte[] {(byte) (counter++ % 32)},
            Arrays.asList(
                StringUtils.stringToBinary(String.format("\12%5h", base + id) + "20030f89")));
    return GeoWaveKeyImpl.createKeys(insertionIds, new byte[] {}, (short) 0)[0];
  }

  @Test
  public void testIngest() {
    final PartitionsStatistic statistic = new PartitionsStatistic();
    final PartitionsValue value = statistic.createEmpty();

    for (long i = 0; i < 10000; i++) {
      final GeoWaveRow row = new GeoWaveRowImpl(genKey(i), new GeoWaveValue[] {});
      value.entryIngested(null, 1, row);
    }

    assertEquals(32, value.getValue().size());
    for (byte i = 0; i < 32; i++) {
      Assert.assertTrue(value.getValue().contains(new ByteArray(new byte[] {i})));
    }
  }
}
