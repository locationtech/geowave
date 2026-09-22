/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce.splits;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class GeoWaveInputSplitTest {

  private static GeoWaveInputSplit splitWithCardinalities(final double... cardinalities) {
    final List<RangeLocationPair> pairs = new ArrayList<>(cardinalities.length);
    for (final double cardinality : cardinalities) {
      pairs.add(
          new RangeLocationPair(
              new GeoWaveRowRange(
                  new byte[] {(byte) 0x81},
                  new byte[] {0},
                  new byte[] {1},
                  true,
                  false),
              cardinality));
    }
    final Map<String, SplitInfo> splitInfo = new HashMap<>();
    splitInfo.put("index", new SplitInfo(null, pairs));
    return new GeoWaveInputSplit(splitInfo, new String[] {});
  }

  /**
   * Cardinality is a RowRangeHistogram estimate, so a range holding rows routinely estimates below
   * one. Consumers that honor getLength() - Spark, via spark.hadoopRDD.ignoreEmptySplits - drop a
   * zero-length split without ever reading it.
   */
  @Test
  public void fractionalCardinalityDoesNotReportAnEmptySplit() throws IOException {
    assertTrue(splitWithCardinalities(1.6e-4).getLength() > 0);
    assertTrue(splitWithCardinalities(1.6e-4, 4.8e-4, 0.0033, 0.027).getLength() > 0);
  }

  /** A histogram can estimate zero for a range that does hold rows. */
  @Test
  public void zeroCardinalityDoesNotReportAnEmptySplit() throws IOException {
    assertTrue(splitWithCardinalities(0.0).getLength() > 0);
    assertTrue(splitWithCardinalities(0.0, 0.0, 0.0, 1.0).getLength() > 0);
  }

  @Test
  public void splitWithNoRangesIsEmpty() throws IOException {
    final Map<String, SplitInfo> splitInfo = new HashMap<>();
    splitInfo.put("index", new SplitInfo(null, Collections.emptyList()));
    assertEquals(0, new GeoWaveInputSplit(splitInfo, new String[] {}).getLength());
    assertEquals(0, new GeoWaveInputSplit(new HashMap<>(), new String[] {}).getLength());
  }

  @Test
  public void lengthTracksCardinalityWhereItIsMeaningful() throws IOException {
    assertEquals(1196, splitWithCardinalities(1195.96).getLength());
    assertEquals(1196, splitWithCardinalities(1000.5, 195.0).getLength());
  }
}
