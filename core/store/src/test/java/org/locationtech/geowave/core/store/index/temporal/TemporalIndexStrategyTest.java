/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.index.temporal;

import java.util.Date;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.lexicoder.Lexicoders;

public class TemporalIndexStrategyTest {
  private final TemporalIndexStrategy strategy = new TemporalIndexStrategy();
  private final String fieldId = "fieldId";
  private final Date date = new Date(1440080038544L);

  @Test
  public void testInsertions() {
    final InsertionIds insertionIds = strategy.getInsertionIds(date);
    Assert.assertTrue(insertionIds.getSize() == 1);
    Assert.assertTrue(
        insertionIds.getCompositeInsertionIds().stream().map(i -> new ByteArray(i)).collect(
            Collectors.toSet()).contains(
                new ByteArray(Lexicoders.LONG.toByteArray(date.getTime()))));
  }

  @Test
  public void testDateRange() {
    final QueryRanges ranges =
        strategy.getQueryRanges(new TemporalQueryConstraint(fieldId, date, date));
    Assert.assertTrue(ranges.getCompositeQueryRanges().size() == 1);
    Assert.assertTrue(
        ranges.getCompositeQueryRanges().get(0).equals(
            new ByteArrayRange(
                Lexicoders.LONG.toByteArray(date.getTime()),
                Lexicoders.LONG.toByteArray(date.getTime()))));
  }
}
