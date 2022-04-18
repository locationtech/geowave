/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.aggregate;

import static org.junit.Assert.assertEquals;
import java.util.List;
import org.junit.Test;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;

public class CountAggregationTest extends AbstractCommonIndexAggregationTest {

  @Test
  public void testCountAggregation() {
    final Long expectedCount = 42L;
    final List<CommonIndexedPersistenceEncoding> encodings =
        generateObjects(expectedCount.intValue());
    final Long result = aggregateObjects(null, new CountAggregation(), encodings);
    assertEquals(expectedCount, result);
  }

}
