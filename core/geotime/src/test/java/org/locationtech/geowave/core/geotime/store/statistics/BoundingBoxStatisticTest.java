/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.geotools.referencing.CRS;
import org.junit.Test;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

public class BoundingBoxStatisticTest {

  @Test
  public void testBoundingBoxStatisticSerialization()
      throws NoSuchAuthorityCodeException, FactoryException {
    BoundingBoxStatistic expected = new BoundingBoxStatistic("testType", "testField");
    byte[] statBytes = PersistenceUtils.toBinary(expected);
    BoundingBoxStatistic actual = (BoundingBoxStatistic) PersistenceUtils.fromBinary(statBytes);
    assertEquals(expected.getTypeName(), actual.getTypeName());
    assertEquals(expected.getFieldName(), actual.getFieldName());
    assertNull(actual.getTransform());
    assertNull(actual.getBinningStrategy());

    CoordinateReferenceSystem sourceCrs = CRS.decode("EPSG:4326");
    CoordinateReferenceSystem destinationCrs = CRS.decode("EPSG:3857");
    MathTransform expectedTransform = CRS.findMathTransform(sourceCrs, destinationCrs);
    expected = new BoundingBoxStatistic("testType", "testField", sourceCrs, destinationCrs);
    statBytes = PersistenceUtils.toBinary(expected);
    actual = (BoundingBoxStatistic) PersistenceUtils.fromBinary(statBytes);
    assertEquals(expected.getTypeName(), actual.getTypeName());
    assertEquals(expected.getFieldName(), actual.getFieldName());
    assertEquals(expected.getSourceCrs(), actual.getSourceCrs());
    assertEquals(expected.getDestinationCrs(), actual.getDestinationCrs());
    assertEquals(expected.getTransform(), actual.getTransform());
    assertEquals(expectedTransform, actual.getTransform());
    assertNull(actual.getBinningStrategy());
  }

}
