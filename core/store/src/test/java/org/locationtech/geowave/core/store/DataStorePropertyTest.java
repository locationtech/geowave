/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.data.visibility.GlobalVisibilityHandler;

public class DataStorePropertyTest {

  @Test
  public void testSerialization() {
    DataStoreProperty property = new DataStoreProperty("key", 15L);
    assertEquals("key", property.getKey());
    assertEquals(15L, property.getValue());
    byte[] serialized = PersistenceUtils.toBinary(property);
    property = (DataStoreProperty) PersistenceUtils.fromBinary(serialized);
    assertEquals("key", property.getKey());
    assertEquals(15L, property.getValue());

    property = new DataStoreProperty("key", "some value");
    assertEquals("key", property.getKey());
    assertEquals("some value", property.getValue());
    serialized = PersistenceUtils.toBinary(property);
    property = (DataStoreProperty) PersistenceUtils.fromBinary(serialized);
    assertEquals("key", property.getKey());
    assertEquals("some value", property.getValue());

    // You should be able to store persistables as well
    property = new DataStoreProperty("key", new GlobalVisibilityHandler("a"));
    assertEquals("key", property.getKey());
    assertTrue(property.getValue() instanceof GlobalVisibilityHandler);
    assertEquals(
        "a",
        ((GlobalVisibilityHandler) property.getValue()).getVisibility(null, null, null));
    serialized = PersistenceUtils.toBinary(property);
    property = (DataStoreProperty) PersistenceUtils.fromBinary(serialized);
    assertEquals("key", property.getKey());
    assertTrue(property.getValue() instanceof GlobalVisibilityHandler);
    assertEquals(
        "a",
        ((GlobalVisibilityHandler) property.getValue()).getVisibility(null, null, null));
  }

}
