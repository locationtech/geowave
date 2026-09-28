/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.plugin;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.geotools.api.data.DataStore;
import org.junit.Test;
import org.locationtech.geowave.core.store.memory.MemoryStoreFactoryFamily;

public class GeoWaveGTDataStoreFactoryTest {
  @Test
  public void testDisposedStoreIsNotHandedOutAgain() throws IOException {
    final GeoWaveGTDataStoreFactory factory =
        new GeoWaveGTDataStoreFactory(new MemoryStoreFactoryFamily());
    final Map<String, Serializable> params = new HashMap<>();
    params.put("gwNamespace", "test_" + getClass().getName());

    final DataStore store = factory.createDataStore(params);
    assertSame(store, factory.createDataStore(params));

    store.dispose();
    final DataStore recreated = factory.createDataStore(params);
    assertNotSame(store, recreated);
    assertSame(recreated, factory.createDataStore(params));
    recreated.dispose();
  }
}
