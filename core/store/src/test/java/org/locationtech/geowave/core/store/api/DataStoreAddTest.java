/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.store.adapter.MockComponents.MockAbstractDataAdapter;
import org.locationtech.geowave.core.store.index.NullIndex;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;

public class DataStoreAddTest {

  private DataStore dataStore;
  private final String MOCK_DATA_TYPE_1 = "Some Data Type";
  private final String MOCK_DATA_TYPE_2 = "Another Data Type";

  @Before
  public void createStore() {
    dataStore = DataStoreFactory.createDataStore(new MemoryRequiredOptions());
  }

  @After
  public void tearDown() {
    dataStore.deleteAll();
  }

  @Test
  public void addIndex_Basic() {
    final NullIndex index1 = new NullIndex("index1");
    final NullIndex index2 = new NullIndex("index2");
    final MockAbstractDataAdapter adapter = new MockAbstractDataAdapter(MOCK_DATA_TYPE_1);
    dataStore.addType(adapter, index1);
    dataStore.addIndex(MOCK_DATA_TYPE_1, index2);
    assertEquals(2, dataStore.getIndices(MOCK_DATA_TYPE_1).length);
  }

  @Test
  public void addIndex_MultiIndexAdd() {
    final NullIndex index1 = new NullIndex("index1");
    final NullIndex index2 = new NullIndex("index2");
    final NullIndex index3 = new NullIndex("index3");
    final MockAbstractDataAdapter adapter = new MockAbstractDataAdapter(MOCK_DATA_TYPE_1);
    dataStore.addType(adapter, index1);
    dataStore.addIndex(MOCK_DATA_TYPE_1, index2, index3);
    assertEquals(3, dataStore.getIndices(MOCK_DATA_TYPE_1).length);
  }

  @Test
  public void addIndex_SameIndexVarArgs() {
    final NullIndex index1 = new NullIndex("index1");
    final NullIndex index2 = new NullIndex("index2");
    final MockAbstractDataAdapter adapter = new MockAbstractDataAdapter(MOCK_DATA_TYPE_1);
    dataStore.addType(adapter, index1);
    dataStore.addIndex(MOCK_DATA_TYPE_1, index2, index2, index2);
    assertEquals(2, dataStore.getIndices(MOCK_DATA_TYPE_1).length);
  }

  @Test
  public void addIndex_IndexAlreadyAdded() {
    final NullIndex index1 = new NullIndex("index1");
    final MockAbstractDataAdapter adapter = new MockAbstractDataAdapter(MOCK_DATA_TYPE_1);
    dataStore.addType(adapter, index1);
    dataStore.addIndex(MOCK_DATA_TYPE_1, index1);
    assertEquals(1, dataStore.getIndices(MOCK_DATA_TYPE_1).length);
  }

  @Test
  public void addType_Basic() {
    final NullIndex index = new NullIndex("myIndex");
    final MockAbstractDataAdapter adapter = new MockAbstractDataAdapter(MOCK_DATA_TYPE_1);
    dataStore.addType(adapter, index);
    final DataTypeAdapter<?>[] registeredTypes = dataStore.getTypes();
    assertEquals(1, registeredTypes.length);
    assertTrue(registeredTypes[0] instanceof MockAbstractDataAdapter);
  }

  @Test
  public void addType_MultiIndex() {
    final NullIndex index1 = new NullIndex("index1");
    final NullIndex index2 = new NullIndex("index2");
    final NullIndex index3 = new NullIndex("index3");
    final MockAbstractDataAdapter adapter = new MockAbstractDataAdapter(MOCK_DATA_TYPE_1);
    dataStore.addType(adapter, index1, index2, index3);
    final DataTypeAdapter<?>[] registeredTypes = dataStore.getTypes();
    assertEquals(1, registeredTypes.length);
    assertTrue(registeredTypes[0] instanceof MockAbstractDataAdapter);
    assertEquals(3, dataStore.getIndices(MOCK_DATA_TYPE_1).length);
  }

  @Test
  public void addType_SameIndexVarArgs() {
    final NullIndex index1 = new NullIndex("index1");
    final MockAbstractDataAdapter adapter = new MockAbstractDataAdapter(MOCK_DATA_TYPE_1);
    dataStore.addType(adapter, index1, index1, index1);
    final DataTypeAdapter<?>[] registeredTypes = dataStore.getTypes();
    assertEquals(1, registeredTypes.length);
    assertTrue(registeredTypes[0] instanceof MockAbstractDataAdapter);
    assertEquals(1, dataStore.getIndices(MOCK_DATA_TYPE_1).length);
  }

  @Test
  public void addType_MultiIndexAndMultiTypeSameAdapter() {
    final NullIndex mockType1Index1 = new NullIndex("mock1index1");
    final NullIndex mockType1Index2 = new NullIndex("mock1index2");
    final NullIndex mockType1Index3 = new NullIndex("mock1index3");
    final MockAbstractDataAdapter adapter1 = new MockAbstractDataAdapter(MOCK_DATA_TYPE_1);
    dataStore.addType(adapter1, mockType1Index1, mockType1Index2, mockType1Index3);
    final NullIndex mockType2Index1 = new NullIndex("mock2index1");
    final NullIndex mockType2Index2 = new NullIndex("mock2index2");
    final MockAbstractDataAdapter adapter2 = new MockAbstractDataAdapter(MOCK_DATA_TYPE_2);
    dataStore.addType(adapter2, mockType2Index1, mockType2Index2);
    final DataTypeAdapter<?>[] registeredTypes = dataStore.getTypes();
    assertEquals(2, registeredTypes.length);
    assertTrue(registeredTypes[0] instanceof MockAbstractDataAdapter);
    assertTrue(registeredTypes[1] instanceof MockAbstractDataAdapter);
    assertEquals(3, dataStore.getIndices(MOCK_DATA_TYPE_1).length);
    assertEquals(2, dataStore.getIndices(MOCK_DATA_TYPE_2).length);
  }

  @Test
  public void createWriter_NonNullForSeenType() {
    final NullIndex index = new NullIndex("myIndex");
    final MockAbstractDataAdapter adapter = new MockAbstractDataAdapter(MOCK_DATA_TYPE_1);
    dataStore.addType(adapter, index);
    final Writer<Integer> writer = dataStore.createWriter(MOCK_DATA_TYPE_1);
    assertNotNull(writer);
  }

  @Test
  public void createWriter_SeenTypeWriteNoError() {
    final NullIndex index = new NullIndex("myIndex");
    final MockAbstractDataAdapter adapter = new MockAbstractDataAdapter(MOCK_DATA_TYPE_1);
    dataStore.addType(adapter, index);
    final Writer<Integer> writer = dataStore.createWriter(MOCK_DATA_TYPE_1);
    writer.write(15);
    writer.write(0);
    writer.close();
  }

  @Test
  public void createWriter_NullForUnseenType() {
    final Writer<Object> writer = dataStore.createWriter(MOCK_DATA_TYPE_1);
    assertNull(writer);
  }

  @Test
  public void createWriter_NullForUnseenType2() {
    final NullIndex index = new NullIndex("myIndex");
    final MockAbstractDataAdapter adapter = new MockAbstractDataAdapter(MOCK_DATA_TYPE_1);
    dataStore.addType(adapter, index);
    final Writer<Integer> writer = dataStore.createWriter(MOCK_DATA_TYPE_2);
    assertNull(writer);
  }
}
