/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;


import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.locationtech.geowave.core.store.adapter.MockComponents;
import org.locationtech.geowave.core.store.index.IndexImpl;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;

public class DataStoreRemoveTest {
  private static int counter = 0;
  private static final String MOCK_DATA_TYPE_1 = "Some Data Type";
  private static final String MOCK_DATA_TYPE_2 = "Another Data Type";

  private DataStore dataStore;

  @Before
  public void createStore() {
    dataStore = DataStoreFactory.createDataStore(new MemoryRequiredOptions());

    final Index index =
        new IndexImpl(
            new MockComponents.MockIndexStrategy(),
            new MockComponents.TestIndexModel("test1"));

    final DataTypeAdapter<Integer> adapter =
        new MockComponents.MockAbstractDataAdapter(MOCK_DATA_TYPE_1);

    dataStore.addType(adapter, index);
    counter++;
  }

  @After
  public void tearDown() {
    dataStore.deleteAll();
  }

  @Test
  public void testRemoveType() {
    // given
    final DataTypeAdapter<Integer> adapter2 =
        new MockComponents.MockAbstractDataAdapter(MOCK_DATA_TYPE_2);

    final Index index2 =
        new IndexImpl(
            new MockComponents.MockIndexStrategy(),
            new MockComponents.TestIndexModel("test2"));

    dataStore.addType(adapter2, index2);


    // when
    dataStore.removeType(adapter2.getTypeName());


    // then
    Assert.assertEquals(1, dataStore.getTypes().length);
    Assert.assertEquals(MOCK_DATA_TYPE_1, dataStore.getTypes()[0].getTypeName());
  }

  @Test
  public void testRemoveInvalidType() {
    // given
    // when
    dataStore.removeType("Adapter 2");

    // then
    Assert.assertEquals(1, dataStore.getTypes().length);
  }

  /*
   * Untestable code: baseOperations.deleteAll(indexName, typeName, adapterId); just returns false
   * and does not actually delete anything. src: MemoryDataStoreOperations#deleteAll(table, type,
   * adapter, args)
   */
  @Ignore
  @Test
  public void testDelete() {
    // given
    // when
    dataStore.delete(QueryBuilder.newBuilder().addTypeName(MOCK_DATA_TYPE_1).build());

    // then
    Assert.assertEquals(0, dataStore.getTypes().length);
  }

  @Test
  public void testDeleteAll() {
    // given
    // when
    dataStore.deleteAll();

    // then
    Assert.assertEquals(0, dataStore.getTypes().length);
    Assert.assertEquals(0, dataStore.getIndices().length);
  }

  @Test
  public void testRemoveIndexSingle() {
    // given
    final Index index2 =
        new IndexImpl(
            new MockComponents.MockIndexStrategy(),
            new MockComponents.TestIndexModel("test2"));

    final Index index3 =
        new IndexImpl(
            new MockComponents.MockIndexStrategy(),
            new MockComponents.TestIndexModel("test3"));

    final DataTypeAdapter<Integer> adapter2 =
        new MockComponents.MockAbstractDataAdapter(MOCK_DATA_TYPE_2);

    dataStore.addType(adapter2, index2, index3);
    dataStore.addIndex(MOCK_DATA_TYPE_1, index2);

    // when
    dataStore.removeIndex(index2.getName());

    // then
    Assert.assertEquals(1, dataStore.getIndices(MOCK_DATA_TYPE_1).length);
    Assert.assertEquals(1, dataStore.getIndices(MOCK_DATA_TYPE_2).length);
  }

  @Test(expected = IllegalStateException.class)
  public void testRemoveIndexSingleFinal() {
    // given
    // when
    dataStore.removeIndex("Test_test1");

    // then
    throw new AssertionError("Last index should thrown an IllegalStateException");
  }

  @Test
  public void testRemoveIndexSingleInvalid() {
    // given
    // when
    dataStore.removeIndex("Test_test2");

    // then
    Assert.assertEquals(1, dataStore.getIndices(MOCK_DATA_TYPE_1).length);
  }

  /*
   * Untestable code: baseOperations.deleteAll(indexName, typeName, adapterId); just returns false
   * and does not actually delete anything. src: BaseDataStore#removeIndex(type, index) ->
   * MemoryDataStoreOperations#deleteAll(table, type, adapter, args) Also has the error that it
   * tries to delete from all adapters. Not just targeted one.
   */
  @Ignore
  @Test
  public void testRemoveIndexDouble() {
    // given
    final DataTypeAdapter<Integer> adapter2 =
        new MockComponents.MockAbstractDataAdapter(MOCK_DATA_TYPE_2);

    final Index index2 =
        new IndexImpl(
            new MockComponents.MockIndexStrategy(),
            new MockComponents.TestIndexModel("test2"));

    dataStore.addIndex(MOCK_DATA_TYPE_1, index2);
    dataStore.addType(adapter2, index2);

    // when
    dataStore.removeIndex(MOCK_DATA_TYPE_1, index2.getName());

    // then
    Assert.assertEquals(1, dataStore.getIndices(MOCK_DATA_TYPE_1).length);
    Assert.assertEquals(1, dataStore.getIndices(MOCK_DATA_TYPE_2).length);
  }

  @Test(expected = IllegalStateException.class)
  public void testRemoveIndexDoubleFinal() {
    // given
    // when
    dataStore.removeIndex(MOCK_DATA_TYPE_1, "Test_test1");

    // then
    throw new AssertionError();
  }
}
