/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
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
import org.locationtech.geowave.core.store.BaseDataStoreOptions;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.adapter.MockComponents;
import org.locationtech.geowave.core.store.adapter.MockComponents.MockAbstractDataAdapter;
import org.locationtech.geowave.core.store.index.IndexImpl;
import org.locationtech.geowave.core.store.index.NullIndex;
import org.locationtech.geowave.core.store.memory.MemoryStoreFactoryFamily;
import java.util.Arrays;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DataStoreTest {

  private static final String MOCK_DATA_TYPE_1 = "Some Data Type";
  private static final String MOCK_DATA_TYPE_2 = "Another Data Type";
  private static int counter = 0;
  private DataStore dataStore;

  @Before
  public void createStore() {
    dataStore = DataStoreFactory.createDataStore(new MemoryRequiredNameSpaceOptions(counter + ""));
    counter++;
  }

  @After
  public void tearDown() {
    dataStore.deleteAll();
  }

  @Test
  public void addIndex_Basic() {
    NullIndex index1 = new NullIndex("index1");
    NullIndex index2 = new NullIndex("index2");
    MockComponents.MockAbstractDataAdapter adapter =
        new MockComponents.MockAbstractDataAdapter(MOCK_DATA_TYPE_1);
    dataStore.addType(adapter, index1);
    dataStore.addIndex(MOCK_DATA_TYPE_1, index2);
    assertEquals(2, dataStore.getIndices(MOCK_DATA_TYPE_1).length);
  }

  @Test
  public void addIndex_MultiIndexAdd() {
    NullIndex index1 = new NullIndex("index1");
    NullIndex index2 = new NullIndex("index2");
    NullIndex index3 = new NullIndex("index3");
    MockAbstractDataAdapter adapter = new MockAbstractDataAdapter(MOCK_DATA_TYPE_1);
    dataStore.addType(adapter, index1);
    dataStore.addIndex(MOCK_DATA_TYPE_1, index2, index3);
    assertEquals(3, dataStore.getIndices(MOCK_DATA_TYPE_1).length);
  }

  @Test
  public void addIndex_SameIndexVarArgs() {
    NullIndex index1 = new NullIndex("index1");
    NullIndex index2 = new NullIndex("index2");
    MockAbstractDataAdapter adapter = new MockAbstractDataAdapter(MOCK_DATA_TYPE_1);
    dataStore.addType(adapter, index1);
    dataStore.addIndex(MOCK_DATA_TYPE_1, index2, index2, index2);
    assertEquals(2, dataStore.getIndices(MOCK_DATA_TYPE_1).length);
  }

  @Test
  public void addIndex_IndexAlreadyAdded() {
    NullIndex index1 = new NullIndex("index1");
    MockAbstractDataAdapter adapter = new MockAbstractDataAdapter(MOCK_DATA_TYPE_1);
    dataStore.addType(adapter, index1);
    dataStore.addIndex(MOCK_DATA_TYPE_1, index1);
    assertEquals(1, dataStore.getIndices(MOCK_DATA_TYPE_1).length);
  }

  @Test
  public void addType_Basic() {
    NullIndex index = new NullIndex("myIndex");
    MockAbstractDataAdapter adapter = new MockAbstractDataAdapter(MOCK_DATA_TYPE_1);
    dataStore.addType(adapter, index);
    DataTypeAdapter<?>[] registeredTypes = dataStore.getTypes();
    assertEquals(1, registeredTypes.length);
    assertTrue(registeredTypes[0] instanceof MockAbstractDataAdapter);
  }

  @Test
  public void addType_MultiIndex() {
    NullIndex index1 = new NullIndex("index1");
    NullIndex index2 = new NullIndex("index2");
    NullIndex index3 = new NullIndex("index3");
    MockAbstractDataAdapter adapter = new MockAbstractDataAdapter(MOCK_DATA_TYPE_1);
    dataStore.addType(adapter, index1, index2, index3);
    DataTypeAdapter<?>[] registeredTypes = dataStore.getTypes();
    assertEquals(1, registeredTypes.length);
    assertTrue(registeredTypes[0] instanceof MockAbstractDataAdapter);
    assertEquals(3, dataStore.getIndices(MOCK_DATA_TYPE_1).length);
  }

  @Test
  public void addType_SameIndexVarArgs() {
    NullIndex index1 = new NullIndex("index1");
    MockAbstractDataAdapter adapter = new MockAbstractDataAdapter(MOCK_DATA_TYPE_1);
    dataStore.addType(adapter, index1, index1, index1);
    DataTypeAdapter<?>[] registeredTypes = dataStore.getTypes();
    assertEquals(1, registeredTypes.length);
    assertTrue(registeredTypes[0] instanceof MockAbstractDataAdapter);
    assertEquals(1, dataStore.getIndices(MOCK_DATA_TYPE_1).length);
  }

  @Test
  public void addType_MultiIndexAndMultiTypeSameAdapter() {
    NullIndex mockType1Index1 = new NullIndex("mock1index1");
    NullIndex mockType1Index2 = new NullIndex("mock1index2");
    NullIndex mockType1Index3 = new NullIndex("mock1index3");
    MockAbstractDataAdapter adapter1 = new MockAbstractDataAdapter(MOCK_DATA_TYPE_1);
    dataStore.addType(adapter1, mockType1Index1, mockType1Index2, mockType1Index3);
    NullIndex mockType2Index1 = new NullIndex("mock2index1");
    NullIndex mockType2Index2 = new NullIndex("mock2index2");
    MockAbstractDataAdapter adapter2 = new MockAbstractDataAdapter(MOCK_DATA_TYPE_2);
    dataStore.addType(adapter2, mockType2Index1, mockType2Index2);
    DataTypeAdapter<?>[] registeredTypes = dataStore.getTypes();
    assertEquals(2, registeredTypes.length);
    assertTrue(registeredTypes[0] instanceof MockAbstractDataAdapter);
    assertTrue(registeredTypes[1] instanceof MockAbstractDataAdapter);
    assertEquals(3, dataStore.getIndices(MOCK_DATA_TYPE_1).length);
    assertEquals(2, dataStore.getIndices(MOCK_DATA_TYPE_2).length);
  }

  @Test
  public void createWriter_NonNullForSeenType() {
    NullIndex index = new NullIndex("myIndex");
    MockAbstractDataAdapter adapter = new MockAbstractDataAdapter(MOCK_DATA_TYPE_1);
    dataStore.addType(adapter, index);
    Writer<Integer> writer = dataStore.createWriter(MOCK_DATA_TYPE_1);
    assertNotNull(writer);
  }

  @Test
  public void createWriter_SeenTypeWriteNoError() {
    NullIndex index = new NullIndex("myIndex");
    MockAbstractDataAdapter adapter = new MockAbstractDataAdapter(MOCK_DATA_TYPE_1);
    dataStore.addType(adapter, index);
    Writer<Integer> writer = dataStore.createWriter(MOCK_DATA_TYPE_1);
    writer.write(15);
    writer.write(0);
    writer.close();
  }

  @Test
  public void createWriter_NullForUnseenType() {
    Writer<Object> writer = dataStore.createWriter(MOCK_DATA_TYPE_1);
    assertNull(writer);
  }

  @Test
  public void createWriter_NullForUnseenType2() {
    NullIndex index = new NullIndex("myIndex");
    MockAbstractDataAdapter adapter = new MockAbstractDataAdapter(MOCK_DATA_TYPE_1);
    dataStore.addType(adapter, index);
    Writer<Integer> writer = dataStore.createWriter(MOCK_DATA_TYPE_2);
    assertNull(writer);
  }

  @Test
  public void testRemoveType() {
    // given
    final Index index =
        new IndexImpl(
            new MockComponents.MockIndexStrategy(),
            new MockComponents.TestIndexModel("test1"));

    final DataTypeAdapter<Integer> adapter =
        new MockComponents.MockAbstractDataAdapter(MOCK_DATA_TYPE_1);

    dataStore.addType(adapter, index);

    final DataTypeAdapter<Integer> adapter2 =
        new MockComponents.MockAbstractDataAdapter(MOCK_DATA_TYPE_2);

    final Index index2 =
        new IndexImpl(
            new MockComponents.MockIndexStrategy(),
            new MockComponents.TestIndexModel("test2"));

    dataStore.addType(adapter2, index2);


    // when
    dataStore.removeType(adapter2.getTypeName());
    Arrays.stream(dataStore.getTypes()).forEach(t -> System.err.println(t.getTypeName()));
    // then
    Assert.assertEquals(1, dataStore.getTypes().length);
    Assert.assertEquals(MOCK_DATA_TYPE_1, dataStore.getTypes()[0].getTypeName());
  }

  // me thinks this shouldn't throw an NPE
  @Test
  public void testRemoveInvalidType() {
    // given
    final Index index =
        new IndexImpl(
            new MockComponents.MockIndexStrategy(),
            new MockComponents.TestIndexModel("test1"));

    final DataTypeAdapter<Integer> adapter =
        new MockComponents.MockAbstractDataAdapter(MOCK_DATA_TYPE_1);

    dataStore.addType(adapter, index);

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
    final Index index =
        new IndexImpl(
            new MockComponents.MockIndexStrategy(),
            new MockComponents.TestIndexModel("test1"));

    final DataTypeAdapter<Integer> adapter =
        new MockComponents.MockAbstractDataAdapter(MOCK_DATA_TYPE_1);

    dataStore.addType(adapter, index);

    // when
    dataStore.delete(QueryBuilder.newBuilder().addTypeName(MOCK_DATA_TYPE_1).build());

    // then
    Assert.assertEquals(0, dataStore.getTypes().length);
  }

  @Test
  public void testDeleteAll() {
    // given
    final Index index =
        new IndexImpl(
            new MockComponents.MockIndexStrategy(),
            new MockComponents.TestIndexModel("test1"));

    final DataTypeAdapter<Integer> adapter =
        new MockComponents.MockAbstractDataAdapter(MOCK_DATA_TYPE_1);

    dataStore.addType(adapter, index);

    // when
    dataStore.deleteAll();

    // then
    Assert.assertEquals(0, dataStore.getTypes().length);
    Assert.assertEquals(0, dataStore.getIndices().length);
  }

  @Test
  public void testRemoveIndexSingle() {
    // given
    final Index index =
        new IndexImpl(
            new MockComponents.MockIndexStrategy(),
            new MockComponents.TestIndexModel("test1"));

    final DataTypeAdapter<Integer> adapter =
        new MockComponents.MockAbstractDataAdapter(MOCK_DATA_TYPE_1);

    dataStore.addType(adapter, index);

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
    final Index index =
        new IndexImpl(
            new MockComponents.MockIndexStrategy(),
            new MockComponents.TestIndexModel("test1"));

    final DataTypeAdapter<Integer> adapter =
        new MockComponents.MockAbstractDataAdapter(MOCK_DATA_TYPE_1);

    dataStore.addType(adapter, index);

    // when
    dataStore.removeIndex("Test_test1");

    // then
    throw new AssertionError("Last index should thrown an IllegalStateException");
  }

  /*
   * Untestable code: baseOperations.deleteAll(indexName, typeName, adapterId); just returns false
   * and does not actually delete anything. src: BaseDataStore#removeIndex(type, index) ->
   * MemoryDataStoreOperations#deleteAll(table, type, adapter, args)
   *
   * Also has the error that it tries to delete from all adapters. Not just targeted one.
   */
  @Ignore
  @Test
  public void testRemoveIndexDouble() {
    // given
    final Index index =
        new IndexImpl(
            new MockComponents.MockIndexStrategy(),
            new MockComponents.TestIndexModel("test1"));

    final DataTypeAdapter<Integer> adapter =
        new MockComponents.MockAbstractDataAdapter(MOCK_DATA_TYPE_1);

    dataStore.addType(adapter, index);

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
    final Index index =
        new IndexImpl(
            new MockComponents.MockIndexStrategy(),
            new MockComponents.TestIndexModel("test1"));

    final DataTypeAdapter<Integer> adapter =
        new MockComponents.MockAbstractDataAdapter(MOCK_DATA_TYPE_1);

    dataStore.addType(adapter, index);
    // when
    dataStore.removeIndex(MOCK_DATA_TYPE_1, "Test_test1");

    // then
    throw new AssertionError();
  }

  private class MemoryRequiredNameSpaceOptions extends StoreFactoryOptions {
    private final DataStoreOptions options = new BaseDataStoreOptions() {
      @Override
      public boolean isServerSideLibraryEnabled() {
        // memory datastore doesn't have a serverside option
        return false;
      }
    };

    public MemoryRequiredNameSpaceOptions(String namespace) {
      super(namespace);
    }

    @Override
    public StoreFactoryFamilySpi getStoreFactory() {
      return new MemoryStoreFactoryFamily();
    }

    @Override
    public DataStoreOptions getStoreOptions() {
      return options;
    }
  }
}
