package org.locationtech.geowave.core.store.api;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.store.adapter.MockComponents.MockAbstractDataAdapter;
import org.locationtech.geowave.core.store.index.NullIndex;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import static org.junit.Assert.*;

public class DataStoreTest {

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
    NullIndex index1 = new NullIndex("index1");
    NullIndex index2 = new NullIndex("index2");
    MockAbstractDataAdapter adapter = new MockAbstractDataAdapter(MOCK_DATA_TYPE_1);
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
}
