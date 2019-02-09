package org.locationtech.geowave.core.store.api;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.MockComponents;
import org.locationtech.geowave.core.store.index.IndexImpl;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;

import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;

public class QueryTestFailed {
    private DataStore dataStore;
    private static final String MOCK_DATA_TYPE_1 = "Some Data Type";
    private static final String MOCK_DATA_TYPE_2 = "Another Data Type";
    final Index index =
            new IndexImpl(
                    new MockComponents.MockIndexStrategy(),
                    new MockComponents.TestIndexModel("index1"));



    final DataTypeAdapter<Integer> adapter =
            new MockComponents.MockAbstractDataAdapter(MOCK_DATA_TYPE_1);


    final DataTypeAdapter<Integer> adapter2 =
            new MockComponents.MockAbstractDataAdapter(MOCK_DATA_TYPE_2);

    @Before
    public void createStore() {
        dataStore = DataStoreFactory.createDataStore(new MemoryRequiredOptions());

    }

    @After
    public void tearDown(){
        dataStore.deleteAll();
    }

    @Test
    public void queryTwoTypesWithDifferentIndex(){
        dataStore.addType(adapter, index);

        //Different Index
        final Index index2 =
                new IndexImpl(
                        new MockComponents.MockIndexStrategy(),
                        new MockComponents.TestIndexModel("index2"));

        dataStore.addType(adapter2, index2);

        Writer<Integer> writer1 = dataStore.createWriter(adapter.getTypeName());
        Writer<Integer> writer2 = dataStore.createWriter(adapter2.getTypeName());
        assertNotNull(writer1);
        assertNotNull(writer2);
        writer1.write(1);
        writer1.close();

        // check
        try (CloseableIterator<?> itemIt =
                     dataStore.query(
                             QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                                     index.getName()).build())) {
            assertEquals(itemIt.next(),1);
            assertFalse(itemIt.hasNext());
        }
        writer2.write(2);
        writer2.close();

        // check
        try (CloseableIterator<?> itemIt2 =
                     dataStore.query(
                             QueryBuilder.newBuilder().addTypeName(adapter2.getTypeName()).indexName(
                                     index2.getName()).build())) {
            assertEquals(itemIt2.next(),2);
            assertFalse(itemIt2.hasNext());
        }

    }

    @Test
    public void queryTwoTypesWithSameIndexFailed(){
        dataStore.addType(adapter, index);
        dataStore.addType(adapter2, index);
        Writer<Integer> writer1 = dataStore.createWriter(adapter.getTypeName());
        Writer<Integer> writer2 = dataStore.createWriter(adapter2.getTypeName());
        assertNotNull(writer1);
        assertNotNull(writer2);
        writer1.write(1);
        writer1.close();

        // check
        try (CloseableIterator<?> itemIt =
                     dataStore.query(
                             QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                                     index.getName()).build())) {
            assertEquals(itemIt.next(),1);
            assertFalse(itemIt.hasNext());
        }
        writer2.write(2);
        writer2.close();

        // check
        try (CloseableIterator<?> itemIt2 =
                     dataStore.query(
                             QueryBuilder.newBuilder().addTypeName(adapter2.getTypeName()).indexName(
                                     index.getName()).build())) {
            assertEquals(itemIt2.next(),2);
            assertFalse(itemIt2.hasNext());
        }

    }

}
