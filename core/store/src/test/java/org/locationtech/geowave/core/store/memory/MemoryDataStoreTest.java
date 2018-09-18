/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.store.memory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.sfc.data.BasicNumericDataset;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.adapter.MockComponents;
import org.locationtech.geowave.core.store.adapter.MockComponents.IntegerRangeDataStatistics;
import org.locationtech.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import org.locationtech.geowave.core.store.adapter.statistics.CountDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.api.DataAdapter;
import org.locationtech.geowave.core.store.api.DataStatistics;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IndexWriter;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.QueryOptions;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.IndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.VisibilityWriter;
import org.locationtech.geowave.core.store.data.field.FieldVisibilityHandler;
import org.locationtech.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.PrimaryIndex;
import org.locationtech.geowave.core.store.query.DataIdQuery;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class MemoryDataStoreTest
{

	@Test
	public void test()
			throws IOException,
			MismatchedIndexToAdapterMapping {
		final Index index = new PrimaryIndex(
				new MockComponents.MockIndexStrategy(),
				new MockComponents.TestIndexModel());
		final String namespace = "test_" + getClass().getName();
		final StoreFactoryFamilySpi storeFamily = new MemoryStoreFactoryFamily();
		final MemoryRequiredOptions reqOptions = new MemoryRequiredOptions();
		reqOptions.setGeowaveNamespace(namespace);
		final DataStore dataStore = storeFamily.getDataStoreFactory().createStore(
				reqOptions);
		final DataStatisticsStore statsStore = storeFamily.getDataStatisticsStoreFactory().createStore(
				reqOptions);
		final DataAdapter<Integer> adapter = new MockComponents.MockAbstractDataAdapter();

		final VisibilityWriter<Integer> visWriter = new VisibilityWriter<Integer>() {
			@Override
			public FieldVisibilityHandler<Integer, Object> getFieldVisibilityHandler(
					final ByteArrayId fieldId ) {
				return new GlobalVisibilityHandler(
						"aaa&bbb");
			}
		};

		try (final IndexWriter indexWriter = dataStore.createWriter(
				adapter,
				index)) {

			indexWriter.write(
					new Integer(
							25),
					visWriter);
			indexWriter.flush();

			indexWriter.write(
					new Integer(
							35),
					visWriter);
			indexWriter.flush();
		}

		// authorization check
		try (CloseableIterator<?> itemIt = dataStore.query(
				new QueryOptions(
						adapter,
						index,
						new String[] {
							"aaa"
						}),
				new TestQuery(
						23,
						26))) {
			assertFalse(itemIt.hasNext());
		}

		try (CloseableIterator<?> itemIt = dataStore.query(
				new QueryOptions(
						adapter,
						index,
						new String[] {
							"aaa",
							"bbb"
						}),
				new TestQuery(
						23,
						26))) {
			assertTrue(itemIt.hasNext());
			assertEquals(
					new Integer(
							25),
					itemIt.next());
			assertFalse(itemIt.hasNext());
		}
		try (CloseableIterator<?> itemIt = dataStore.query(
				new QueryOptions(
						adapter,
						index,
						new String[] {
							"aaa",
							"bbb"
						}),
				new TestQuery(
						23,
						36))) {
			assertTrue(itemIt.hasNext());
			assertEquals(
					new Integer(
							25),
					itemIt.next());
			assertTrue(itemIt.hasNext());
			assertEquals(
					new Integer(
							35),
					itemIt.next());
			assertFalse(itemIt.hasNext());
		}

		final Iterator<DataStatistics<?>> statsIt = statsStore.getAllDataStatistics();
		assertTrue(checkStats(
				statsIt,
				2,
				new NumericRange(
						25,
						35)));

		dataStore.delete(
				new QueryOptions(
						adapter,
						index,
						new String[] {
							"aaa",
							"bbb"
						}),
				new TestQuery(
						23,
						26));
		try (CloseableIterator<?> itemIt = dataStore.query(
				new QueryOptions(
						adapter,
						index,
						new String[] {
							"aaa",
							"bbb"
						}),
				new TestQuery(
						23,
						36))) {
			assertTrue(itemIt.hasNext());
			assertEquals(
					new Integer(
							35),
					itemIt.next());
			assertFalse(itemIt.hasNext());
		}
		try (CloseableIterator<?> itemIt = dataStore.query(
				new QueryOptions(
						adapter,
						index,
						new String[] {
							"aaa",
							"bbb"
						}),
				new TestQuery(
						23,
						26))) {
			assertFalse(itemIt.hasNext());
		}
		try (CloseableIterator<?> itemIt = dataStore.query(
				new QueryOptions(
						adapter,
						index,
						new String[] {
							"aaa",
							"bbb"
						}),
				new DataIdQuery(
						adapter.getDataId(new Integer(
								35))))) {
			assertTrue(itemIt.hasNext());
			assertEquals(
					new Integer(
							35),
					itemIt.next());
		}

	}

	@Test
	public void testMultipleIndices()
			throws IOException,
			MismatchedIndexToAdapterMapping {
		final Index index1 = new PrimaryIndex(
				new MockComponents.MockIndexStrategy(),
				new MockComponents.TestIndexModel(
						"tm1"));
		final Index index2 = new PrimaryIndex(
				new MockComponents.MockIndexStrategy(),
				new MockComponents.TestIndexModel(
						"tm2"));
		final String namespace = "test2_" + getClass().getName();
		final StoreFactoryFamilySpi storeFamily = new MemoryStoreFactoryFamily();
		final MemoryRequiredOptions opts = new MemoryRequiredOptions();
		opts.setGeowaveNamespace(namespace);
		final DataStore dataStore = storeFamily.getDataStoreFactory().createStore(
				opts);
		final DataStatisticsStore statsStore = storeFamily.getDataStatisticsStoreFactory().createStore(
				opts);
		final DataAdapter<Integer> adapter = new MockComponents.MockAbstractDataAdapter();

		final VisibilityWriter<Integer> visWriter = new VisibilityWriter<Integer>() {
			@Override
			public FieldVisibilityHandler<Integer, Object> getFieldVisibilityHandler(
					final ByteArrayId fieldId ) {
				return new GlobalVisibilityHandler(
						"aaa&bbb");
			}
		};

		try (final IndexWriter indexWriter = dataStore.createWriter(
				adapter,
				index1,
				index2)) {

			indexWriter.write(
					new Integer(
							25),
					visWriter);
			indexWriter.flush();

			indexWriter.write(
					new Integer(
							35),
					visWriter);
			indexWriter.flush();
		}

		// authorization check
		try (CloseableIterator<?> itemIt = dataStore.query(
				new QueryOptions(
						adapter,
						index2,
						new String[] {
							"aaa"
						}),
				new TestQuery(
						23,
						26))) {
			assertFalse(itemIt.hasNext());
		}

		try (CloseableIterator<?> itemIt = dataStore.query(
				new QueryOptions(
						adapter,
						index1,
						new String[] {
							"aaa",
							"bbb"
						}),
				new TestQuery(
						23,
						26))) {
			assertTrue(itemIt.hasNext());
			assertEquals(
					new Integer(
							25),
					itemIt.next());
			assertFalse(itemIt.hasNext());
		}
		// pick an index
		try (CloseableIterator<?> itemIt = dataStore.query(
				new QueryOptions(
						adapter,
						new String[] {
							"aaa",
							"bbb"
						}),
				new TestQuery(
						23,
						36))) {
			assertTrue(itemIt.hasNext());
			assertEquals(
					new Integer(
							25),
					itemIt.next());
			assertTrue(itemIt.hasNext());
			assertEquals(
					new Integer(
							35),
					itemIt.next());
			assertFalse(itemIt.hasNext());
		}

		final Iterator<DataStatistics<?>> statsIt = statsStore.getAllDataStatistics();
		assertTrue(checkStats(
				statsIt,
				2,
				new NumericRange(
						25,
						35)));

		dataStore.delete(
				new QueryOptions(
						adapter,
						new String[] {
							"aaa",
							"bbb"
						}),
				new TestQuery(
						23,
						26));
		try (CloseableIterator<?> itemIt = dataStore.query(
				new QueryOptions(
						adapter,
						index1,
						new String[] {
							"aaa",
							"bbb"
						}),
				new TestQuery(
						23,
						36))) {
			assertTrue(itemIt.hasNext());
			assertEquals(
					new Integer(
							35),
					itemIt.next());
			assertFalse(itemIt.hasNext());
		}
		try (CloseableIterator<?> itemIt = dataStore.query(
				new QueryOptions(
						adapter,
						index2,
						new String[] {
							"aaa",
							"bbb"
						}),
				new TestQuery(
						23,
						36))) {
			assertTrue(itemIt.hasNext());
			assertEquals(
					new Integer(
							35),
					itemIt.next());
			assertFalse(itemIt.hasNext());
		}
		try (CloseableIterator<?> itemIt = dataStore.query(
				new QueryOptions(
						adapter,
						index1,
						new String[] {
							"aaa",
							"bbb"
						}),
				new TestQuery(
						23,
						26))) {
			assertFalse(itemIt.hasNext());
		}
		try (CloseableIterator<?> itemIt = dataStore.query(
				new QueryOptions(
						adapter,
						index2,
						new String[] {
							"aaa",
							"bbb"
						}),
				new TestQuery(
						23,
						26))) {
			assertFalse(itemIt.hasNext());
		}
		try (CloseableIterator<?> itemIt = dataStore.query(
				new QueryOptions(
						adapter,
						index1,
						new String[] {
							"aaa",
							"bbb"
						}),
				new DataIdQuery(
						adapter.getDataId(new Integer(
								35))))) {
			assertTrue(itemIt.hasNext());
			assertEquals(
					new Integer(
							35),
					itemIt.next());
		}
		try (CloseableIterator<?> itemIt = dataStore.query(
				new QueryOptions(
						adapter,
						index2,
						new String[] {
							"aaa",
							"bbb"
						}),
				new DataIdQuery(
						adapter.getDataId(new Integer(
								35))))) {
			assertTrue(itemIt.hasNext());
			assertEquals(
					new Integer(
							35),
					itemIt.next());
		}

	}

	private boolean checkStats(
			final Iterator<DataStatistics<?>> statIt,
			final int count,
			final NumericRange range ) {
		while (statIt.hasNext()) {
			final DataStatistics<Integer> stat = (DataStatistics<Integer>) statIt.next();
			if ((stat instanceof CountDataStatistics) && (((CountDataStatistics) stat).getCount() != count)) {
				return false;
			}
			else if ((stat instanceof IntegerRangeDataStatistics)
					&& ((((IntegerRangeDataStatistics) stat).getMin() != range.getMin()) || (((IntegerRangeDataStatistics) stat)
							.getMax() != range.getMax()))) {
				return false;
			}
		}
		return true;
	}

	private class TestQueryFilter implements
			QueryFilter
	{
		final CommonIndexModel indexModel;
		final double min, max;

		public TestQueryFilter(
				final CommonIndexModel indexModel,
				final double min,
				final double max ) {
			super();
			this.indexModel = indexModel;
			this.min = min;
			this.max = max;
		}

		@Override
		public boolean accept(
				final CommonIndexModel indexModel,
				final IndexedPersistenceEncoding<?> persistenceEncoding ) {
			final double min = ((CommonIndexedPersistenceEncoding) persistenceEncoding).getNumericData(
					indexModel.getDimensions()).getDataPerDimension()[0].getMin();
			final double max = ((CommonIndexedPersistenceEncoding) persistenceEncoding).getNumericData(
					indexModel.getDimensions()).getDataPerDimension()[0].getMax();
			return !((this.max <= min) || (this.min > max));
		}

	}

	private class TestQuery implements
			Query
	{

		final double min, max;

		public TestQuery(
				final double min,
				final double max ) {
			super();
			this.min = min;
			this.max = max;
		}

		@Override
		public List<QueryFilter> createFilters(
				final Index index ) {
			return Arrays.asList((QueryFilter) new TestQueryFilter(
					index.getIndexModel(),
					min,
					max));
		}

		@Override
		public List<MultiDimensionalNumericData> getIndexConstraints(
				final Index index ) {
			return Collections.<MultiDimensionalNumericData> singletonList(new BasicNumericDataset(
					new NumericData[] {
						new NumericRange(
								min,
								max)
					}));
		}

	}
}
