/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.store.memory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.adapter.MockComponents;
import mil.nga.giat.geowave.core.store.adapter.MockComponents.IntegerRangeDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.MockComponents.TestIndexModel;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.core.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

public class MemoryDataStoreTest
{

	@Test
	public void test()
			throws IOException,
			MismatchedIndexToAdapterMapping {
		final PrimaryIndex index = new PrimaryIndex(
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
		final WritableDataAdapter<Integer> adapter = new MockComponents.MockAbstractDataAdapter();

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
						adapter.getAdapterId(),
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
		final PrimaryIndex index1 = new PrimaryIndex(
				new MockComponents.MockIndexStrategy(),
				new MockComponents.TestIndexModel(
						"tm1"));
		final PrimaryIndex index2 = new PrimaryIndex(
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
		final WritableDataAdapter<Integer> adapter = new MockComponents.MockAbstractDataAdapter();

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
						adapter.getAdapterId(),
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
						adapter.getAdapterId(),
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
				final PrimaryIndex index ) {
			return Arrays.asList((QueryFilter) new TestQueryFilter(
					index.getIndexModel(),
					min,
					max));
		}

		@Override
		public boolean isSupported(
				final Index<?, ?> index ) {
			return ((PrimaryIndex) index).getIndexModel() instanceof TestIndexModel;
		}

		@Override
		public List<MultiDimensionalNumericData> getIndexConstraints(
				final PrimaryIndex index ) {
			return Collections.<MultiDimensionalNumericData> singletonList(new BasicNumericDataset(
					new NumericData[] {
						new NumericRange(
								min,
								max)
					}));
		}

	}
}
