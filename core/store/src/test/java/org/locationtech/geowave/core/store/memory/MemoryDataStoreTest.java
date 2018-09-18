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
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.IndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.VisibilityWriter;
import org.locationtech.geowave.core.store.data.field.FieldVisibilityHandler;
import org.locationtech.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.PrimaryIndex;
import org.locationtech.geowave.core.store.query.constraints.DataIdQuery;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
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
		final DataTypeAdapter<Integer> adapter = new MockComponents.MockAbstractDataAdapter();

		final VisibilityWriter<Integer> visWriter = new VisibilityWriter<Integer>() {
			@Override
			public FieldVisibilityHandler<Integer, Object> getFieldVisibilityHandler(
					final String fieldId ) {
				return new GlobalVisibilityHandler(
						"aaa&bbb");
			}
		};
		dataStore.addType(
				adapter,
				index);
		try (final Writer indexWriter = dataStore.createWriter(adapter.getTypeName())) {

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
		try (CloseableIterator<?> itemIt = dataStore.query(QueryBuilder.newBuilder().addTypeName(
				adapter.getTypeName()).indexName(
				index.getName()).addAuthorization(
				"aaa").constraints(
				new TestQuery(
						23,
						26)).build())) {
			assertFalse(itemIt.hasNext());
		}

		try (CloseableIterator<?> itemIt = dataStore.query(QueryBuilder.newBuilder().addTypeName(
				adapter.getTypeName()).indexName(
				index.getName()).addAuthorization(
				"aaa").addAuthorization(
				"bbb").constraints(
				new TestQuery(
						23,
						26)).build())) {
			assertTrue(itemIt.hasNext());
			assertEquals(
					new Integer(
							25),
					itemIt.next());
			assertFalse(itemIt.hasNext());
		}
		try (CloseableIterator<?> itemIt = dataStore.query(QueryBuilder.newBuilder().addTypeName(
				adapter.getTypeName()).indexName(
				index.getName()).addAuthorization(
				"aaa").addAuthorization(
				"bbb").constraints(
				new TestQuery(
						23,
						36)).build())) {
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

		final Iterator<InternalDataStatistics<?, ?, ?>> statsIt = statsStore.getAllDataStatistics();
		assertTrue(checkStats(
				statsIt,
				2,
				new NumericRange(
						25,
						35)));

		dataStore.delete(QueryBuilder.newBuilder().addTypeName(
				adapter.getTypeName()).indexName(
				index.getName()).addAuthorization(
				"aaa").addAuthorization(
				"bbb").constraints(
				new TestQuery(
						23,
						26)).build());
		try (CloseableIterator<?> itemIt = dataStore.query(QueryBuilder.newBuilder().addTypeName(
				adapter.getTypeName()).indexName(
				index.getName()).addAuthorization(
				"aaa").addAuthorization(
				"bbb").constraints(
				new TestQuery(
						23,
						36)).build())) {
			assertTrue(itemIt.hasNext());
			assertEquals(
					new Integer(
							35),
					itemIt.next());
			assertFalse(itemIt.hasNext());
		}
		try (CloseableIterator<?> itemIt = dataStore.query(QueryBuilder.newBuilder().addTypeName(
				adapter.getTypeName()).indexName(
				index.getName()).addAuthorization(
				"aaa").addAuthorization(
				"bbb").constraints(
				new TestQuery(
						23,
						26)).build())) {
			assertFalse(itemIt.hasNext());
		}
		try (CloseableIterator<?> itemIt = dataStore.query(QueryBuilder.newBuilder().addTypeName(
				adapter.getTypeName()).indexName(
				index.getName()).addAuthorization(
				"aaa").addAuthorization(
				"bbb").constraints(
				new DataIdQuery(
						adapter.getDataId(new Integer(
								35)))).build())) {
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
		final DataTypeAdapter<Integer> adapter = new MockComponents.MockAbstractDataAdapter();

		final VisibilityWriter<Integer> visWriter = new VisibilityWriter<Integer>() {
			@Override
			public FieldVisibilityHandler<Integer, Object> getFieldVisibilityHandler(
					final String fieldId ) {
				return new GlobalVisibilityHandler(
						"aaa&bbb");
			}
		};

		dataStore.addType(
				adapter,
				index1,
				index2);
		try (final Writer indexWriter = dataStore.createWriter(adapter.getTypeName())) {

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
		try (CloseableIterator<?> itemIt = dataStore.query(QueryBuilder.newBuilder().addTypeName(
				adapter.getTypeName()).indexName(
				index2.getName()).addAuthorization(
				"aaa").constraints(
				new TestQuery(
						23,
						26)).build())) {
			assertFalse(itemIt.hasNext());
		}

		try (CloseableIterator<?> itemIt = dataStore.query(QueryBuilder.newBuilder().addTypeName(
				adapter.getTypeName()).indexName(
				index1.getName()).addAuthorization(
				"aaa").addAuthorization(
				"bbb").constraints(
				new TestQuery(
						23,
						26)).build())) {
			assertTrue(itemIt.hasNext());
			assertEquals(
					new Integer(
							25),
					itemIt.next());
			assertFalse(itemIt.hasNext());
		}
		// pick an index
		try (CloseableIterator<?> itemIt = dataStore.query(QueryBuilder.newBuilder().addTypeName(
				adapter.getTypeName()).addAuthorization(
				"aaa").addAuthorization(
				"bbb").constraints(
				new TestQuery(
						23,
						36)).build())) {
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

		final Iterator<InternalDataStatistics<?, ?, ?>> statsIt = statsStore.getAllDataStatistics();
		assertTrue(checkStats(
				statsIt,
				2,
				new NumericRange(
						25,
						35)));

		dataStore.delete(QueryBuilder.newBuilder().addTypeName(
				adapter.getTypeName()).addAuthorization(
				"aaa").addAuthorization(
				"bbb").constraints(
				new TestQuery(
						23,
						26)).build());
		try (CloseableIterator<?> itemIt = dataStore.query(QueryBuilder.newBuilder().addTypeName(
				adapter.getTypeName()).indexName(
				index1.getName()).addAuthorization(
				"aaa").addAuthorization(
				"bbb").constraints(
				new TestQuery(
						23,
						36)).build())) {
			assertTrue(itemIt.hasNext());
			assertEquals(
					new Integer(
							35),
					itemIt.next());
			assertFalse(itemIt.hasNext());
		}
		try (CloseableIterator<?> itemIt = dataStore.query(QueryBuilder.newBuilder().addTypeName(
				adapter.getTypeName()).indexName(
				index2.getName()).addAuthorization(
				"aaa").addAuthorization(
				"bbb").constraints(
				new TestQuery(
						23,
						36)).build())) {
			assertTrue(itemIt.hasNext());
			assertEquals(
					new Integer(
							35),
					itemIt.next());
			assertFalse(itemIt.hasNext());
		}
		try (CloseableIterator<?> itemIt = dataStore.query(QueryBuilder.newBuilder().addTypeName(
				adapter.getTypeName()).indexName(
				index1.getName()).addAuthorization(
				"aaa").addAuthorization(
				"bbb").constraints(
				new TestQuery(
						23,
						26)).build())) {
			assertFalse(itemIt.hasNext());
		}
		try (CloseableIterator<?> itemIt = dataStore.query(QueryBuilder.newBuilder().addTypeName(
				adapter.getTypeName()).indexName(
				index2.getName()).addAuthorization(
				"aaa").addAuthorization(
				"bbb").constraints(
				new TestQuery(
						23,
						26)).build())) {
			assertFalse(itemIt.hasNext());
		}
		try (CloseableIterator<?> itemIt = dataStore.query(QueryBuilder.newBuilder().addTypeName(
				adapter.getTypeName()).indexName(
				index1.getName()).addAuthorization(
				"aaa").addAuthorization(
				"bbb").constraints(
				new DataIdQuery(
						adapter.getDataId(new Integer(
								35)))).build())) {
			assertTrue(itemIt.hasNext());
			assertEquals(
					new Integer(
							35),
					itemIt.next());
		}
		try (CloseableIterator<?> itemIt = dataStore.query(QueryBuilder.newBuilder().addTypeName(
				adapter.getTypeName()).indexName(
				index2.getName()).addAuthorization(
				"aaa").addAuthorization(
				"bbb").constraints(
				new DataIdQuery(
						adapter.getDataId(new Integer(
								35)))).build())) {
			assertTrue(itemIt.hasNext());
			assertEquals(
					new Integer(
							35),
					itemIt.next());
		}

	}

	private boolean checkStats(
			final Iterator<InternalDataStatistics<?, ?, ?>> statIt,
			final int count,
			final NumericRange range ) {
		while (statIt.hasNext()) {
			final InternalDataStatistics<Integer, ?, ?> stat = (InternalDataStatistics<Integer, ?, ?>) statIt.next();
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

		@Override
		public byte[] toBinary() {
			return new byte[0];
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {}

	}

	private class TestQuery implements
			QueryConstraints
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

		@Override
		public byte[] toBinary() {
			return new byte[0];
		}

		@Override
		public void fromBinary(
				byte[] bytes ) {

		}

	}
}
