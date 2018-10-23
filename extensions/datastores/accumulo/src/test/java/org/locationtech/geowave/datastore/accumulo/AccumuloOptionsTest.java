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
package org.locationtech.geowave.datastore.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.geotime.store.dimension.GeometryWrapper;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.AbstractDataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.NativeFieldHandler;
import org.locationtech.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentIndexFieldHandler;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.data.PersistentValue;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.metadata.AdapterStoreImpl;
import org.locationtech.geowave.core.store.metadata.IndexStoreImpl;
import org.locationtech.geowave.core.store.metadata.InternalAdapterStoreImpl;
import org.locationtech.geowave.core.store.query.constraints.DataIdQuery;
import org.locationtech.geowave.core.store.query.constraints.EverythingQuery;
import org.locationtech.geowave.core.store.query.constraints.InsertionIdQuery;
import org.locationtech.geowave.datastore.accumulo.cli.config.AccumuloOptions;
import org.locationtech.geowave.datastore.accumulo.operations.AccumuloOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class AccumuloOptionsTest
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AccumuloOptionsTest.class);

	final AccumuloOptions accumuloOptions = new AccumuloOptions();

	final GeometryFactory factory = new GeometryFactory();

	AccumuloOperations accumuloOperations;

	IndexStore indexStore;

	PersistentAdapterStore adapterStore;
	InternalAdapterStore internalAdapterStore;

	AccumuloDataStore mockDataStore;

	@Before
	public void setUp() {
		final MockInstance mockInstance = new MockInstance();
		Connector mockConnector = null;
		try {
			mockConnector = mockInstance.getConnector(
					"root",
					new PasswordToken(
							new byte[0]));
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			LOGGER.error(
					"Failed to create mock accumulo connection",
					e);
		}
		final AccumuloOptions options = new AccumuloOptions();
		accumuloOperations = new AccumuloOperations(
				mockConnector,
				accumuloOptions);

		indexStore = new IndexStoreImpl(
				accumuloOperations,
				accumuloOptions);

		adapterStore = new AdapterStoreImpl(
				accumuloOperations,
				accumuloOptions);

		internalAdapterStore = new InternalAdapterStoreImpl(
				accumuloOperations);

		mockDataStore = new AccumuloDataStore(
				accumuloOperations,
				options);
	}

	@Test
	public void testIndexOptions()
			throws IOException {

		final Index index = new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions());
		final DataTypeAdapter<TestGeometry> adapter = new TestGeometryAdapter();

		mockDataStore.addType(
				adapter,
				index);
		try (Writer<TestGeometry> indexWriter = mockDataStore.createWriter(adapter.getTypeName())) {
			final Pair<ByteArray, ByteArray> rowId2 = indexWriter.write(
					new TestGeometry(
							factory.createPoint(new Coordinate(
									25,
									32)),
							"test_pt_2")).getFirstPartitionAndSortKeyPair();

			final TestGeometry geom2 = (TestGeometry) mockDataStore.query(
					QueryBuilder.newBuilder().addTypeName(
							adapter.getTypeName()).indexName(
							index.getName()).constraints(
							new InsertionIdQuery(
									rowId2.getLeft(),
									rowId2.getRight(),
									new ByteArray(
											"test_pt_2"))).build()).next();

			// as we have chosen to persist the index, we will see the index
			// entry
			// in the index store
			assertEquals(
					true,
					indexStore.indexExists(index.getName()));

			// of course, the point is actually stored in this case
			assertEquals(
					"test_pt_2",
					geom2.id);

		}
	}

	@Test
	public void testLocalityGroups()
			throws IOException {

		final Index index = new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions());
		final DataTypeAdapter<TestGeometry> adapter = new TestGeometryAdapter();

		final String tableName = index.getName();
		final String typeName = adapter.getTypeName();

		accumuloOptions.setUseLocalityGroups(false);
		mockDataStore.addType(
				adapter,
				index);
		try (Writer<TestGeometry> indexWriter = mockDataStore.createWriter(adapter.getTypeName())) {
			final Pair<ByteArray, ByteArray> rowId1 = indexWriter.write(
					new TestGeometry(
							factory.createPoint(new Coordinate(
									25,
									32)),
							"test_pt_1")).getFirstPartitionAndSortKeyPair();

			try {
				// as we are not using locality groups, we expect that this will
				// return false
				assertEquals(
						false,
						accumuloOperations.localityGroupExists(
								tableName,
								typeName));
			}
			catch (final AccumuloException | TableNotFoundException e) {
				LOGGER.error(
						"Locality Group check failed",
						e);
			}

			final TestGeometry geom1 = (TestGeometry) mockDataStore.query(
					QueryBuilder.newBuilder().addTypeName(
							adapter.getTypeName()).indexName(
							index.getName()).constraints(
							new InsertionIdQuery(
									rowId1.getLeft(),
									rowId1.getRight(),
									new ByteArray(
											"test_pt_1"))).build()).next();

			// of course, the point is actually stored in this case
			assertEquals(
					"test_pt_1",
					geom1.id);

		}

		accumuloOptions.setUseLocalityGroups(true);
		mockDataStore.deleteAll();
		mockDataStore.addType(
				adapter,
				index);
		try (Writer<TestGeometry> indexWriter = mockDataStore.createWriter(adapter.getTypeName())) {
			final Pair<ByteArray, ByteArray> rowId2 = indexWriter.write(
					new TestGeometry(
							factory.createPoint(new Coordinate(
									25,
									32)),
							"test_pt_2")).getFirstPartitionAndSortKeyPair();

			try {
				// now that locality groups are turned on, we expect this to
				// return
				// true
				assertEquals(
						true,
						accumuloOperations.localityGroupExists(
								tableName,
								typeName));
			}
			catch (final AccumuloException | TableNotFoundException e) {
				LOGGER.error(
						"Locality Group check failed",
						e);
			}
			final TestGeometry geom2 = (TestGeometry) mockDataStore.query(
					QueryBuilder.newBuilder().addTypeName(
							adapter.getTypeName()).indexName(
							index.getName()).constraints(
							new InsertionIdQuery(
									rowId2.getLeft(),
									rowId2.getRight(),
									new ByteArray(
											"test_pt_2"))).build()).next();

			// of course, the point is actually stored in this case
			assertEquals(
					"test_pt_2",
					geom2.id);

		}

	}

	@Test
	public void testAdapterOptions()
			throws IOException {

		final Index index = new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions());
		final DataTypeAdapter<TestGeometry> adapter = new TestGeometryAdapter();

		mockDataStore.addType(
				adapter,
				index);
		try (Writer<TestGeometry> indexWriter = mockDataStore.createWriter(adapter.getTypeName())) {
			final Pair<ByteArray, ByteArray> rowId2 = indexWriter.write(

					new TestGeometry(
							factory.createPoint(new Coordinate(
									25,
									32)),
							"test_pt_2")).getFirstPartitionAndSortKeyPair();

			try (final CloseableIterator<?> geomItr = mockDataStore.query(QueryBuilder.newBuilder().addTypeName(
					adapter.getTypeName()).indexName(
					index.getName()).constraints(
					new InsertionIdQuery(
							rowId2.getLeft(),
							rowId2.getRight(),
							new ByteArray(
									"test_pt_2"))).build())) {
				assertTrue(geomItr.hasNext());
				final TestGeometry geom2 = (TestGeometry) geomItr.next();

				// specifying the adapter, this method returns the entry
				assertEquals(
						"test_pt_2",
						geom2.id);
			}

			try (final CloseableIterator<TestGeometry> geomItr = (CloseableIterator) mockDataStore.query(QueryBuilder
					.newBuilder()
					.addTypeName(
							adapter.getTypeName())
					.indexName(
							index.getName())
					.build())) {

				while (geomItr.hasNext()) {
					final TestGeometry geom2 = geomItr.next();

					// specifying the adapter, this method returns the entry

					assertTrue(Arrays.asList(
							"test_pt_2",
							"test_pt_1").contains(
							geom2.id));
				}
			}

			final short internalAdapterId = internalAdapterStore.getAdapterId(adapter.getTypeName());
			// the adapter should not exist in the metadata table
			assertEquals(
					true,
					adapterStore.adapterExists(internalAdapterId));

		}

		final short internalAdapterId = internalAdapterStore.getAdapterId(adapter.getTypeName());
		// the adapter should exist in the metadata table
		assertEquals(
				true,
				adapterStore.adapterExists(internalAdapterId));
	}

	@Test
	public void testDeleteAll()
			throws IOException {
		final Index index = new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions());
		final DataTypeAdapter<TestGeometry> adapter0 = new TestGeometryAdapter();
		final DataTypeAdapter<TestGeometry> adapter1 = new AnotherAdapter();

		mockDataStore.addType(
				adapter0,
				index);
		try (Writer<TestGeometry> indexWriter = mockDataStore.createWriter(adapter0.getTypeName())) {
			final Pair<ByteArray, ByteArray> rowId0 = indexWriter.write(
					new TestGeometry(
							factory.createPoint(new Coordinate(
									25,
									32)),
							"test_pt_0")).getFirstPartitionAndSortKeyPair();
		}

		mockDataStore.addType(
				adapter1,
				index);
		try (Writer<TestGeometry> indexWriter = mockDataStore.createWriter(adapter1.getTypeName())) {
			final Pair<ByteArray, ByteArray> rowId0 = indexWriter.write(
					new TestGeometry(
							factory.createPoint(new Coordinate(
									25,
									32)),
							"test_pt_0")).getFirstPartitionAndSortKeyPair();

			final Pair<ByteArray, ByteArray> rowId1 = indexWriter.write(
					new TestGeometry(
							factory.createPoint(new Coordinate(
									25,
									32)),
							"test_pt_1")).getFirstPartitionAndSortKeyPair();
		}

		CloseableIterator it = mockDataStore.query(QueryBuilder.newBuilder().addTypeName(
				adapter0.getTypeName()).indexName(
				index.getName()).constraints(
				new EverythingQuery()).build());
		int count = 0;
		while (it.hasNext()) {
			it.next();
			count++;
		}
		assertEquals(
				1,
				count);

		it = mockDataStore.query(QueryBuilder.newBuilder().addTypeName(
				adapter1.getTypeName()).indexName(
				index.getName()).constraints(
				new EverythingQuery()).build());
		count = 0;
		while (it.hasNext()) {
			it.next();
			count++;
		}
		assertEquals(
				2,
				count);

		it = mockDataStore.query(QueryBuilder.newBuilder().indexName(
				index.getName()).constraints(
				new EverythingQuery()).build());
		count = 0;
		while (it.hasNext()) {
			it.next();
			count++;
		}
		assertEquals(
				3,
				count);

		// delete entry by data id & adapter id

		assertTrue(mockDataStore.delete(QueryBuilder.newBuilder().addTypeName(
				adapter0.getTypeName()).indexName(
				index.getName()).constraints(
				new EverythingQuery()).build()));

		it = mockDataStore.query(QueryBuilder.newBuilder().indexName(
				index.getName()).constraints(
				new EverythingQuery()).build());
		count = 0;
		while (it.hasNext()) {
			it.next();
			count++;
		}
		assertEquals(
				2,
				count);

		it = mockDataStore.query(QueryBuilder.newBuilder().addTypeName(
				adapter0.getTypeName()).indexName(
				index.getName()).constraints(
				new EverythingQuery()).build());
		count = 0;
		while (it.hasNext()) {
			it.next();
			count++;
		}
		assertEquals(
				0,
				count);

		mockDataStore.addType(
				adapter0,
				index);
		try (Writer<TestGeometry> indexWriter = mockDataStore.createWriter(adapter0.getTypeName())) {
			indexWriter.write(
					new TestGeometry(
							factory.createPoint(new Coordinate(
									25,
									32)),
							"test_pt_2")).getFirstPartitionAndSortKeyPair();

		}
		it = mockDataStore.query(QueryBuilder.newBuilder().build());
		count = 0;
		while (it.hasNext()) {
			it.next();
			count++;
		}
		assertEquals(
				3,
				count);

		assertTrue(mockDataStore.delete(QueryBuilder.newBuilder().addTypeName(
				adapter1.getTypeName()).indexName(
				index.getName()).constraints(
				new DataIdQuery(
						new ByteArray(
								"test_pt_1"))).build()));

		it = mockDataStore.query(QueryBuilder.newBuilder().addTypeName(
				adapter1.getTypeName()).indexName(
				index.getName()).constraints(
				new EverythingQuery()).build());
		count = 0;
		while (it.hasNext()) {
			it.next();
			count++;
		}
		assertEquals(
				1,
				count);

		it = mockDataStore.query(QueryBuilder.newBuilder().build());
		count = 0;
		while (it.hasNext()) {
			it.next();
			count++;
		}
		assertEquals(
				2,
				count);

		assertTrue(mockDataStore.delete(QueryBuilder.newBuilder().indexName(
				index.getName()).constraints(
				new EverythingQuery()).build()));

		it = mockDataStore.query(QueryBuilder.newBuilder().indexName(
				index.getName()).constraints(
				new EverythingQuery()).build());
		count = 0;
		while (it.hasNext()) {
			it.next();
			count++;
		}
		assertEquals(
				0,
				count);

	}

	private static class TestGeometry
	{
		private final Geometry geom;
		private final String id;

		public TestGeometry(
				final Geometry geom,
				final String id ) {
			this.geom = geom;
			this.id = id;
		}
	}

	protected static class TestGeometryAdapter extends
			AbstractDataAdapter<TestGeometry>
	{
		private static final String GEOM = "myGeo";
		private static final String ID = "myId";
		private static final PersistentIndexFieldHandler<TestGeometry, ? extends CommonIndexValue, Object> GEOM_FIELD_HANDLER = new PersistentIndexFieldHandler<TestGeometry, CommonIndexValue, Object>() {

			@Override
			public String[] getNativeFieldNames() {
				return new String[] {
					GEOM
				};
			}

			@Override
			public CommonIndexValue toIndexValue(
					final TestGeometry row ) {
				return new GeometryWrapper(
						row.geom,
						new byte[0]);
			}

			@Override
			public PersistentValue<Object>[] toNativeValues(
					final CommonIndexValue indexValue ) {
				return new PersistentValue[] {
					new PersistentValue<Object>(
							GEOM,
							((GeometryWrapper) indexValue).getGeometry())
				};
			}

			@Override
			public byte[] toBinary() {
				return new byte[0];
			}

			@Override
			public void fromBinary(
					final byte[] bytes ) {

			}
		};
		private static final NativeFieldHandler<TestGeometry, Object> ID_FIELD_HANDLER = new NativeFieldHandler<TestGeometry, Object>() {

			@Override
			public String getFieldName() {
				return ID;
			}

			@Override
			public Object getFieldValue(
					final TestGeometry row ) {
				return row.id;
			}

		};

		private static final List<NativeFieldHandler<TestGeometry, Object>> NATIVE_FIELD_HANDLER_LIST = new ArrayList<>();
		private static final List<PersistentIndexFieldHandler<TestGeometry, ? extends CommonIndexValue, Object>> COMMON_FIELD_HANDLER_LIST = new ArrayList<>();

		static {
			COMMON_FIELD_HANDLER_LIST.add(GEOM_FIELD_HANDLER);
			NATIVE_FIELD_HANDLER_LIST.add(ID_FIELD_HANDLER);
		}

		public TestGeometryAdapter() {
			super(
					COMMON_FIELD_HANDLER_LIST,
					NATIVE_FIELD_HANDLER_LIST);
		}

		@Override
		public String getTypeName() {
			return "test";
		}

		@Override
		public ByteArray getDataId(
				final TestGeometry entry ) {
			return new ByteArray(
					entry.id);
		}

		@Override
		public FieldReader getReader(
				final String fieldId ) {
			if (fieldId.equals(GEOM)) {
				return FieldUtils.getDefaultReaderForClass(Geometry.class);
			}
			else if (fieldId.equals(ID)) {
				return FieldUtils.getDefaultReaderForClass(String.class);
			}
			return null;
		}

		@Override
		public FieldWriter getWriter(
				final String fieldId ) {
			if (fieldId.equals(GEOM)) {
				return FieldUtils.getDefaultWriterForClass(Geometry.class);
			}
			else if (fieldId.equals(ID)) {
				return FieldUtils.getDefaultWriterForClass(String.class);
			}
			return null;
		}

		@Override
		protected RowBuilder newBuilder() {
			return new RowBuilder<TestGeometry, Object>() {
				private String id;
				private Geometry geom;

				@Override
				public void setField(
						final String id,
						final Object fieldValue ) {
					if (id.equals(GEOM)) {
						geom = (Geometry) fieldValue;
					}
					else if (id.equals(ID)) {
						this.id = (String) fieldValue;
					}
				}

				@Override
				public void setFields(
						final Map<String, Object> values ) {
					if (values.containsKey(GEOM)) {
						geom = (Geometry) values.get(GEOM);
					}
					if (values.containsKey(ID)) {
						id = (String) values.get(ID);
					}
				}

				@Override
				public TestGeometry buildRow(
						final ByteArray dataId ) {
					return new TestGeometry(
							geom,
							id);
				}
			};
		}

		@Override
		public int getPositionOfOrderedField(
				final CommonIndexModel model,
				final String fieldId ) {
			int i = 0;
			for (final NumericDimensionField<? extends CommonIndexValue> dimensionField : model.getDimensions()) {
				if (fieldId.equals(dimensionField.getFieldName())) {
					return i;
				}
				i++;
			}
			if (fieldId.equals(GEOM)) {
				return i;
			}
			else if (fieldId.equals(ID)) {
				return i + 1;
			}
			return -1;
		}

		@Override
		public String getFieldNameForPosition(
				final CommonIndexModel model,
				final int position ) {
			if (position < model.getDimensions().length) {
				int i = 0;
				for (final NumericDimensionField<? extends CommonIndexValue> dimensionField : model.getDimensions()) {
					if (i == position) {
						return dimensionField.getFieldName();
					}
					i++;
				}
			}
			else {
				final int numDimensions = model.getDimensions().length;
				if (position == numDimensions) {
					return GEOM;
				}
				else if (position == (numDimensions + 1)) {
					return ID;
				}
			}
			return null;
		}
	}

	public static class AnotherAdapter extends
			TestGeometryAdapter
	{
		@Override
		public String getTypeName() {
			return "test1";
		}
	}
}
