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
package mil.nga.giat.geowave.datastore.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialOptions;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AbstractDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.PersistentIndexFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.AdapterStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.DataStatisticsStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.IndexStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.InternalAdapterStoreImpl;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.EverythingQuery;
import mil.nga.giat.geowave.core.store.query.InsertionIdQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.cli.config.AccumuloOptions;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.accumulo.operations.AccumuloOperations;

public class AccumuloOptionsTest
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AccumuloOptionsTest.class);

	final AccumuloOptions accumuloOptions = new AccumuloOptions();

	final GeometryFactory factory = new GeometryFactory();

	AccumuloOperations accumuloOperations;

	IndexStore indexStore;

	PersistentAdapterStore adapterStore;
	InternalAdapterStore internalAdapterStore;

	DataStatisticsStore statsStore;

	AccumuloDataStore mockDataStore;

	AccumuloSecondaryIndexDataStore secondaryIndexDataStore;

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

		statsStore = new DataStatisticsStoreImpl(
				accumuloOperations,
				accumuloOptions);

		secondaryIndexDataStore = new AccumuloSecondaryIndexDataStore(
				accumuloOperations,
				accumuloOptions);

		internalAdapterStore = new InternalAdapterStoreImpl(
				accumuloOperations);

		mockDataStore = new AccumuloDataStore(
				indexStore,
				adapterStore,
				statsStore,
				secondaryIndexDataStore,
				new AdapterIndexMappingStoreImpl(
						accumuloOperations,
						options),
				accumuloOperations,
				accumuloOptions,
				internalAdapterStore);
	}

	@Test
	public void testIndexOptions()
			throws IOException {

		final PrimaryIndex index = new SpatialDimensionalityTypeProvider().createPrimaryIndex(new SpatialOptions());
		final WritableDataAdapter<TestGeometry> adapter = new TestGeometryAdapter();

		accumuloOptions.setCreateTable(false);
		accumuloOptions.setPersistIndex(false);

		try (IndexWriter<TestGeometry> indexWriter = mockDataStore.createWriter(
				adapter,
				index)) {
			final List<ByteArrayId> rowIds = indexWriter.write(
					new TestGeometry(
							factory.createPoint(new Coordinate(
									25,
									32)),
							"test_pt")).getCompositeInsertionIds();

			// as the table didn't already exist, the flag indicates not to
			// create
			// it, so no rows will be returned
			assertEquals(
					0,
					rowIds.size());

			assertEquals(
					false,
					indexStore.indexExists(index.getId()));

		}

		accumuloOptions.setCreateTable(true);

		try (IndexWriter<TestGeometry> indexWriter = mockDataStore.createWriter(
				adapter,
				index)) {
			final Pair<ByteArrayId, ByteArrayId> rowId1 = indexWriter.write(
					new TestGeometry(
							factory.createPoint(new Coordinate(
									25,
									32)),
							"test_pt_1")).getFirstPartitionAndSortKeyPair();

			assertFalse(mockDataStore.query(
					new QueryOptions(
							adapter,
							(PrimaryIndex) null),
					new InsertionIdQuery(
							rowId1.getLeft(),
							rowId1.getRight(),
							new ByteArrayId(
									"test_pt_1"))).hasNext());

			// as we have chosen not to persist the index, we will not see an
			// index
			// entry in the index store
			assertEquals(
					false,
					indexStore.indexExists(index.getId()));

			/** Still can query providing the index */
			final TestGeometry geom1 = (TestGeometry) mockDataStore.query(
					new QueryOptions(
							adapter,
							index),
					new InsertionIdQuery(
							rowId1.getLeft(),
							rowId1.getRight(),
							new ByteArrayId(
									"test_pt_1"))).next();

			// even though we didn't persist the index, the test point was still
			// stored
			assertEquals(
					"test_pt_1",
					geom1.id);

		}

		accumuloOptions.setPersistIndex(true);

		try (IndexWriter<TestGeometry> indexWriter = mockDataStore.createWriter(
				adapter,
				index)) {
			final Pair<ByteArrayId, ByteArrayId> rowId2 = indexWriter.write(
					new TestGeometry(
							factory.createPoint(new Coordinate(
									25,
									32)),
							"test_pt_2")).getFirstPartitionAndSortKeyPair();

			final TestGeometry geom2 = (TestGeometry) mockDataStore.query(
					new QueryOptions(
							adapter,
							index),
					new InsertionIdQuery(
							rowId2.getLeft(),
							rowId2.getRight(),
							new ByteArrayId(
									"test_pt_2"))).next();

			// as we have chosen to persist the index, we will see the index
			// entry
			// in the index store
			assertEquals(
					true,
					indexStore.indexExists(index.getId()));

			// of course, the point is actually stored in this case
			assertEquals(
					"test_pt_2",
					geom2.id);

		}
	}

	@Test
	public void testLocalityGroups()
			throws IOException {

		final PrimaryIndex index = new SpatialDimensionalityTypeProvider().createPrimaryIndex(new SpatialOptions());
		final WritableDataAdapter<TestGeometry> adapter = new TestGeometryAdapter();

		final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
		final byte[] adapterId = adapter.getAdapterId().getBytes();

		accumuloOptions.setUseLocalityGroups(false);

		try (IndexWriter<TestGeometry> indexWriter = mockDataStore.createWriter(
				adapter,
				index)) {
			final Pair<ByteArrayId, ByteArrayId> rowId1 = indexWriter.write(
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
								adapterId));
			}
			catch (final AccumuloException | TableNotFoundException e) {
				LOGGER.error(
						"Locality Group check failed",
						e);
			}

			final TestGeometry geom1 = (TestGeometry) mockDataStore.query(
					new QueryOptions(
							adapter,
							index),
					new InsertionIdQuery(
							rowId1.getLeft(),
							rowId1.getRight(),
							new ByteArrayId(
									"test_pt_1"))).next();

			// of course, the point is actually stored in this case
			assertEquals(
					"test_pt_1",
					geom1.id);

		}

		accumuloOptions.setUseLocalityGroups(true);

		try (IndexWriter<TestGeometry> indexWriter = mockDataStore.createWriter(
				adapter,
				index)) {
			final Pair<ByteArrayId, ByteArrayId> rowId2 = indexWriter.write(
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
								adapterId));
			}
			catch (final AccumuloException | TableNotFoundException e) {
				LOGGER.error(
						"Locality Group check failed",
						e);
			}
			final TestGeometry geom2 = (TestGeometry) mockDataStore.query(
					new QueryOptions(
							adapter,
							index),
					new InsertionIdQuery(
							rowId2.getLeft(),
							rowId2.getRight(),
							new ByteArrayId(
									"test_pt_2"))).next();

			// of course, the point is actually stored in this case
			assertEquals(
					"test_pt_2",
					geom2.id);

		}

	}

	@Test
	public void testAdapterOptions()
			throws IOException {

		final PrimaryIndex index = new SpatialDimensionalityTypeProvider().createPrimaryIndex(new SpatialOptions());
		final WritableDataAdapter<TestGeometry> adapter = new TestGeometryAdapter();
		accumuloOptions.setPersistAdapter(false);

		try (IndexWriter<TestGeometry> indexWriter = mockDataStore.createWriter(
				adapter,
				index)) {
			final Pair<ByteArrayId, ByteArrayId> rowId1 = indexWriter.write(
					new TestGeometry(
							factory.createPoint(new Coordinate(
									25,
									32)),
							"test_pt_1")).getFirstPartitionAndSortKeyPair();

			assertFalse(mockDataStore.query(
					new QueryOptions(
							adapter.getAdapterId(),
							index.getId()),
					new InsertionIdQuery(
							rowId1.getLeft(),
							rowId1.getRight(),
							new ByteArrayId(
									"test_pt_1"))).hasNext());

		}

		try (IndexWriter<TestGeometry> indexWriter = mockDataStore.createWriter(
				adapter,
				index)) {
			final Pair<ByteArrayId, ByteArrayId> rowId1 = indexWriter.write(
					new TestGeometry(
							factory.createPoint(new Coordinate(
									25,
									32)),
							"test_pt_1")).getFirstPartitionAndSortKeyPair();

			assertFalse(mockDataStore.query(
					new QueryOptions(
							adapter.getAdapterId(),
							index.getId()),
					new InsertionIdQuery(
							rowId1.getLeft(),
							rowId1.getRight(),
							new ByteArrayId(
									"test_pt_1"))).hasNext());

			try (final CloseableIterator<TestGeometry> geomItr = mockDataStore.query(
					new QueryOptions(
							adapter,
							index),
					new EverythingQuery())) {

				final TestGeometry geom1 = geomItr.next();

				// specifying the adapter, this method returns the entry
				assertEquals(
						"test_pt_1",
						geom1.id);

			}

			short internalAdapterId = internalAdapterStore.getInternalAdapterId(adapter.getAdapterId());
			// the adapter should not exist in the metadata table
			assertEquals(
					false,
					adapterStore.adapterExists(internalAdapterId));

		}

		accumuloOptions.setPersistAdapter(true);

		try (IndexWriter<TestGeometry> indexWriter = mockDataStore.createWriter(
				adapter,
				index)) {
			final Pair<ByteArrayId, ByteArrayId> rowId2 = indexWriter.write(

					new TestGeometry(
							factory.createPoint(new Coordinate(
									25,
									32)),
							"test_pt_2")).getFirstPartitionAndSortKeyPair();

			try (final CloseableIterator<?> geomItr = mockDataStore.query(
					new QueryOptions(
							adapter.getAdapterId(),
							index.getId()),
					new InsertionIdQuery(
							rowId2.getLeft(),
							rowId2.getRight(),
							new ByteArrayId(
									"test_pt_2")))) {
				assertTrue(geomItr.hasNext());
				final TestGeometry geom2 = (TestGeometry) geomItr.next();

				// specifying the adapter, this method returns the entry
				assertEquals(
						"test_pt_2",
						geom2.id);
			}

			try (final CloseableIterator<TestGeometry> geomItr = mockDataStore.query(
					new QueryOptions(
							adapter.getAdapterId(),
							index.getId()),
					null)) {

				while (geomItr.hasNext()) {
					final TestGeometry geom2 = geomItr.next();

					// specifying the adapter, this method returns the entry

					assertTrue(Arrays.asList(
							"test_pt_2",
							"test_pt_1").contains(
							geom2.id));
				}
			}

			short internalAdapterId = internalAdapterStore.getInternalAdapterId(adapter.getAdapterId());
			// the adapter should not exist in the metadata table
			assertEquals(
					true,
					adapterStore.adapterExists(internalAdapterId));

		}

		short internalAdapterId = internalAdapterStore.getInternalAdapterId(adapter.getAdapterId());
		// the adapter should exist in the metadata table
		assertEquals(
				true,
				adapterStore.adapterExists(internalAdapterId));
	}

	@Test
	public void testDeleteAll()
			throws IOException {
		final PrimaryIndex index = new SpatialDimensionalityTypeProvider().createPrimaryIndex(new SpatialOptions());
		final WritableDataAdapter<TestGeometry> adapter0 = new TestGeometryAdapter();
		final WritableDataAdapter<TestGeometry> adapter1 = new AnotherAdapter();

		accumuloOptions.setUseAltIndex(true);

		try (IndexWriter<TestGeometry> indexWriter = mockDataStore.createWriter(
				adapter0,
				index)) {
			final Pair<ByteArrayId, ByteArrayId> rowId0 = indexWriter.write(
					new TestGeometry(
							factory.createPoint(new Coordinate(
									25,
									32)),
							"test_pt_0")).getFirstPartitionAndSortKeyPair();
		}
		try (IndexWriter<TestGeometry> indexWriter = mockDataStore.createWriter(
				adapter1,
				index)) {
			final Pair<ByteArrayId, ByteArrayId> rowId0 = indexWriter.write(
					new TestGeometry(
							factory.createPoint(new Coordinate(
									25,
									32)),
							"test_pt_0")).getFirstPartitionAndSortKeyPair();

			final Pair<ByteArrayId, ByteArrayId> rowId1 = indexWriter.write(
					new TestGeometry(
							factory.createPoint(new Coordinate(
									25,
									32)),
							"test_pt_1")).getFirstPartitionAndSortKeyPair();
		}

		CloseableIterator it = mockDataStore.query(
				new QueryOptions(
						adapter0,
						index),
				new EverythingQuery());
		int count = 0;
		while (it.hasNext()) {
			it.next();
			count++;
		}
		assertEquals(
				1,
				count);

		it = mockDataStore.query(
				new QueryOptions(
						adapter1,
						index),
				new EverythingQuery());
		count = 0;
		while (it.hasNext()) {
			it.next();
			count++;
		}
		assertEquals(
				2,
				count);

		it = mockDataStore.query(
				new QueryOptions(
						index),
				new EverythingQuery());
		count = 0;
		while (it.hasNext()) {
			it.next();
			count++;
		}
		assertEquals(
				3,
				count);

		// delete entry by data id & adapter id

		assertTrue(mockDataStore.delete(
				new QueryOptions(
						adapter0,
						index),
				new EverythingQuery()));

		it = mockDataStore.query(
				new QueryOptions(
						index),
				new EverythingQuery());
		count = 0;
		while (it.hasNext()) {
			it.next();
			count++;
		}
		assertEquals(
				2,
				count);

		it = mockDataStore.query(
				new QueryOptions(
						adapter0,
						index),
				new EverythingQuery());
		count = 0;
		while (it.hasNext()) {
			it.next();
			count++;
		}
		assertEquals(
				0,
				count);

		try (IndexWriter<TestGeometry> indexWriter = mockDataStore.createWriter(
				adapter0,
				index)) {
			indexWriter.write(
					new TestGeometry(
							factory.createPoint(new Coordinate(
									25,
									32)),
							"test_pt_2")).getFirstPartitionAndSortKeyPair();

		}
		it = mockDataStore.query(
				new QueryOptions(),
				new EverythingQuery());
		count = 0;
		while (it.hasNext()) {
			it.next();
			count++;
		}
		assertEquals(
				3,
				count);

		assertTrue(mockDataStore.delete(
				new QueryOptions(
						adapter1,
						index),
				new DataIdQuery(
						new ByteArrayId(
								"test_pt_1"))));

		it = mockDataStore.query(
				new QueryOptions(
						adapter1,
						index),
				new EverythingQuery());
		count = 0;
		while (it.hasNext()) {
			it.next();
			count++;
		}
		assertEquals(
				1,
				count);

		it = mockDataStore.query(
				new QueryOptions(),
				new EverythingQuery());
		count = 0;
		while (it.hasNext()) {
			it.next();
			count++;
		}
		assertEquals(
				2,
				count);

		assertTrue(mockDataStore.delete(
				new QueryOptions(
						index),
				new EverythingQuery()));

		it = mockDataStore.query(
				new QueryOptions(
						index),
				new EverythingQuery());
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
		private static final ByteArrayId GEOM = new ByteArrayId(
				"myGeo");
		private static final ByteArrayId ID = new ByteArrayId(
				"myId");
		private static final PersistentIndexFieldHandler<TestGeometry, ? extends CommonIndexValue, Object> GEOM_FIELD_HANDLER = new PersistentIndexFieldHandler<TestGeometry, CommonIndexValue, Object>() {

			@Override
			public ByteArrayId[] getNativeFieldIds() {
				return new ByteArrayId[] {
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
			public ByteArrayId getFieldId() {
				return ID;
			}

			@Override
			public Object getFieldValue(
					final TestGeometry row ) {
				return row.id;
			}

		};

		private static final List<NativeFieldHandler<TestGeometry, Object>> NATIVE_FIELD_HANDLER_LIST = new ArrayList<NativeFieldHandler<TestGeometry, Object>>();
		private static final List<PersistentIndexFieldHandler<TestGeometry, ? extends CommonIndexValue, Object>> COMMON_FIELD_HANDLER_LIST = new ArrayList<PersistentIndexFieldHandler<TestGeometry, ? extends CommonIndexValue, Object>>();

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
		public ByteArrayId getAdapterId() {
			return new ByteArrayId(
					"test");
		}

		@Override
		public boolean isSupported(
				final TestGeometry entry ) {
			return true;
		}

		@Override
		public ByteArrayId getDataId(
				final TestGeometry entry ) {
			return new ByteArrayId(
					entry.id);
		}

		@Override
		public FieldReader getReader(
				final ByteArrayId fieldId ) {
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
				final ByteArrayId fieldId ) {
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
						ByteArrayId id,
						Object fieldValue ) {
					if (id.equals(GEOM)) {
						geom = (Geometry) fieldValue;
					}
					else if (id.equals(ID)) {
						this.id = (String) fieldValue;
					}
				}

				@Override
				public void setFields(
						Map<ByteArrayId, Object> values ) {
					if (values.containsKey(GEOM)) {
						geom = (Geometry) values.get(GEOM);
					}
					if (values.containsKey(ID)) {
						this.id = (String) values.get(ID);
					}
				}

				@Override
				public TestGeometry buildRow(
						final ByteArrayId dataId ) {
					return new TestGeometry(
							geom,
							id);
				}
			};
		}

		@Override
		public int getPositionOfOrderedField(
				final CommonIndexModel model,
				final ByteArrayId fieldId ) {
			int i = 0;
			for (final NumericDimensionField<? extends CommonIndexValue> dimensionField : model.getDimensions()) {
				if (fieldId.equals(dimensionField.getFieldId())) {
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
		public ByteArrayId getFieldIdForPosition(
				final CommonIndexModel model,
				final int position ) {
			if (position < model.getDimensions().length) {
				int i = 0;
				for (final NumericDimensionField<? extends CommonIndexValue> dimensionField : model.getDimensions()) {
					if (i == position) {
						return dimensionField.getFieldId();
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

		@Override
		public void init(
				PrimaryIndex... indices ) {
			// TODO Auto-generated method stub

		}
	}

	public static class AnotherAdapter extends
			TestGeometryAdapter
	{
		@Override
		public ByteArrayId getAdapterId() {
			return new ByteArrayId(
					"test1");
		}
	}
}
