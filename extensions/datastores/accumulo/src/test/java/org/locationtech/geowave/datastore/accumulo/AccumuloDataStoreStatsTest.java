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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.geotime.store.dimension.GeometryWrapper;
import org.locationtech.geowave.core.geotime.store.query.SpatialQuery;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.EntryVisibilityHandler;
import org.locationtech.geowave.core.store.IndexWriter;
import org.locationtech.geowave.core.store.adapter.AbstractDataAdapter;
import org.locationtech.geowave.core.store.adapter.DataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.NativeFieldHandler;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentIndexFieldHandler;
import org.locationtech.geowave.core.store.adapter.WritableDataAdapter;
import org.locationtech.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.CountDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.adapter.statistics.DefaultFieldStatisticVisibility;
import org.locationtech.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsProvider;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.data.PersistentValue;
import org.locationtech.geowave.core.store.data.VisibilityWriter;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldVisibilityHandler;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.index.PrimaryIndex;
import org.locationtech.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import org.locationtech.geowave.core.store.metadata.AdapterStoreImpl;
import org.locationtech.geowave.core.store.metadata.DataStatisticsStoreImpl;
import org.locationtech.geowave.core.store.metadata.IndexStoreImpl;
import org.locationtech.geowave.core.store.metadata.InternalAdapterStoreImpl;
import org.locationtech.geowave.core.store.query.DataIdQuery;
import org.locationtech.geowave.core.store.query.EverythingQuery;
import org.locationtech.geowave.core.store.query.QueryOptions;
import org.locationtech.geowave.datastore.accumulo.AccumuloDataStore;
import org.locationtech.geowave.datastore.accumulo.cli.config.AccumuloOptions;
import org.locationtech.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStore;
import org.locationtech.geowave.datastore.accumulo.operations.AccumuloOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class AccumuloDataStoreStatsTest
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AccumuloDataStoreStatsTest.class);

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
	public void setUp()
			throws AccumuloException,
			AccumuloSecurityException {
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
				options);
		indexStore = new IndexStoreImpl(
				accumuloOperations,
				options);

		adapterStore = new AdapterStoreImpl(
				accumuloOperations,
				options);

		statsStore = new DataStatisticsStoreImpl(
				accumuloOperations,
				options);

		secondaryIndexDataStore = new AccumuloSecondaryIndexDataStore(
				accumuloOperations,
				options);

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

	public static final VisibilityWriter<TestGeometry> visWriterAAA = new VisibilityWriter<TestGeometry>() {

		@Override
		public FieldVisibilityHandler<TestGeometry, Object> getFieldVisibilityHandler(
				final ByteArrayId fieldId ) {
			return new FieldVisibilityHandler<TestGeometry, Object>() {
				@Override
				public byte[] getVisibility(
						final TestGeometry rowValue,
						final ByteArrayId fieldId,
						final Object fieldValue ) {
					return "aaa".getBytes();
				}

			};
		}

	};

	public static final VisibilityWriter<TestGeometry> visWriterBBB = new VisibilityWriter<TestGeometry>() {

		@Override
		public FieldVisibilityHandler<TestGeometry, Object> getFieldVisibilityHandler(
				final ByteArrayId fieldId ) {
			return new FieldVisibilityHandler<TestGeometry, Object>() {
				@Override
				public byte[] getVisibility(
						final TestGeometry rowValue,
						final ByteArrayId fieldId,
						final Object fieldValue ) {
					return "bbb".getBytes();
				}

			};
		}

	};

	@Test
	public void test()
			throws IOException {
		accumuloOptions.setCreateTable(true);
		accumuloOptions.setPersistDataStatistics(true);
		runtest();
	}

	private void runtest()
			throws IOException {

		final PrimaryIndex index = new SpatialDimensionalityTypeProvider().createPrimaryIndex(new SpatialOptions());
		final WritableDataAdapter<TestGeometry> adapter = new TestGeometryAdapter();

		final Geometry testGeoFilter = factory.createPolygon(new Coordinate[] {
			new Coordinate(
					24,
					33),
			new Coordinate(
					28,
					33),
			new Coordinate(
					28,
					31),
			new Coordinate(
					24,
					31),
			new Coordinate(
					24,
					33)
		});
		ByteArrayId partitionKey = null;
		try (IndexWriter<TestGeometry> indexWriter = mockDataStore.createWriter(
				adapter,
				index)) {
			partitionKey = indexWriter.write(
					new TestGeometry(
							factory.createPoint(new Coordinate(
									25,
									32)),
							"test_pt"),
					visWriterAAA).getPartitionKeys().iterator().next().getPartitionKey();
			ByteArrayId testPartitionKey = indexWriter.write(
					new TestGeometry(
							factory.createPoint(new Coordinate(
									26,
									32)),
							"test_pt_1"),
					visWriterAAA).getPartitionKeys().iterator().next().getPartitionKey();
			// they should all be the same partition key, let's just make sure
			Assert.assertEquals(
					"test_pt_1 should have the same partition key as test_pt",
					partitionKey,
					testPartitionKey);
			testPartitionKey = indexWriter.write(
					new TestGeometry(
							factory.createPoint(new Coordinate(
									27,
									32)),
							"test_pt_2"),
					visWriterBBB).getPartitionKeys().iterator().next().getPartitionKey();
			Assert.assertEquals(
					"test_pt_2 should have the same partition key as test_pt",
					partitionKey,
					testPartitionKey);
		}

		final SpatialQuery query = new SpatialQuery(
				testGeoFilter);

		try (CloseableIterator<?> it1 = mockDataStore.query(
				new QueryOptions(
						adapter,
						index,
						-1,
						null,
						new String[] {
							"aaa",
							"bbb"
						}),
				query)) {
			int count = 0;
			while (it1.hasNext()) {
				it1.next();
				count++;
			}
			assertEquals(
					3,
					count);
		}

		final short internalAdapterId = internalAdapterStore.getInternalAdapterId(adapter.getAdapterId());
		CountDataStatistics<?> countStats = (CountDataStatistics<?>) statsStore.getDataStatistics(
				internalAdapterId,
				CountDataStatistics.STATS_TYPE,
				"aaa",
				"bbb");
		assertEquals(
				3,
				countStats.getCount());

		countStats = (CountDataStatistics<?>) statsStore.getDataStatistics(
				internalAdapterId,
				CountDataStatistics.STATS_TYPE,
				"aaa");
		assertEquals(
				2,
				countStats.getCount());

		countStats = (CountDataStatistics<?>) statsStore.getDataStatistics(
				internalAdapterId,
				CountDataStatistics.STATS_TYPE,
				"bbb");
		assertEquals(
				1,
				countStats.getCount());

		BoundingBoxDataStatistics<?> bboxStats = (BoundingBoxDataStatistics<?>) statsStore.getDataStatistics(
				internalAdapterId,
				BoundingBoxDataStatistics.STATS_TYPE,
				"aaa");
		assertTrue((bboxStats.getMinX() == 25) && (bboxStats.getMaxX() == 26) && (bboxStats.getMinY() == 32)
				&& (bboxStats.getMaxY() == 32));

		bboxStats = (BoundingBoxDataStatistics<?>) statsStore.getDataStatistics(
				internalAdapterId,
				BoundingBoxDataStatistics.STATS_TYPE,
				"bbb");
		assertTrue((bboxStats.getMinX() == 27) && (bboxStats.getMaxX() == 27) && (bboxStats.getMinY() == 32)
				&& (bboxStats.getMaxY() == 32));

		bboxStats = (BoundingBoxDataStatistics<?>) statsStore.getDataStatistics(
				internalAdapterId,
				BoundingBoxDataStatistics.STATS_TYPE,
				"aaa",
				"bbb");
		assertTrue((bboxStats.getMinX() == 25) && (bboxStats.getMaxX() == 27) && (bboxStats.getMinY() == 32)
				&& (bboxStats.getMaxY() == 32));

		final AtomicBoolean found = new AtomicBoolean(
				false);
		mockDataStore.delete(
				new QueryOptions(
						adapter,
						index,
						-1,
						new ScanCallback<TestGeometry, GeoWaveRow>() {

							@Override
							public void entryScanned(
									final TestGeometry entry,
									final GeoWaveRow row ) {
								found.getAndSet(true);
							}
						},
						new String[] {
							"aaa"
						}),
				new DataIdQuery(
						new ByteArrayId(
								"test_pt_2".getBytes(StringUtils.getGeoWaveCharset()))));
		assertFalse(found.get());

		try (CloseableIterator<?> it1 = mockDataStore.query(
				new QueryOptions(
						adapter,
						index,
						-1,
						null,
						new String[] {
							"aaa",
							"bbb"
						}),
				query)) {
			int count = 0;
			while (it1.hasNext()) {
				it1.next();
				count++;
			}
			assertEquals(
					3,
					count);
		}

		countStats = (CountDataStatistics<?>) statsStore.getDataStatistics(
				internalAdapterId,
				CountDataStatistics.STATS_TYPE,
				"aaa",
				"bbb");
		assertEquals(
				3,
				countStats.getCount());
		mockDataStore.delete(
				new QueryOptions(
						adapter,
						index,
						-1,
						null,
						new String[] {
							"aaa"
						}),
				new DataIdQuery(
						new ByteArrayId(
								"test_pt".getBytes(StringUtils.getGeoWaveCharset()))));

		try (CloseableIterator<?> it1 = mockDataStore.query(
				new QueryOptions(
						adapter,
						index,
						-1,
						null,
						new String[] {
							"aaa",
							"bbb"
						}),
				query)) {
			int count = 0;
			while (it1.hasNext()) {
				it1.next();
				count++;
			}
			assertEquals(
					2,
					count);
		}

		countStats = (CountDataStatistics<?>) statsStore.getDataStatistics(
				internalAdapterId,
				CountDataStatistics.STATS_TYPE,
				"aaa",
				"bbb");

		assertEquals(
				2,
				countStats.getCount());
		countStats = (CountDataStatistics<?>) statsStore.getDataStatistics(
				internalAdapterId,
				CountDataStatistics.STATS_TYPE,
				"aaa");
		assertEquals(
				1,
				countStats.getCount());
		countStats = (CountDataStatistics<?>) statsStore.getDataStatistics(
				internalAdapterId,
				CountDataStatistics.STATS_TYPE,
				"bbb");
		assertEquals(
				1,
				countStats.getCount());

		bboxStats = (BoundingBoxDataStatistics<?>) statsStore.getDataStatistics(
				internalAdapterId,
				BoundingBoxDataStatistics.STATS_TYPE,
				"aaa");
		assertTrue((bboxStats.getMinX() == 25) && (bboxStats.getMaxX() == 26) && (bboxStats.getMinY() == 32)
				&& (bboxStats.getMaxY() == 32));

		bboxStats = (BoundingBoxDataStatistics<?>) statsStore.getDataStatistics(
				internalAdapterId,
				BoundingBoxDataStatistics.STATS_TYPE,
				"bbb");
		assertTrue((bboxStats.getMinX() == 27) && (bboxStats.getMaxX() == 27) && (bboxStats.getMinY() == 32)
				&& (bboxStats.getMaxY() == 32));

		bboxStats = (BoundingBoxDataStatistics<?>) statsStore.getDataStatistics(
				internalAdapterId,
				BoundingBoxDataStatistics.STATS_TYPE,
				"aaa",
				"bbb");
		assertTrue((bboxStats.getMinX() == 25) && (bboxStats.getMaxX() == 27) && (bboxStats.getMinY() == 32)
				&& (bboxStats.getMaxY() == 32));

		found.set(false);

		assertTrue(mockDataStore.delete(
				new QueryOptions(
						adapter,
						index,
						-1,
						new ScanCallback<TestGeometry, GeoWaveRow>() {

							@Override
							public void entryScanned(
									final TestGeometry entry,
									final GeoWaveRow row ) {
								found.getAndSet(true);
							}
						},
						new String[] {
							"aaa"
						}),
				new EverythingQuery()));
		assertTrue(mockDataStore.delete(
				new QueryOptions(
						adapter,
						index,
						-1,
						new ScanCallback<TestGeometry, GeoWaveRow>() {

							@Override
							public void entryScanned(
									final TestGeometry entry,
									final GeoWaveRow row ) {
								found.getAndSet(true);
							}
						},
						new String[] {
							"bbb"
						}),
				new EverythingQuery()));
		try (CloseableIterator<?> it1 = mockDataStore.query(
				new QueryOptions(
						adapter,
						index,
						-1,
						null,
						new String[] {
							"aaa",
							"bbb"
						}),
				query)) {
			int count = 0;
			while (it1.hasNext()) {
				it1.next();
				count++;
			}
			assertEquals(
					0,
					count);
		}

		countStats = (CountDataStatistics<?>) statsStore.getDataStatistics(
				internalAdapterId,
				CountDataStatistics.STATS_TYPE,
				"aaa",
				"bbb");
		assertNull(countStats);

		try (IndexWriter<TestGeometry> indexWriter = mockDataStore.createWriter(
				adapter,
				index)) {
			indexWriter.write(new TestGeometry(
					factory.createPoint(new Coordinate(
							25,
							32)),
					"test_pt_2"));
		}

		countStats = (CountDataStatistics<?>) statsStore.getDataStatistics(
				internalAdapterId,
				CountDataStatistics.STATS_TYPE,
				"bbb");
		assertTrue(countStats != null);
		assertTrue(countStats.getCount() == 1);

		RowRangeHistogramStatistics<?> histogramStats = (RowRangeHistogramStatistics<?>) statsStore.getDataStatistics(
				internalAdapterId,
				RowRangeHistogramStatistics.composeId(
						index.getId(),
						partitionKey),
				"bbb");

		assertTrue(histogramStats != null);
		statsStore.removeAllStatistics(
				internalAdapterId,
				"bbb");

		countStats = (CountDataStatistics<?>) statsStore.getDataStatistics(
				internalAdapterId,
				CountDataStatistics.STATS_TYPE,
				"bbb");
		assertNull(countStats);

		histogramStats = (RowRangeHistogramStatistics<?>) statsStore.getDataStatistics(
				internalAdapterId,
				RowRangeHistogramStatistics.composeId(
						index.getId(),
						partitionKey),
				"bbb");

		assertNull(histogramStats);
	}

	protected static class TestGeometry
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
			AbstractDataAdapter<TestGeometry> implements
			StatisticsProvider<TestGeometry>
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

			@SuppressWarnings("unchecked")
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

		private final static EntryVisibilityHandler<TestGeometry> GEOMETRY_VISIBILITY_HANDLER = new DefaultFieldStatisticVisibility();
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

		@SuppressWarnings("unchecked")
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
		public DataStatistics<TestGeometry> createDataStatistics(
				final ByteArrayId statisticsId ) {
			if (BoundingBoxDataStatistics.STATS_TYPE.equals(statisticsId)) {
				return new GeoBoundingBoxStatistics();
			}
			else if (CountDataStatistics.STATS_TYPE.equals(statisticsId)) {
				return new CountDataStatistics<>();
			}
			LOGGER.warn("Unrecognized statistics ID " + statisticsId.getString() + " using count statistic");
			return null;
		}

		@Override
		protected RowBuilder newBuilder() {
			return new RowBuilder<TestGeometry, Object>() {
				private String id;
				private Geometry geom;

				@Override
				public TestGeometry buildRow(
						final ByteArrayId dataId ) {
					return new TestGeometry(
							geom,
							id);
				}

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
			};
		}

		@Override
		public ByteArrayId[] getSupportedStatisticsTypes() {
			return SUPPORTED_STATS_IDS;
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
				final PrimaryIndex... indices ) {
			// TODO Auto-generated method stub

		}

		@Override
		public EntryVisibilityHandler<TestGeometry> getVisibilityHandler(
				final CommonIndexModel indexModel,
				final DataAdapter<TestGeometry> adapter,
				final ByteArrayId statisticsId ) {
			return GEOMETRY_VISIBILITY_HANDLER;
		}
	}

	private final static ByteArrayId[] SUPPORTED_STATS_IDS = new ByteArrayId[] {
		BoundingBoxDataStatistics.STATS_TYPE,
		CountDataStatistics.STATS_TYPE
	};

	protected static class GeoBoundingBoxStatistics extends
			BoundingBoxDataStatistics<TestGeometry>
	{
		protected GeoBoundingBoxStatistics() {
			super(
					null);
		}

		@Override
		protected Envelope getEnvelope(
				final TestGeometry entry ) {
			// incorporate the bounding box of the entry's envelope
			final Geometry geometry = entry.geom;
			if ((geometry != null) && !geometry.isEmpty()) {
				return geometry.getEnvelopeInternal();
			}
			return null;
		}
	}
}
