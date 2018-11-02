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
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.EntryVisibilityHandler;
import org.locationtech.geowave.core.store.adapter.AbstractDataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.NativeFieldHandler;
import org.locationtech.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import org.locationtech.geowave.core.store.adapter.PersistentIndexFieldHandler;
import org.locationtech.geowave.core.store.adapter.statistics.CountDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.adapter.statistics.DefaultFieldStatisticVisibility;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsId;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsProvider;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.StatisticsQueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.base.BaseDataStore;
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
import org.locationtech.geowave.core.store.metadata.DataStatisticsStoreImpl;
import org.locationtech.geowave.core.store.metadata.InternalAdapterStoreImpl;
import org.locationtech.geowave.core.store.query.constraints.DataIdQuery;
import org.locationtech.geowave.datastore.accumulo.cli.config.AccumuloOptions;
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

	InternalAdapterStore internalAdapterStore;

	DataStatisticsStore statsStore;

	AccumuloDataStore mockDataStore;

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

		statsStore = new DataStatisticsStoreImpl(
				accumuloOperations,
				options);

		internalAdapterStore = new InternalAdapterStoreImpl(
				accumuloOperations);

		mockDataStore = new AccumuloDataStore(
				accumuloOperations,
				options);
	}

	public static final VisibilityWriter<TestGeometry> visWriterAAA = new VisibilityWriter<TestGeometry>() {

		@Override
		public FieldVisibilityHandler<TestGeometry, Object> getFieldVisibilityHandler(
				final String fieldId ) {
			return new FieldVisibilityHandler<TestGeometry, Object>() {
				@Override
				public byte[] getVisibility(
						final TestGeometry rowValue,
						final String fieldId,
						final Object fieldValue ) {
					return "aaa".getBytes();
				}

			};
		}

	};

	public static final VisibilityWriter<TestGeometry> visWriterBBB = new VisibilityWriter<TestGeometry>() {

		@Override
		public FieldVisibilityHandler<TestGeometry, Object> getFieldVisibilityHandler(
				final String fieldId ) {
			return new FieldVisibilityHandler<TestGeometry, Object>() {
				@Override
				public byte[] getVisibility(
						final TestGeometry rowValue,
						final String fieldId,
						final Object fieldValue ) {
					return "bbb".getBytes();
				}

			};
		}

	};

	@Test
	public void test()
			throws IOException {
		accumuloOptions.setPersistDataStatistics(true);
		runtest();
	}

	private void runtest()
			throws IOException {

		final Index index = new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions());
		final DataTypeAdapter<TestGeometry> adapter = new TestGeometryAdapter();

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
		ByteArray partitionKey = null;
		mockDataStore.addType(
				adapter,
				index);
		try (Writer<TestGeometry> indexWriter = mockDataStore.createWriter(adapter.getTypeName())) {
			partitionKey = indexWriter.write(
					new TestGeometry(
							factory.createPoint(new Coordinate(
									25,
									32)),
							"test_pt"),
					visWriterAAA).getPartitionKeys().iterator().next().getPartitionKey();
			ByteArray testPartitionKey = indexWriter.write(
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

		try (CloseableIterator<?> it1 = mockDataStore.query(QueryBuilder.newBuilder().addTypeName(
				adapter.getTypeName()).indexName(
				index.getName()).setAuthorizations(
				new String[] {
					"aaa",
					"bbb"
				}).constraints(
				query).build())) {
			int count = 0;
			while (it1.hasNext()) {
				it1.next();
				count++;
			}
			assertEquals(
					3,
					count);
		}

		final short internalAdapterId = internalAdapterStore.getAdapterId(adapter.getTypeName());
		Long count = mockDataStore.aggregateStatistics(StatisticsQueryBuilder.newBuilder().factory().count().dataType(
				adapter.getTypeName()).addAuthorization(
				"aaa").addAuthorization(
				"bbb").build());
		assertEquals(
				3,
				count.longValue());

		count = mockDataStore.aggregateStatistics(StatisticsQueryBuilder.newBuilder().factory().count().dataType(
				adapter.getTypeName()).addAuthorization(
				"aaa").build());
		assertEquals(
				2,
				count.longValue());

		count = mockDataStore.aggregateStatistics(StatisticsQueryBuilder.newBuilder().factory().count().dataType(
				adapter.getTypeName()).addAuthorization(
				"bbb").build());
		assertEquals(
				1,
				count.longValue());

		BoundingBoxDataStatistics<?> bboxStats = (BoundingBoxDataStatistics<?>) statsStore.getDataStatistics(
				internalAdapterId,
				BoundingBoxDataStatistics.STATS_TYPE,
				new String[] {
					"aaa"
				}).next();
		assertTrue((bboxStats.getMinX() == 25) && (bboxStats.getMaxX() == 26) && (bboxStats.getMinY() == 32)
				&& (bboxStats.getMaxY() == 32));

		bboxStats = (BoundingBoxDataStatistics<?>) statsStore.getDataStatistics(
				internalAdapterId,
				BoundingBoxDataStatistics.STATS_TYPE,
				new String[] {
					"bbb"
				}).next();
		assertTrue((bboxStats.getMinX() == 27) && (bboxStats.getMaxX() == 27) && (bboxStats.getMinY() == 32)
				&& (bboxStats.getMaxY() == 32));

		bboxStats = (BoundingBoxDataStatistics<?>) statsStore.getDataStatistics(
				internalAdapterId,
				BoundingBoxDataStatistics.STATS_TYPE,
				new String[] {
					"aaa",
					"bbb"
				}).next();
		assertTrue((bboxStats.getMinX() == 25) && (bboxStats.getMaxX() == 27) && (bboxStats.getMinY() == 32)
				&& (bboxStats.getMaxY() == 32));

		final AtomicBoolean found = new AtomicBoolean(
				false);
		((BaseDataStore) mockDataStore).delete(
				(Query) QueryBuilder.newBuilder().addTypeName(
						adapter.getTypeName()).indexName(
						index.getName()).setAuthorizations(
						new String[] {
							"aaa"
						}).constraints(
						new DataIdQuery(
								new ByteArray(
										"test_pt_2".getBytes(StringUtils.getGeoWaveCharset())))).build(),
				new ScanCallback<TestGeometry, GeoWaveRow>() {

					@Override
					public void entryScanned(
							final TestGeometry entry,
							final GeoWaveRow row ) {
						found.getAndSet(true);
					}
				});
		assertFalse(found.get());

		try (CloseableIterator<?> it1 = mockDataStore.query(QueryBuilder.newBuilder().addTypeName(
				adapter.getTypeName()).indexName(
				index.getName()).setAuthorizations(
				new String[] {
					"aaa",
					"bbb"
				}).constraints(
				query).build())) {
			int c = 0;
			while (it1.hasNext()) {
				it1.next();
				c++;
			}
			assertEquals(
					3,
					c);
		}
		count = mockDataStore.aggregateStatistics(StatisticsQueryBuilder.newBuilder().factory().count().dataType(
				adapter.getTypeName()).addAuthorization(
				"aaa").addAuthorization(
				"bbb").build());
		assertEquals(
				3,
				count.longValue());
		mockDataStore.delete(QueryBuilder.newBuilder().addTypeName(
				adapter.getTypeName()).indexName(
				index.getName()).setAuthorizations(
				new String[] {
					"aaa"
				}).constraints(
				new DataIdQuery(
						new ByteArray(
								"test_pt".getBytes(StringUtils.getGeoWaveCharset())))).build());

		try (CloseableIterator<?> it1 = mockDataStore.query(QueryBuilder.newBuilder().addTypeName(
				adapter.getTypeName()).indexName(
				index.getName()).setAuthorizations(
				new String[] {
					"aaa",
					"bbb"
				}).constraints(
				query).build())) {
			int c = 0;
			while (it1.hasNext()) {
				it1.next();
				c++;
			}
			assertEquals(
					2,
					c);
		}

		count = mockDataStore.aggregateStatistics(StatisticsQueryBuilder.newBuilder().factory().count().dataType(
				adapter.getTypeName()).addAuthorization(
				"aaa").addAuthorization(
				"bbb").build());
		assertEquals(
				2,
				count.longValue());

		count = mockDataStore.aggregateStatistics(StatisticsQueryBuilder.newBuilder().factory().count().dataType(
				adapter.getTypeName()).addAuthorization(
				"aaa").build());
		assertEquals(
				1,
				count.longValue());

		count = mockDataStore.aggregateStatistics(StatisticsQueryBuilder.newBuilder().factory().count().dataType(
				adapter.getTypeName()).addAuthorization(
				"bbb").build());
		assertEquals(
				1,
				count.longValue());

		bboxStats = (BoundingBoxDataStatistics<?>) statsStore.getDataStatistics(
				internalAdapterId,
				BoundingBoxDataStatistics.STATS_TYPE,
				new String[] {
					"aaa"
				}).next();
		assertTrue((bboxStats.getMinX() == 25) && (bboxStats.getMaxX() == 26) && (bboxStats.getMinY() == 32)
				&& (bboxStats.getMaxY() == 32));

		bboxStats = (BoundingBoxDataStatistics<?>) statsStore.getDataStatistics(
				internalAdapterId,
				BoundingBoxDataStatistics.STATS_TYPE,
				new String[] {
					"bbb"
				}).next();
		assertTrue((bboxStats.getMinX() == 27) && (bboxStats.getMaxX() == 27) && (bboxStats.getMinY() == 32)
				&& (bboxStats.getMaxY() == 32));

		bboxStats = (BoundingBoxDataStatistics<?>) statsStore.getDataStatistics(
				internalAdapterId,
				BoundingBoxDataStatistics.STATS_TYPE,
				new String[] {
					"aaa",
					"bbb"
				}).next();
		assertTrue((bboxStats.getMinX() == 25) && (bboxStats.getMaxX() == 27) && (bboxStats.getMinY() == 32)
				&& (bboxStats.getMaxY() == 32));

		found.set(false);

		assertTrue(((BaseDataStore) mockDataStore).delete(
				(Query) QueryBuilder.newBuilder().addTypeName(
						adapter.getTypeName()).indexName(
						index.getName()).setAuthorizations(
						new String[] {
							"aaa"
						}).build(),
				new ScanCallback<TestGeometry, GeoWaveRow>() {

					@Override
					public void entryScanned(
							final TestGeometry entry,
							final GeoWaveRow row ) {
						found.getAndSet(true);
					}
				}));
		assertTrue(((BaseDataStore) mockDataStore).delete(
				(Query) QueryBuilder.newBuilder().addTypeName(
						adapter.getTypeName()).indexName(
						index.getName()).setAuthorizations(
						new String[] {
							"bbb"
						}).build(),
				new ScanCallback<TestGeometry, GeoWaveRow>() {

					@Override
					public void entryScanned(
							final TestGeometry entry,
							final GeoWaveRow row ) {
						found.getAndSet(true);
					}
				}));
		try (CloseableIterator<?> it1 = mockDataStore.query(QueryBuilder.newBuilder().addTypeName(
				adapter.getTypeName()).indexName(
				index.getName()).setAuthorizations(
				new String[] {
					"aaa",
					"bbb"
				}).constraints(
				query).build())) {
			int c = 0;
			while (it1.hasNext()) {
				it1.next();
				c++;
			}
			assertEquals(
					0,
					c);
		}

		assertFalse(statsStore.getDataStatistics(
				internalAdapterId,
				CountDataStatistics.STATS_TYPE,
				new String[] {
					"aaa",
					"bbb"
				}).hasNext());
		mockDataStore.addType(
				adapter,
				index);
		try (Writer<TestGeometry> indexWriter = mockDataStore.createWriter(adapter.getTypeName())) {
			indexWriter.write(new TestGeometry(
					factory.createPoint(new Coordinate(
							25,
							32)),
					"test_pt_2"));
		}
		count = mockDataStore.aggregateStatistics(StatisticsQueryBuilder.newBuilder().factory().count().dataType(
				adapter.getTypeName()).addAuthorization(
				"bbb").build());
		assertEquals(
				1,
				count.longValue());

		final StatisticsId id = StatisticsQueryBuilder.newBuilder().factory().rowHistogram().indexName(
				index.getName()).partition(
				partitionKey).build().getId();
		RowRangeHistogramStatistics<?> histogramStats;
		try (CloseableIterator<InternalDataStatistics<?, ?, ?>> it = statsStore.getDataStatistics(
				internalAdapterId,
				id.getExtendedId(),
				id.getType(),
				new String[] {
					"bbb"
				})) {
			assertTrue(it.hasNext());
			histogramStats = (RowRangeHistogramStatistics<?>) it.next();
			assertTrue(histogramStats != null);
		}

		statsStore.removeAllStatistics(
				internalAdapterId,
				"bbb");
		assertFalse(statsStore.getDataStatistics(
				internalAdapterId,
				CountDataStatistics.STATS_TYPE,
				new String[] {
					"bbb"
				}).hasNext());

		try (CloseableIterator<InternalDataStatistics<?, ?, ?>> it = statsStore.getDataStatistics(
				internalAdapterId,
				id.getExtendedId(),
				id.getType(),
				new String[] {
					"bbb"
				})) {
			assertFalse(it.hasNext());
		}
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

		@SuppressWarnings("unchecked")
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
		public InternalDataStatistics<TestGeometry, ?, ?> createDataStatistics(
				final StatisticsId statisticsId ) {
			if (BoundingBoxDataStatistics.STATS_TYPE.equals(statisticsId.getType())) {
				return new GeoBoundingBoxStatistics();
			}
			else if (CountDataStatistics.STATS_TYPE.equals(statisticsId.getType())) {
				return new CountDataStatistics<>();
			}
			LOGGER.warn("Unrecognized statistics type " + statisticsId.getType().getString()
					+ "; using count statistic");
			return null;
		}

		@Override
		protected RowBuilder newBuilder() {
			return new RowBuilder<TestGeometry, Object>() {
				private String id;
				private Geometry geom;

				@Override
				public TestGeometry buildRow(
						final ByteArray dataId ) {
					return new TestGeometry(
							geom,
							id);
				}

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
			};
		}

		@Override
		public StatisticsId[] getSupportedStatistics() {
			return SUPPORTED_STATS_IDS;
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

		@Override
		public EntryVisibilityHandler<TestGeometry> getVisibilityHandler(
				final CommonIndexModel indexModel,
				final DataTypeAdapter<TestGeometry> adapter,
				final StatisticsId statisticsId ) {
			return GEOMETRY_VISIBILITY_HANDLER;
		}
	}

	private final static StatisticsId[] SUPPORTED_STATS_IDS = new StatisticsId[] {
		BoundingBoxDataStatistics.STATS_TYPE.newBuilder().build().getId(),
		CountDataStatistics.STATS_TYPE.newBuilder().build().getId()
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
