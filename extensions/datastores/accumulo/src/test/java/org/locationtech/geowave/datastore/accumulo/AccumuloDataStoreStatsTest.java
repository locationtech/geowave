/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.adapter.SpatialFieldDescriptorBuilder;
import org.locationtech.geowave.core.geotime.index.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialOptions;
import org.locationtech.geowave.core.geotime.store.query.ExplicitSpatialQuery;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic.BoundingBoxValue;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.adapter.FieldDescriptorBuilder;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.base.BaseDataStore;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.data.VisibilityWriter;
import org.locationtech.geowave.core.store.data.field.FieldVisibilityHandler;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.metadata.DataStatisticsStoreImpl;
import org.locationtech.geowave.core.store.metadata.InternalAdapterStoreImpl;
import org.locationtech.geowave.core.store.query.constraints.DataIdQuery;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.DefaultStatisticsProvider;
import org.locationtech.geowave.core.store.statistics.InternalStatisticsHelper;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic.CountValue;
import org.locationtech.geowave.core.store.statistics.index.RowRangeHistogramStatistic.RowRangeHistogramValue;
import org.locationtech.geowave.datastore.accumulo.config.AccumuloOptions;
import org.locationtech.geowave.datastore.accumulo.operations.AccumuloOperations;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Functions;
import com.google.common.collect.Lists;

public class AccumuloDataStoreStatsTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloDataStoreStatsTest.class);

  final AccumuloOptions accumuloOptions = new AccumuloOptions();

  final GeometryFactory factory = new GeometryFactory();

  AccumuloOperations accumuloOperations;

  InternalAdapterStore internalAdapterStore;

  DataStatisticsStore statsStore;

  AccumuloDataStore mockDataStore;

  @Before
  public void setUp() throws AccumuloException, AccumuloSecurityException {
    final MockInstance mockInstance = new MockInstance();
    Connector mockConnector = null;
    try {
      mockConnector = mockInstance.getConnector("root", new PasswordToken(new byte[0]));
    } catch (AccumuloException | AccumuloSecurityException e) {
      LOGGER.error("Failed to create mock accumulo connection", e);
    }
    final AccumuloOptions options = new AccumuloOptions();

    accumuloOperations = new AccumuloOperations(mockConnector, options);

    statsStore = new DataStatisticsStoreImpl(accumuloOperations, options);

    internalAdapterStore = new InternalAdapterStoreImpl(accumuloOperations);

    mockDataStore = new AccumuloDataStore(accumuloOperations, options);
  }

  public static final VisibilityWriter<TestGeometry> visWriterAAA =
      new VisibilityWriter<TestGeometry>() {

        @Override
        public FieldVisibilityHandler<TestGeometry, Object> getFieldVisibilityHandler(
            final String fieldId) {
          return new FieldVisibilityHandler<TestGeometry, Object>() {
            @Override
            public byte[] getVisibility(
                final TestGeometry rowValue,
                final String fieldId,
                final Object fieldValue) {
              return "aaa".getBytes();
            }
          };
        }
      };

  public static final VisibilityWriter<TestGeometry> visWriterBBB =
      new VisibilityWriter<TestGeometry>() {

        @Override
        public FieldVisibilityHandler<TestGeometry, Object> getFieldVisibilityHandler(
            final String fieldId) {
          return new FieldVisibilityHandler<TestGeometry, Object>() {
            @Override
            public byte[] getVisibility(
                final TestGeometry rowValue,
                final String fieldId,
                final Object fieldValue) {
              return "bbb".getBytes();
            }
          };
        }
      };

  @Test
  public void test() throws IOException {
    accumuloOptions.setPersistDataStatistics(true);
    runtest();
  }

  private void runtest() throws IOException {

    final Index index = new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions());
    final DataTypeAdapter<TestGeometry> adapter = new TestGeometryAdapter();

    final Geometry testGeoFilter =
        factory.createPolygon(
            new Coordinate[] {
                new Coordinate(24, 33),
                new Coordinate(28, 33),
                new Coordinate(28, 31),
                new Coordinate(24, 31),
                new Coordinate(24, 33)});
    ByteArray partitionKey = null;
    mockDataStore.addType(adapter, index);
    try (Writer<TestGeometry> indexWriter = mockDataStore.createWriter(adapter.getTypeName())) {
      partitionKey =
          new ByteArray(
              indexWriter.write(
                  new TestGeometry(factory.createPoint(new Coordinate(25, 32)), "test_pt"),
                  visWriterAAA).getInsertionIdsWritten(
                      index.getName()).getPartitionKeys().iterator().next().getPartitionKey());
      ByteArray testPartitionKey =
          new ByteArray(
              indexWriter.write(
                  new TestGeometry(factory.createPoint(new Coordinate(26, 32)), "test_pt_1"),
                  visWriterAAA).getInsertionIdsWritten(
                      index.getName()).getPartitionKeys().iterator().next().getPartitionKey());
      // they should all be the same partition key, let's just make sure
      Assert.assertEquals(
          "test_pt_1 should have the same partition key as test_pt",
          partitionKey,
          testPartitionKey);
      testPartitionKey =
          new ByteArray(
              indexWriter.write(
                  new TestGeometry(factory.createPoint(new Coordinate(27, 32)), "test_pt_2"),
                  visWriterBBB).getInsertionIdsWritten(
                      index.getName()).getPartitionKeys().iterator().next().getPartitionKey());
      Assert.assertEquals(
          "test_pt_2 should have the same partition key as test_pt",
          partitionKey,
          testPartitionKey);
    }

    final ExplicitSpatialQuery query = new ExplicitSpatialQuery(testGeoFilter);

    try (CloseableIterator<?> it1 =
        mockDataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index.getName()).setAuthorizations(new String[] {"aaa", "bbb"}).constraints(
                    query).build())) {
      int count = 0;
      while (it1.hasNext()) {
        it1.next();
        count++;
      }
      assertEquals(3, count);
    }

    final short internalAdapterId = internalAdapterStore.getAdapterId(adapter.getTypeName());

    final CountStatistic countStat =
        (CountStatistic) statsStore.getDataTypeStatistics(
            adapter,
            CountStatistic.STATS_TYPE,
            Statistic.INTERNAL_TAG).next();
    CountValue count = statsStore.getStatisticValue(countStat, "aaa", "bbb");
    assertEquals(3, count.getValue().intValue());

    count = statsStore.getStatisticValue(countStat, "aaa");
    assertEquals(2, count.getValue().intValue());

    count = statsStore.getStatisticValue(countStat, "bbb");
    assertEquals(1, count.getValue().intValue());

    final BoundingBoxStatistic bboxStat =
        (BoundingBoxStatistic) statsStore.getFieldStatistics(
            adapter,
            BoundingBoxStatistic.STATS_TYPE,
            TestGeometryAdapter.GEOM,
            Statistic.INTERNAL_TAG).next();
    BoundingBoxValue bboxStats = statsStore.getStatisticValue(bboxStat, "aaa");
    final double EPSILON = 0.000001;
    assertEquals(25.0, bboxStats.getMinX(), EPSILON);
    assertEquals(26.0, bboxStats.getMaxX(), EPSILON);
    assertEquals(32.0, bboxStats.getMinY(), EPSILON);
    assertEquals(32.0, bboxStats.getMaxY(), EPSILON);

    bboxStats = statsStore.getStatisticValue(bboxStat, "bbb");
    assertEquals(27.0, bboxStats.getMinX(), EPSILON);
    assertEquals(27.0, bboxStats.getMaxX(), EPSILON);
    assertEquals(32.0, bboxStats.getMinY(), EPSILON);
    assertEquals(32.0, bboxStats.getMaxY(), EPSILON);

    bboxStats = statsStore.getStatisticValue(bboxStat, "aaa", "bbb");
    assertEquals(25.0, bboxStats.getMinX(), EPSILON);
    assertEquals(27.0, bboxStats.getMaxX(), EPSILON);
    assertEquals(32.0, bboxStats.getMinY(), EPSILON);
    assertEquals(32.0, bboxStats.getMaxY(), EPSILON);

    final AtomicBoolean found = new AtomicBoolean(false);
    ((BaseDataStore) mockDataStore).delete(
        (Query) QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
            index.getName()).setAuthorizations(new String[] {"aaa"}).constraints(
                new DataIdQuery("test_pt_2".getBytes(StringUtils.getGeoWaveCharset()))).build(),
        new ScanCallback<TestGeometry, GeoWaveRow>() {

          @Override
          public void entryScanned(final TestGeometry entry, final GeoWaveRow row) {
            found.getAndSet(true);
          }
        });
    assertFalse(found.get());

    try (CloseableIterator<?> it1 =
        mockDataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index.getName()).setAuthorizations(new String[] {"aaa", "bbb"}).constraints(
                    query).build())) {
      int c = 0;
      while (it1.hasNext()) {
        it1.next();
        c++;
      }
      assertEquals(3, c);
    }

    count = statsStore.getStatisticValue(countStat, "aaa", "bbb");
    assertEquals(3, count.getValue().intValue());
    mockDataStore.delete(
        QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
            index.getName()).setAuthorizations(new String[] {"aaa"}).constraints(
                new DataIdQuery("test_pt".getBytes(StringUtils.getGeoWaveCharset()))).build());

    try (CloseableIterator<?> it1 =
        mockDataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index.getName()).setAuthorizations(new String[] {"aaa", "bbb"}).constraints(
                    query).build())) {
      int c = 0;
      while (it1.hasNext()) {
        it1.next();
        c++;
      }
      assertEquals(2, c);
    }

    count = statsStore.getStatisticValue(countStat, "aaa", "bbb");
    assertEquals(2, count.getValue().intValue());

    count = statsStore.getStatisticValue(countStat, "aaa");
    assertEquals(1, count.getValue().intValue());

    count = statsStore.getStatisticValue(countStat, "bbb");
    assertEquals(1, count.getValue().intValue());

    bboxStats = statsStore.getStatisticValue(bboxStat, "aaa");
    assertEquals(25.0, bboxStats.getMinX(), EPSILON);
    assertEquals(26.0, bboxStats.getMaxX(), EPSILON);
    assertEquals(32.0, bboxStats.getMinY(), EPSILON);
    assertEquals(32.0, bboxStats.getMaxY(), EPSILON);

    bboxStats = statsStore.getStatisticValue(bboxStat, "bbb");
    assertEquals(27.0, bboxStats.getMinX(), EPSILON);
    assertEquals(27.0, bboxStats.getMaxX(), EPSILON);
    assertEquals(32.0, bboxStats.getMinY(), EPSILON);
    assertEquals(32.0, bboxStats.getMaxY(), EPSILON);

    bboxStats = statsStore.getStatisticValue(bboxStat, "aaa", "bbb");
    assertEquals(25.0, bboxStats.getMinX(), EPSILON);
    assertEquals(27.0, bboxStats.getMaxX(), EPSILON);
    assertEquals(32.0, bboxStats.getMinY(), EPSILON);
    assertEquals(32.0, bboxStats.getMaxY(), EPSILON);

    found.set(false);

    assertTrue(
        ((BaseDataStore) mockDataStore).delete(
            (Query) QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index.getName()).setAuthorizations(new String[] {"aaa"}).build(),
            new ScanCallback<TestGeometry, GeoWaveRow>() {

              @Override
              public void entryScanned(final TestGeometry entry, final GeoWaveRow row) {
                found.getAndSet(true);
              }
            }));
    assertTrue(
        ((BaseDataStore) mockDataStore).delete(
            (Query) QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index.getName()).setAuthorizations(new String[] {"bbb"}).build(),
            new ScanCallback<TestGeometry, GeoWaveRow>() {

              @Override
              public void entryScanned(final TestGeometry entry, final GeoWaveRow row) {
                found.getAndSet(true);
              }
            }));
    try (CloseableIterator<?> it1 =
        mockDataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index.getName()).setAuthorizations(new String[] {"aaa", "bbb"}).constraints(
                    query).build())) {
      int c = 0;
      while (it1.hasNext()) {
        it1.next();
        c++;
      }
      assertEquals(0, c);
    }

    assertNull(statsStore.getStatisticValue(bboxStat, "aaa", "bbb"));
    mockDataStore.addType(adapter, index);
    try (Writer<TestGeometry> indexWriter = mockDataStore.createWriter(adapter.getTypeName())) {
      indexWriter.write(new TestGeometry(factory.createPoint(new Coordinate(25, 32)), "test_pt_2"));
    }

    count = statsStore.getStatisticValue(countStat, "bbb");
    assertEquals(1, count.getValue().intValue());

    RowRangeHistogramValue histogram =
        InternalStatisticsHelper.getRangeStats(
            statsStore,
            index.getName(),
            adapter.getTypeName(),
            partitionKey,
            "bbb");

    assertNotNull(histogram);

    statsStore.removeStatistics(adapter, index);
    assertFalse(
        statsStore.getDataTypeStatistics(
            adapter,
            CountStatistic.STATS_TYPE,
            Statistic.INTERNAL_TAG).hasNext());

    histogram =
        InternalStatisticsHelper.getRangeStats(
            statsStore,
            index.getName(),
            adapter.getTypeName(),
            partitionKey,
            "bbb");

    assertNull(histogram);
  }

  public static class TestGeometry {
    public final Geometry geom;
    public final String id;

    public TestGeometry(final Geometry geom, final String id) {
      this.geom = geom;
      this.id = id;
    }
  }

  public static class TestGeometryAdapter implements
      DataTypeAdapter<TestGeometry>,
      DefaultStatisticsProvider {
    public static final String GEOM = "myGeo";
    public static final String ID = "myId";

    private static final FieldDescriptor<Geometry> GEO_FIELD =
        new SpatialFieldDescriptorBuilder<>(Geometry.class).spatialIndexHint().fieldName(
            GEOM).build();
    private static final FieldDescriptor<String> ID_FIELD =
        new FieldDescriptorBuilder<>(String.class).fieldName(ID).build();
    private static final FieldDescriptor<?>[] DESCRIPTORS =
        new FieldDescriptor[] {GEO_FIELD, ID_FIELD};
    private static final Map<String, FieldDescriptor<?>> DESCRIPTOR_MAP =
        Arrays.stream(DESCRIPTORS).collect(
            Collectors.toMap(FieldDescriptor::fieldName, Functions.identity()));

    public TestGeometryAdapter() {
      super();
    }

    @Override
    public String getTypeName() {
      return "test";
    }

    @Override
    public byte[] getDataId(final TestGeometry entry) {
      return StringUtils.stringToBinary(entry.id);
    }

    @Override
    public RowBuilder<TestGeometry> newRowBuilder(
        final FieldDescriptor<?>[] outputFieldDescriptors) {
      return new RowBuilder<TestGeometry>() {
        private String id;
        private Geometry geom;

        @Override
        public TestGeometry buildRow(final byte[] dataId) {
          return new TestGeometry(geom, id);
        }

        @Override
        public void setField(final String id, final Object fieldValue) {
          if (id.equals(GEOM)) {
            geom = (Geometry) fieldValue;
          } else if (id.equals(ID)) {
            this.id = (String) fieldValue;
          }
        }

        @Override
        public void setFields(final Map<String, Object> values) {
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
    public Object getFieldValue(final TestGeometry entry, final String fieldName) {
      switch (fieldName) {
        case GEOM:
          return entry.geom;
        case ID:
          return entry.id;
      }
      return null;
    }

    @Override
    public Class<TestGeometry> getDataClass() {
      return TestGeometry.class;
    }

    @Override
    public List<Statistic<? extends StatisticValue<?>>> getDefaultStatistics() {
      final List<Statistic<? extends StatisticValue<?>>> statistics = Lists.newArrayList();
      final CountStatistic count = new CountStatistic(getTypeName());
      count.setInternal();
      statistics.add(count);

      final BoundingBoxStatistic bbox = new BoundingBoxStatistic(getTypeName(), GEOM);
      bbox.setInternal();
      statistics.add(bbox);
      return statistics;
    }

    @Override
    public byte[] toBinary() {
      return new byte[0];
    }

    @Override
    public void fromBinary(final byte[] bytes) {}

    @Override
    public FieldDescriptor<?>[] getFieldDescriptors() {
      return DESCRIPTORS;
    }

    @Override
    public FieldDescriptor<?> getFieldDescriptor(final String fieldName) {
      return DESCRIPTOR_MAP.get(fieldName);
    }
  }
}
