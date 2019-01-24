/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.index.dimension.LatitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.LongitudeDefinition;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.geotime.store.query.ExplicitSpatialQuery;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.SFCFactory.SFCType;
import org.locationtech.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.CountDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.base.BaseDataStore;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.IndexImpl;
import org.locationtech.geowave.core.store.metadata.InternalAdapterStoreImpl;
import org.locationtech.geowave.core.store.query.constraints.DataIdQuery;
import org.locationtech.geowave.core.store.query.constraints.InsertionIdQuery;
import org.locationtech.geowave.core.store.query.constraints.PrefixIdQuery;
import org.locationtech.geowave.datastore.accumulo.AccumuloDataStoreStatsTest.TestGeometry;
import org.locationtech.geowave.datastore.accumulo.AccumuloDataStoreStatsTest.TestGeometryAdapter;
import org.locationtech.geowave.datastore.accumulo.cli.config.AccumuloOptions;
import org.locationtech.geowave.datastore.accumulo.minicluster.MiniAccumuloClusterFactory;
import org.locationtech.geowave.datastore.accumulo.operations.AccumuloOperations;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteWriterTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteWriterTest.class);
  private AccumuloOperations operations;
  private DataStore mockDataStore;
  private InsertionIds rowIds1;
  private InsertionIds rowIds3;
  private DataTypeAdapter<AccumuloDataStoreStatsTest.TestGeometry> adapter;
  private DataStatisticsStore statsStore;
  InternalAdapterStore internalAdapterStore;
  protected AccumuloOptions options = new AccumuloOptions();

  private static final CommonIndexModel MODEL =
      new SpatialDimensionalityTypeProvider().createIndex(new SpatialOptions()).getIndexModel();

  private static final NumericDimensionDefinition[] SPATIAL_DIMENSIONS =
      new NumericDimensionDefinition[] {new LongitudeDefinition(), new LatitudeDefinition()};

  private static final NumericIndexStrategy STRATEGY =
      TieredSFCIndexFactory.createSingleTierStrategy(
          SPATIAL_DIMENSIONS,
          new int[] {16, 16},
          SFCType.HILBERT);

  final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");

  private static final Index index = new IndexImpl(STRATEGY, MODEL);

  protected static final String DEFAULT_MINI_ACCUMULO_PASSWORD = "Ge0wave";
  protected static final String HADOOP_WINDOWS_UTIL = "winutils.exe";
  protected static final String HADOOP_DLL = "hadoop.dll";
  // breaks on windows if temp directory isn't on same drive as project
  protected static final File TEMP_DIR = new File("./target/accumulo_temp");
  private static final boolean USE_MOCK = true;
  protected MiniAccumuloClusterImpl miniAccumulo;
  protected String zookeeper;
  // just increment port so there is no potential conflict
  protected static int port = 2181;

  @Before
  public void setUp()
      throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException {
    if (USE_MOCK) {
      Connector mockConnector;
      mockConnector = new MockInstance().getConnector("root", new PasswordToken(new byte[0]));
      operations = new AccumuloOperations(mockConnector, options);
    } else {
      if (TEMP_DIR.exists()) {
        FileUtils.deleteDirectory(TEMP_DIR);
      }
      zookeeper = "localhost:" + port;
      final MiniAccumuloConfigImpl config =
          new MiniAccumuloConfigImpl(TEMP_DIR, DEFAULT_MINI_ACCUMULO_PASSWORD);
      config.setZooKeeperPort(port++);
      config.setNumTservers(2);

      miniAccumulo = MiniAccumuloClusterFactory.newAccumuloCluster(config, DeleteWriterTest.class);

      startMiniAccumulo(config);
      operations =
          new AccumuloOperations(
              miniAccumulo.getConnector("root", new PasswordToken(DEFAULT_MINI_ACCUMULO_PASSWORD)),
              new AccumuloOptions());
    }
    operations.createTable("test_table", true, true);
    mockDataStore = new AccumuloDataStore(operations, options);

    internalAdapterStore = new InternalAdapterStoreImpl(operations);

    statsStore = ((AccumuloDataStore) mockDataStore).getStatisticsStore();

    adapter = new TestGeometryAdapter();
    final GeometryFactory factory = new GeometryFactory();
    mockDataStore.addType(adapter, index);
    try (Writer indexWriter = mockDataStore.createWriter(adapter.getTypeName())) {
      rowIds1 =
          indexWriter.write(
              new AccumuloDataStoreStatsTest.TestGeometry(
                  factory.createLineString(
                      new Coordinate[] {
                          new Coordinate(43.444, 28.232),
                          new Coordinate(43.454, 28.242),
                          new Coordinate(43.444, 28.252),
                          new Coordinate(43.444, 28.232),}),
                  "test_line_1")).getInsertionIdsWritten(index.getName());

      indexWriter.write(
          new AccumuloDataStoreStatsTest.TestGeometry(
              factory.createLineString(
                  new Coordinate[] {
                      new Coordinate(43.444, 28.232),
                      new Coordinate(43.454, 28.242),
                      new Coordinate(43.444, 28.252),
                      new Coordinate(43.444, 28.232),}),
              "test_line_2"));
      rowIds3 =
          indexWriter.write(
              new AccumuloDataStoreStatsTest.TestGeometry(
                  factory.createPoint(new Coordinate(-77.0352, 38.8895)),
                  "test_pt_1")).getInsertionIdsWritten(index.getName());
    }
  }

  @After
  public void tearDown() {
    if (!USE_MOCK) {
      try {
        miniAccumulo.stop();
      } catch (final InterruptedException | IOException e) {
        LOGGER.warn("unable to stop mini accumulo", e);
      }
      if (TEMP_DIR != null) {
        try {
          // sleep because mini accumulo processes still have a
          // hold on the log files and there is no hook to get
          // notified when it is completely stopped

          Thread.sleep(2000);
          FileUtils.deleteDirectory(TEMP_DIR);
        } catch (final IOException | InterruptedException e) {
          LOGGER.warn("unable to delete temp directory", e);
        }
      }
    }
  }

  private void startMiniAccumulo(final MiniAccumuloConfigImpl config)
      throws IOException, InterruptedException {

    final LinkedList<String> jvmArgs = new LinkedList<>();
    jvmArgs.add("-XX:CompressedClassSpaceSize=512m");
    jvmArgs.add("-XX:MaxMetaspaceSize=512m");
    jvmArgs.add("-Xmx512m");

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        tearDown();
      }
    });
    final Map<String, String> siteConfig = config.getSiteConfig();
    siteConfig.put(Property.INSTANCE_ZK_HOST.getKey(), zookeeper);
    config.setSiteConfig(siteConfig);
    miniAccumulo.start();
  }

  @Test
  public void testDeleteByInsertionId() throws IOException {
    final short internalAdapterId = internalAdapterStore.getAdapterId(adapter.getTypeName());
    CountDataStatistics countStats =
        (CountDataStatistics) statsStore.getDataStatistics(
            internalAdapterId,
            CountDataStatistics.STATS_TYPE).next();
    assertEquals(3, countStats.getCount());
    assertTrue(rowIds1.getSize() > 1);

    final Pair<byte[], byte[]> key = rowIds1.getFirstPartitionAndSortKeyPair();
    try (final CloseableIterator it1 =
        mockDataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index.getName()).constraints(
                    new InsertionIdQuery(
                        key.getLeft(),
                        key.getRight(),
                        StringUtils.stringToBinary("test_line_1"))).build())) {
      assertTrue(it1.hasNext());
    }
    assertTrue(
        mockDataStore.delete(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index.getName()).constraints(
                    new DataIdQuery(StringUtils.stringToBinary("test_pt_1"))).build()));
    try (final CloseableIterator it2 =
        mockDataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index.getName()).constraints(
                    new InsertionIdQuery(
                        key.getLeft(),
                        key.getRight(),
                        StringUtils.stringToBinary("test_pt_1"))).build())) {
      assertTrue(!it2.hasNext());
    }
    countStats =
        (CountDataStatistics) statsStore.getDataStatistics(
            internalAdapterId,
            CountDataStatistics.STATS_TYPE).next();
    assertEquals(2, countStats.getCount());
  }

  @Test
  public void testDeleteBySpatialConstraint() throws IOException {
    final short internalAdapterId = internalAdapterStore.getAdapterId(adapter.getTypeName());
    CountDataStatistics countStats =
        (CountDataStatistics) statsStore.getDataStatistics(
            internalAdapterId,
            CountDataStatistics.STATS_TYPE).next();
    assertEquals(3, countStats.getCount());
    final ExplicitSpatialQuery spatialQuery =
        new ExplicitSpatialQuery(new GeometryFactory().toGeometry(new Envelope(-78, -77, 38, 39)));
    try (final CloseableIterator it1 =
        mockDataStore.query(QueryBuilder.newBuilder().constraints(spatialQuery).build())) {
      assertTrue(it1.hasNext());
      assertTrue(
          StringUtils.stringFromBinary(adapter.getDataId((TestGeometry) it1.next())).equals(
              "test_pt_1"));
    }
    assertTrue(mockDataStore.delete(QueryBuilder.newBuilder().constraints(spatialQuery).build()));
    try (final CloseableIterator it2 =
        mockDataStore.query(QueryBuilder.newBuilder().constraints(spatialQuery).build())) {
      assertTrue(!it2.hasNext());
    }
    countStats =
        (CountDataStatistics) statsStore.getDataStatistics(
            internalAdapterId,
            CountDataStatistics.STATS_TYPE).next();
    assertEquals(2, countStats.getCount());
  }

  @Test
  public void testDeleteByPrefixId() throws IOException {
    final short internalAdapterId = internalAdapterStore.getAdapterId(adapter.getTypeName());
    CountDataStatistics countStats =
        (CountDataStatistics) statsStore.getDataStatistics(
            internalAdapterId,
            CountDataStatistics.STATS_TYPE).next();
    assertEquals(3, countStats.getCount());
    final Pair<byte[], byte[]> rowId3 = rowIds3.getFirstPartitionAndSortKeyPair();
    // just take the first half of the row ID as the prefix
    final byte[] rowId3Prefix = Arrays.copyOf(rowId3.getRight(), rowId3.getRight().length / 2);

    final PrefixIdQuery prefixIdQuery = new PrefixIdQuery(rowId3.getLeft(), rowId3Prefix);
    try (final CloseableIterator it1 =
        mockDataStore.query(QueryBuilder.newBuilder().constraints(prefixIdQuery).build())) {
      assertTrue(it1.hasNext());
      assertTrue(
          StringUtils.stringFromBinary(adapter.getDataId((TestGeometry) it1.next())).equals(
              "test_pt_1"));
      assertFalse(it1.hasNext());
    }
    assertTrue(mockDataStore.delete(QueryBuilder.newBuilder().constraints(prefixIdQuery).build()));
    try (final CloseableIterator it2 =
        mockDataStore.query(QueryBuilder.newBuilder().constraints(prefixIdQuery).build())) {
      assertTrue(!it2.hasNext());
    }
    countStats =
        (CountDataStatistics) statsStore.getDataStatistics(
            internalAdapterId,
            CountDataStatistics.STATS_TYPE).next();
    assertEquals(2, countStats.getCount());
  }

  @Test
  public void testDeleteByDataId() throws IOException {
    final short internalAdapterId = internalAdapterStore.getAdapterId(adapter.getTypeName());
    CountDataStatistics countStats =
        (CountDataStatistics) statsStore.getDataStatistics(
            internalAdapterId,
            CountDataStatistics.STATS_TYPE).next();
    assertEquals(3, countStats.getCount());
    assertTrue(rowIds1.getSize() > 1);
    try (final CloseableIterator it1 =
        mockDataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index.getName()).constraints(
                    new DataIdQuery(StringUtils.stringToBinary("test_pt_1"))).build())) {
      assertTrue(it1.hasNext());
    }
    assertTrue(
        ((BaseDataStore) mockDataStore).delete(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index.getName()).constraints(
                    new DataIdQuery(StringUtils.stringToBinary("test_pt_1"))).build()));
    try (final CloseableIterator it2 =
        mockDataStore.query(
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index.getName()).constraints(
                    new DataIdQuery(StringUtils.stringToBinary("test_pt_1"))).build())) {

      assertTrue(!it2.hasNext());
    }
    countStats =
        (CountDataStatistics) statsStore.getDataStatistics(
            internalAdapterId,
            CountDataStatistics.STATS_TYPE).next();

    assertEquals(2, countStats.getCount());
  }
}
