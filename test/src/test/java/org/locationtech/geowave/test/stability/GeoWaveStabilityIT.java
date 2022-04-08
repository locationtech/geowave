/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.stability;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.InternalGeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.OptimalCQLQuery;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.base.BaseDataStoreUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.locationtech.geowave.core.store.operations.ReaderParamsBuilder;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.TestUtils.DimensionalityType;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.basic.AbstractGeoWaveBasicVectorIT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Iterators;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveStabilityIT extends AbstractGeoWaveBasicVectorIT {
  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.BIGTABLE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB,
          GeoWaveStoreType.FILESYSTEM})
  protected DataStorePluginOptions dataStore;
  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.BIGTABLE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB,
          GeoWaveStoreType.FILESYSTEM},
      namespace = "badDataStore")
  protected DataStorePluginOptions badDataStore;

  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveStabilityIT.class);
  private static long startMillis;
  private static final int NUM_THREADS = 4;

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStore;
  }

  @BeforeClass
  public static void startTimer() {
    startMillis = System.currentTimeMillis();
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*      RUNNING GeoWaveStabilityIT       *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @AfterClass
  public static void reportTest() {
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*      FINISHED GeoWaveStabilityIT      *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @Test
  public void testBadMetadataStability() throws Exception {
    TestUtils.deleteAll(badDataStore);
    TestUtils.testLocalIngest(
        dataStore,
        DimensionalityType.SPATIAL_TEMPORAL,
        HAIL_SHAPEFILE_FILE,
        NUM_THREADS);

    copyBadData(true);

    queryBadData(true);
    queryGoodData();
  }

  @Test
  public void testBadDataStability() throws Exception {
    TestUtils.deleteAll(badDataStore);
    TestUtils.testLocalIngest(
        dataStore,
        DimensionalityType.SPATIAL_TEMPORAL,
        HAIL_SHAPEFILE_FILE,
        NUM_THREADS);

    copyBadData(false);

    queryBadData(false);
    queryGoodData();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void copyBadData(final boolean badMetadata) throws Exception {
    final DataStoreOperations badStoreOperations = badDataStore.createDataStoreOperations();
    final DataStoreOperations storeOperations = dataStore.createDataStoreOperations();
    final PersistentAdapterStore adapterStore = dataStore.createAdapterStore();
    final InternalAdapterStore internalAdapterStore = dataStore.createInternalAdapterStore();
    final AdapterIndexMappingStore indexMappingStore = dataStore.createAdapterIndexMappingStore();
    final IndexStore indexStore = dataStore.createIndexStore();
    for (final MetadataType metadataType : MetadataType.values()) {
      try (MetadataWriter writer = badStoreOperations.createMetadataWriter(metadataType)) {
        final MetadataReader reader = storeOperations.createMetadataReader(metadataType);
        try (CloseableIterator<GeoWaveMetadata> it = reader.query(new MetadataQuery(null, null))) {
          while (it.hasNext()) {
            if (badMetadata) {
              writer.write(new BadGeoWaveMetadata(it.next()));
            } else {
              writer.write(it.next());
            }
          }
        }
      } catch (final Exception e) {
        LOGGER.error("Unable to write metadata on copy", e);
      }
    }
    final InternalDataAdapter<?>[] adapters = adapterStore.getAdapters();
    for (final InternalDataAdapter<?> adapter : adapters) {
      for (final AdapterToIndexMapping indexMapping : indexMappingStore.getIndicesForAdapter(
          adapter.getAdapterId())) {
        final boolean rowMerging = BaseDataStoreUtils.isRowMerging(adapter);
        final Index index = indexMapping.getIndex(indexStore);
        final ReaderParamsBuilder bldr =
            new ReaderParamsBuilder(
                index,
                adapterStore,
                indexMappingStore,
                internalAdapterStore,
                GeoWaveRowIteratorTransformer.NO_OP_TRANSFORMER);
        bldr.adapterIds(new short[] {adapter.getAdapterId()});
        bldr.isClientsideRowMerging(rowMerging);
        try (RowReader<GeoWaveRow> reader = storeOperations.createReader(bldr.build())) {
          try (RowWriter writer = badStoreOperations.createWriter(index, adapter)) {
            while (reader.hasNext()) {
              if (!badMetadata) {
                writer.write(new BadGeoWaveRow(reader.next()));
              } else {
                writer.write(reader.next());
              }
            }
          }
        } catch (final Exception e) {
          LOGGER.error("Unable to write metadata on copy", e);
        }
      }
    }
    try {
      badDataStore.createDataStatisticsStore().mergeStats();
    } catch (final Exception e) {
      LOGGER.info("Caught exception while merging bad stats.");
    }

  }

  private void queryBadData(final boolean badMetadata) throws Exception {
    final PersistentAdapterStore badAdapterStore = badDataStore.createAdapterStore();
    try {
      final InternalDataAdapter<?>[] dataAdapters = badAdapterStore.getAdapters();
      final InternalDataAdapter<?> adapter = dataAdapters[0];
      Assert.assertTrue(adapter instanceof InternalGeotoolsFeatureDataAdapter);
      Assert.assertTrue(adapter.getAdapter() instanceof GeotoolsFeatureDataAdapter);
      final QueryConstraints constraints =
          OptimalCQLQuery.createOptimalQuery(
              "BBOX(geom,-105,28,-87,44) and STATE = 'IL'",
              (InternalGeotoolsFeatureDataAdapter) adapter,
              null,
              null);
      final QueryBuilder<?, ?> bldr = QueryBuilder.newBuilder();

      try (final CloseableIterator<?> actualResults =
          badDataStore.createDataStore().query(bldr.constraints(constraints).build())) {
        final int size = Iterators.size(actualResults);
        LOGGER.error(String.format("Found %d results, expected exception...", size));
        Assert.fail();
      } catch (final Exception e) {
        // Expected exception
      }
    } catch (final Exception e) {
      if (!badMetadata) {
        Assert.fail();
      }
    }
  }

  private void queryGoodData() {
    try {
      final URL[] expectedResultsUrls =
          new URL[] {new File(HAIL_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL()};

      testQuery(
          new File(TEST_BOX_TEMPORAL_FILTER_FILE).toURI().toURL(),
          expectedResultsUrls,
          "bounding box and time range");
    } catch (final Exception e) {
      e.printStackTrace();
      TestUtils.deleteAll(dataStore);
      Assert.fail(
          "Error occurred while testing a bounding box and time range query of spatial temporal index: '"
              + e.getLocalizedMessage()
              + "'");
    }
  }

  private static class BadGeoWaveMetadata extends GeoWaveMetadata {

    public BadGeoWaveMetadata(final GeoWaveMetadata source) {
      super(
          reverse(source.getPrimaryId()),
          reverse(source.getSecondaryId()),
          reverse(source.getVisibility()),
          reverse(source.getValue()));
    }

    private static byte[] reverse(final byte[] source) {
      ArrayUtils.reverse(source);
      return source;
    }

  }

  private static class BadGeoWaveRow implements GeoWaveRow {

    private final GeoWaveRow source;

    public BadGeoWaveRow(final GeoWaveRow source) {
      this.source = source;
    }

    @Override
    public byte[] getDataId() {
      return source.getDataId();
    }

    @Override
    public short getAdapterId() {
      return source.getAdapterId();
    }

    @Override
    public byte[] getSortKey() {
      return source.getSortKey();
    }

    @Override
    public byte[] getPartitionKey() {
      return source.getPartitionKey();
    }

    @Override
    public int getNumberOfDuplicates() {
      return source.getNumberOfDuplicates();
    }

    @Override
    public GeoWaveValue[] getFieldValues() {
      return Arrays.stream(source.getFieldValues()).map(BadGeoWaveValue::new).toArray(
          BadGeoWaveValue[]::new);
    }

    private static class BadGeoWaveValue implements GeoWaveValue {

      private final GeoWaveValue source;
      private final byte[] valueBytes;

      public BadGeoWaveValue(final GeoWaveValue source) {
        this.source = source;
        valueBytes = ArrayUtils.clone(source.getValue());
        ArrayUtils.reverse(valueBytes);
      }

      @Override
      public byte[] getFieldMask() {
        return source.getFieldMask();
      }

      @Override
      public byte[] getVisibility() {
        return source.getVisibility();
      }

      @Override
      public byte[] getValue() {
        return valueBytes;
      }

      @Override
      public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = (prime * result) + Arrays.hashCode(getFieldMask());
        result = (prime * result) + Arrays.hashCode(getValue());
        result = (prime * result) + Arrays.hashCode(getVisibility());
        return result;
      }

      @Override
      public boolean equals(final Object obj) {
        if (this == obj) {
          return true;
        }
        if (obj == null) {
          return false;
        }
        if (getClass() != obj.getClass()) {
          return false;
        }
        final GeoWaveValueImpl other = (GeoWaveValueImpl) obj;
        if (!Arrays.equals(getFieldMask(), other.getFieldMask())) {
          return false;
        }
        if (!Arrays.equals(getValue(), other.getValue())) {
          return false;
        }
        if (!Arrays.equals(getVisibility(), other.getVisibility())) {
          return false;
        }
        return true;
      }

    }

  }

}
