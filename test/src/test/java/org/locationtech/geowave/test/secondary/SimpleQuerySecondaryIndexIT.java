package org.locationtech.geowave.test.secondary;

import java.util.List;
import org.apache.commons.lang3.Range;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.vector.FeatureAttributeDimensionField;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.index.simple.SimpleDoubleIndexStrategy;
import org.locationtech.geowave.core.index.simple.SimpleLongIndexStrategy;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.BasicIndexModel;
import org.locationtech.geowave.core.store.index.CustomNameIndex;
import org.locationtech.geowave.core.store.query.constraints.SimpleNumericQuery;
import org.locationtech.geowave.examples.ingest.SimpleIngest;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.basic.AbstractGeoWaveIT;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
public class SimpleQuerySecondaryIndexIT extends AbstractGeoWaveIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleQuerySecondaryIndexIT.class);

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          // TODO GEOWAVE Issue #1573 prevents deletion from passing on Kudu
          // GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB},
      options = {"enableSecondaryIndexing=true"})
  protected DataStorePluginOptions dataStoreOptions;
  private static long startMillis;
  private static final String testName = "SimpleQuerySecondaryIndexIT";

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStoreOptions;
  }

  @BeforeClass
  public static void startTimer() {
    startMillis = System.currentTimeMillis();
    TestUtils.printStartOfTest(LOGGER, testName);
  }

  @AfterClass
  public static void reportTest() {
    TestUtils.printEndOfTest(LOGGER, testName, startMillis);
  }

  @Test
  public void testMultipleSecondaryIndices() {
    final DataStore ds = dataStoreOptions.createDataStore();
    final SimpleFeatureType sft = SimpleIngest.createPointFeatureType();
    final GeotoolsFeatureDataAdapter fda = SimpleIngest.createDataAdapter(sft);
    final List<SimpleFeature> features =
        SimpleIngest.getGriddedFeatures(new SimpleFeatureBuilder(sft), 1234);
    final Index timeIdx =
        new CustomNameIndex(
            new SimpleLongIndexStrategy(),
            new BasicIndexModel(
                new NumericDimensionField[] {
                    new FeatureAttributeDimensionField(sft.getDescriptor("TimeStamp"))}),
            "TimeStamp_IDX");
    final Index latIdx =
        new CustomNameIndex(
            new SimpleDoubleIndexStrategy(),
            new BasicIndexModel(
                new NumericDimensionField[] {
                    new FeatureAttributeDimensionField(sft.getDescriptor("Latitude"))}),
            "Lat_IDX");
    final Index lonIdx =
        new CustomNameIndex(
            new SimpleDoubleIndexStrategy(),
            new BasicIndexModel(
                new NumericDimensionField[] {
                    new FeatureAttributeDimensionField(sft.getDescriptor("Longitude"))}),
            "Lon_IDX");
    ds.addType(fda, TestUtils.DEFAULT_SPATIAL_INDEX, timeIdx, latIdx, lonIdx);
    int ingestedFeatures = 0;
    try (Writer<SimpleFeature> writer = ds.createWriter(fda.getTypeName())) {
      for (final SimpleFeature feat : features) {
        ingestedFeatures++;
        if ((ingestedFeatures % 5) == 0) {
          // just write 20 percent of the grid
          writer.write(feat);
        }
      }
    }
    try (CloseableIterator<SimpleFeature> it =
        ds.query(
            VectorQueryBuilder.newBuilder().indexName("Lon_IDX").addTypeName(
                sft.getTypeName()).constraints(
                    new SimpleNumericQuery(Range.between((double) 0, (double) 0))).build())) {
      int count = 0;
      while (it.hasNext()) {
        it.next();
        count++;
      }
      Assert.assertTrue(count > 1);
    }
    Assert.assertTrue(
        ds.delete(
            VectorQueryBuilder.newBuilder().indexName("Lon_IDX").addTypeName(
                sft.getTypeName()).constraints(
                    new SimpleNumericQuery(Range.between((double) 0, (double) 0))).build()));
    try (CloseableIterator<SimpleFeature> it =
        ds.query(
            VectorQueryBuilder.newBuilder().indexName("Lon_IDX").addTypeName(
                sft.getTypeName()).constraints(
                    new SimpleNumericQuery(Range.between((double) 0, (double) 0))).build())) {
      int count = 0;
      while (it.hasNext()) {
        it.next();
        count++;
      }
      Assert.assertTrue(count == 0);
    }
    try (CloseableIterator<SimpleFeature> it =
        ds.query(
            VectorQueryBuilder.newBuilder().indexName("Lon_IDX").addTypeName(
                sft.getTypeName()).constraints(
                    new SimpleNumericQuery(Range.between((double) 1, (double) 45))).build())) {
      int count = 0;
      while (it.hasNext()) {
        it.next();
        count++;
      }
      Assert.assertTrue(count > 1);
    }
  }
}
