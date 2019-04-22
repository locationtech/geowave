package org.locationtech.geowave.test.secondary;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.api.VectorAggregationQueryBuilder;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.geotime.store.query.api.VectorStatisticsQueryBuilder;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.AdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.data.MultiFieldPersistentDataset;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.TestUtils.DimensionalityType;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
public class DataIndexOnlyIT extends AbstractSecondaryIndexIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(BasicSecondaryIndexIT.class);

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB},
      options = {"enableSecondaryIndexing=true"})
  protected DataStorePluginOptions dataStoreOptions;

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB},
      options = {"enableSecondaryIndexing=true"},
      namespace = "BasicSecondaryIndexIT_dataIdxOnly")
  protected DataStorePluginOptions dataIdxOnlyDataStoreOptions;
  private static long startMillis;
  private static final String testName = "DataIndexOnlyIT";

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
  public void testDataIndexOnly() throws Exception {
    TestUtils.testLocalIngest(
        getDataStorePluginOptions(),
        DimensionalityType.SPATIAL,
        HAIL_SHAPEFILE_FILE,
        1);

    final DataStore store = dataStoreOptions.createDataStore();
    final DataStore dataIdxStore = dataIdxOnlyDataStoreOptions.createDataStore();
    final FeatureDataAdapter adapter = (FeatureDataAdapter) store.getTypes()[0];
    dataIdxStore.addType(adapter);
    try (Writer<SimpleFeature> writer = dataIdxStore.createWriter(adapter.getTypeName())) {
      try (CloseableIterator<SimpleFeature> it =
          store.query(VectorQueryBuilder.newBuilder().build())) {
        while (it.hasNext()) {
          writer.write(it.next());
        }
      }
    }
    Long count =
        (Long) dataIdxStore.aggregate(
            VectorAggregationQueryBuilder.newBuilder().count(adapter.getTypeName()).build());
    final Long originalCount =
        (Long) store.aggregate(
            VectorAggregationQueryBuilder.newBuilder().count(adapter.getTypeName()).build());
    Assert.assertTrue(count > 0);
    Assert.assertEquals(originalCount, count);
    final VectorStatisticsQueryBuilder bldr = VectorStatisticsQueryBuilder.newBuilder();
    count = dataIdxStore.aggregateStatistics(bldr.factory().count().build());
    Assert.assertEquals(originalCount, count);
    count = 0L;
    try (CloseableIterator<SimpleFeature> it =
        store.query(VectorQueryBuilder.newBuilder().build())) {
      while (it.hasNext()) {
        it.next();
        count++;
      }
    }
    Assert.assertEquals(originalCount, count);
    TestUtils.deleteAll(dataStoreOptions);
    TestUtils.deleteAll(dataIdxOnlyDataStoreOptions);
  }

  @Test
  public void testDataIndexOnlyOnCustomType() throws Exception {
    final DataStore dataStore = dataIdxOnlyDataStoreOptions.createDataStore();
    final LatLonTimeAdapter adapter = new LatLonTimeAdapter();
    dataStore.addType(adapter);
    try (Writer<LatLonTime> writer = dataStore.createWriter(adapter.getTypeName())) {
      for (int i = 0; i < 10; i++) {
        writer.write(new LatLonTime(i, 100 * i, 0.25f * i, -0.5f * i));
      }
    }

    final Set<Integer> expectedIntIds =
        IntStream.rangeClosed(0, 9).boxed().collect(Collectors.toSet());
    try (CloseableIterator<LatLonTime> it =
        (CloseableIterator) dataStore.query(QueryBuilder.newBuilder().build())) {
      while (it.hasNext()) {
        Assert.assertTrue(expectedIntIds.remove(it.next().getId()));
      }
    }
    Assert.assertTrue(expectedIntIds.isEmpty());
    TestUtils.deleteAll(dataIdxOnlyDataStoreOptions);
  }

  public static class LatLonTime {
    private transient int id;
    private long time;
    private float lat;
    private float lon;

    public LatLonTime() {}

    public LatLonTime(final int id, final long time, final float lat, final float lon) {
      this.id = id;
      this.time = time;
      this.lat = lat;
      this.lon = lon;
    }

    public void setId(final int id) {
      this.id = id;
    }

    public int getId() {
      return id;
    }

    public long getTime() {
      return time;
    }

    public float getLat() {
      return lat;
    }

    public float getLon() {
      return lon;
    }

    public byte[] toBinary() {
      // ID can be set from the adapter so no need to persist
      final ByteBuffer buf = ByteBuffer.allocate(12);
      buf.putInt((int) time);
      buf.putFloat(lat);
      buf.putFloat(lon);
      return buf.array();
    }

    public void fromBinary(final byte[] bytes) {
      final ByteBuffer buf = ByteBuffer.wrap(bytes);
      time = buf.getInt();
      lat = buf.getFloat();
      lon = buf.getFloat();
    }
  }
  public static class LatLonTimeAdapter implements DataTypeAdapter<LatLonTime> {
    private static final FieldReader READER = new LatLonTimeReader();
    private static final FieldWriter WRITER = new LatLonTimeWriter();
    private static final String SINGLETON_FIELD = "LLT";

    @Override
    public FieldReader<Object> getReader(final String fieldName) {
      return READER;
    }

    @Override
    public FieldWriter<LatLonTime, Object> getWriter(final String fieldName) {
      return WRITER;
    }

    @Override
    public byte[] toBinary() {
      return new byte[0];
    }

    @Override
    public void fromBinary(final byte[] bytes) {}

    @Override
    public String getTypeName() {
      return "LLT";
    }

    @Override
    public byte[] getDataId(final LatLonTime entry) {
      return VarintUtils.writeUnsignedInt(entry.getId());
    }

    @Override
    public LatLonTime decode(final IndexedAdapterPersistenceEncoding data, final Index index) {
      final LatLonTime retVal =
          (LatLonTime) data.getAdapterExtendedData().getValue(SINGLETON_FIELD);
      retVal.setId(VarintUtils.readUnsignedInt(ByteBuffer.wrap(data.getDataId())));
      return retVal;
    }

    @Override
    public AdapterPersistenceEncoding encode(
        final LatLonTime entry,
        final CommonIndexModel indexModel) {
      return new AdapterPersistenceEncoding(
          getDataId(entry),
          new MultiFieldPersistentDataset<>(),
          new MultiFieldPersistentDataset<>(Collections.singletonMap(SINGLETON_FIELD, entry)));
    }

    @Override
    public int getPositionOfOrderedField(final CommonIndexModel model, final String fieldName) {
      return 0;
    }

    @Override
    public String getFieldNameForPosition(final CommonIndexModel model, final int position) {
      return "LLT";
    }

    private static class LatLonTimeReader implements FieldReader<LatLonTime> {
      @Override
      public LatLonTime readField(final byte[] fieldData) {
        final LatLonTime retVal = new LatLonTime();
        retVal.fromBinary(fieldData);
        return retVal;
      }
    }
    private static class LatLonTimeWriter implements FieldWriter<LatLonTime, LatLonTime> {
      @Override
      public byte[] writeField(final LatLonTime fieldValue) {
        final byte[] bytes = fieldValue.toBinary();
        return bytes;
      }
    }
  }
}
