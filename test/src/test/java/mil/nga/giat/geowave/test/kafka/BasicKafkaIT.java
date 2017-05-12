package mil.nga.giat.geowave.test.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import mil.nga.giat.geowave.adapter.raster.util.ZipUtils;
import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureBoundingBoxStatistics;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.CountDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.annotation.Environments;
import mil.nga.giat.geowave.test.annotation.Environments.Environment;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

@RunWith(GeoWaveITRunner.class)
@Environments({
	Environment.KAFKA
})
public class BasicKafkaIT
{
	private final static Logger LOGGER = LoggerFactory.getLogger(BasicKafkaIT.class);
	private static final Map<ByteArrayId, Integer> EXPECTED_COUNT_PER_ADAPTER_ID = new HashMap<ByteArrayId, Integer>();

	static {
		EXPECTED_COUNT_PER_ADAPTER_ID.put(
				new ByteArrayId(
						"gpxpoint"),
				11911);
		EXPECTED_COUNT_PER_ADAPTER_ID.put(
				new ByteArrayId(
						"gpxtrack"),
				5);
	}

	protected static final String TEST_DATA_ZIP_RESOURCE_PATH = TestUtils.TEST_RESOURCE_PACKAGE
			+ "mapreduce-testdata.zip";
	protected static final String OSM_GPX_INPUT_DIR = TestUtils.TEST_CASE_BASE + "osm_gpx_test_case/";

	@GeoWaveTestStore({
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStorePluginOptions;

	private static long startMillis;

	@BeforeClass
	public static void extractTestFiles()
			throws URISyntaxException {
		ZipUtils.unZipFile(
				new File(
						BasicKafkaIT.class.getClassLoader().getResource(
								TEST_DATA_ZIP_RESOURCE_PATH).toURI()),
				TestUtils.TEST_CASE_BASE);

		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*         RUNNING BasicKafkaIT          *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTest() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*      FINISHED BasicKafkaIT            *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testBasicIngestGpx()
			throws Exception {
		KafkaTestUtils.testKafkaStage(OSM_GPX_INPUT_DIR);
		KafkaTestUtils.testKafkaIngest(
				dataStorePluginOptions,
				false,
				OSM_GPX_INPUT_DIR);

		final DataStatisticsStore statsStore = dataStorePluginOptions.createDataStatisticsStore();
		final AdapterStore adapterStore = dataStorePluginOptions.createAdapterStore();
		int adapterCount = 0;
		try (CloseableIterator<DataAdapter<?>> adapterIterator = adapterStore.getAdapters()) {
			while (adapterIterator.hasNext()) {
				final FeatureDataAdapter adapter = (FeatureDataAdapter) adapterIterator.next();

				// query by the full bounding box, make sure there is more than
				// 0 count and make sure the count matches the number of results
				final BoundingBoxDataStatistics<?> bboxStat = (BoundingBoxDataStatistics<SimpleFeature>) statsStore
						.getDataStatistics(
								adapter.getAdapterId(),
								FeatureBoundingBoxStatistics.composeId(adapter
										.getFeatureType()
										.getGeometryDescriptor()
										.getLocalName()));
				final CountDataStatistics<?> countStat = (CountDataStatistics<SimpleFeature>) statsStore
						.getDataStatistics(
								adapter.getAdapterId(),
								CountDataStatistics.STATS_TYPE);
				// then query it
				final GeometryFactory factory = new GeometryFactory();
				final Envelope env = new Envelope(
						bboxStat.getMinX(),
						bboxStat.getMaxX(),
						bboxStat.getMinY(),
						bboxStat.getMaxY());
				final Geometry spatialFilter = factory.toGeometry(env);
				final Query query = new SpatialQuery(
						spatialFilter);
				final int resultCount = testQuery(
						adapter,
						query);
				assertTrue(
						"'" + adapter.getAdapterId().getString()
								+ "' adapter must have at least one element in its statistic",
						countStat.getCount() > 0);
				assertEquals(
						"'" + adapter.getAdapterId().getString()
								+ "' adapter should have the same results from a spatial query of '" + env
								+ "' as its total count statistic",
						countStat.getCount(),
						resultCount);
				assertEquals(
						"'" + adapter.getAdapterId().getString()
								+ "' adapter entries ingested does not match expected count",
						EXPECTED_COUNT_PER_ADAPTER_ID.get(adapter.getAdapterId()),
						new Integer(
								resultCount));
				adapterCount++;
			}
		}
		assertTrue(
				"There should be exactly two adapters",
				(adapterCount == 2));
	}

	private int testQuery(
			final DataAdapter<?> adapter,
			final Query query )
			throws Exception {
		final mil.nga.giat.geowave.core.store.DataStore geowaveStore = dataStorePluginOptions.createDataStore();

		final CloseableIterator<?> accumuloResults = geowaveStore.query(
				new QueryOptions(
						adapter,
						TestUtils.DEFAULT_SPATIAL_INDEX),
				query);

		int resultCount = 0;
		while (accumuloResults.hasNext()) {
			accumuloResults.next();

			resultCount++;
		}
		accumuloResults.close();

		return resultCount;

	}
}
