package mil.nga.giat.geowave.test.basic;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.export.VectorLocalExportCommand;
import mil.nga.giat.geowave.adapter.vector.export.VectorLocalExportOptions;
import mil.nga.giat.geowave.adapter.vector.utils.TimeDescriptors;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.TestUtils.DimensionalityType;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveBasicSpatialTemporalVectorIT extends
		AbstractGeoWaveBasicVectorIT
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveBasicSpatialTemporalVectorIT.class);

	private static final String HAIL_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE
			+ "hail-box-temporal-filter.shp";
	private static final String HAIL_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE
			+ "hail-polygon-temporal-filter.shp";
	private static final String TORNADO_TRACKS_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE
			+ "tornado_tracks-box-temporal-filter.shp";
	private static final String TORNADO_TRACKS_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE
			+ "tornado_tracks-polygon-temporal-filter.shp";
	private static final String TEST_BOX_TEMPORAL_FILTER_FILE = TEST_FILTER_PACKAGE + "Box-Temporal-Filter.shp";
	private static final String TEST_POLYGON_TEMPORAL_FILTER_FILE = TEST_FILTER_PACKAGE + "Polygon-Temporal-Filter.shp";
	private static final String TEST_EXPORT_DIRECTORY = "export";
	private static final String TEST_BASE_EXPORT_FILE_NAME = "basicIT-export.avro";

	private static final SimpleDateFormat CQL_DATE_FORMAT = new SimpleDateFormat(
			"yyyy-MM-dd'T'hh:mm:ss'Z'");

	@GeoWaveTestStore(value = {
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.HBASE
	}, options = {
		/**
		 * Here we are testing non-default HBase options, we may want to
		 * consider testing some non-default Accumulo options as well
		 */
		"disableCustomFilters=true",
		"disableCoprocessors=true"
	})
	protected DataStorePluginOptions dataStore;
	private static long startMillis;

	@BeforeClass
	public static void reportTestStart() {
		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------------");
		LOGGER.warn("*                                             *");
		LOGGER.warn("* RUNNING GeoWaveBasicSpatialTemporalVectorIT *");
		LOGGER.warn("*                                             *");
		LOGGER.warn("-----------------------------------------------");
	}

	@AfterClass
	public static void reportTestFinish() {
		LOGGER.warn("------------------------------------------------");
		LOGGER.warn("*                                              *");
		LOGGER.warn("* FINISHED GeoWaveBasicSpatialTemporalVectorIT *");
		LOGGER.warn("*                " + ((System.currentTimeMillis() - startMillis) / 1000)
				+ "s elapsed.                  *");
		LOGGER.warn("*                                              *");
		LOGGER.warn("------------------------------------------------");
	}

	@Test
	public void testIngestAndQuerySpatialTemporalPointsAndLines() {
		// ingest both lines and points
		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL_TEMPORAL,
				HAIL_SHAPEFILE_FILE,
				1);

		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL_TEMPORAL,
				TORNADO_TRACKS_SHAPEFILE_FILE,
				1);

		try {
			testQuery(
					new File(
							TEST_BOX_TEMPORAL_FILTER_FILE).toURI().toURL(),
					new URL[] {
						new File(
								HAIL_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL(),
						new File(
								TORNADO_TRACKS_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL()
					},
					"bounding box and time range");
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing a bounding box and time range query of spatial temporal index: '"
					+ e.getLocalizedMessage() + "'");
		}

		try {
			testQuery(
					new File(
							TEST_POLYGON_TEMPORAL_FILTER_FILE).toURI().toURL(),
					new URL[] {
						new File(
								HAIL_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL(),
						new File(
								TORNADO_TRACKS_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL()
					},
					"polygon constraint and time range");
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing a polygon and time range query of spatial temporal index: '"
					+ e.getLocalizedMessage() + "'");
		}

		try {
			testStats(
					new File[] {
						new File(
								HAIL_SHAPEFILE_FILE),
						new File(
								TORNADO_TRACKS_SHAPEFILE_FILE)
					},
					TestUtils.DEFAULT_SPATIAL_TEMPORAL_INDEX,
					false);
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing a bounding box stats on spatial temporal index: '"
					+ e.getLocalizedMessage() + "'");
		}

		try {
			testSpatialTemporalLocalExportAndReingestWithCQL(new File(
					TEST_BOX_TEMPORAL_FILTER_FILE).toURI().toURL());
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing deletion of an entry using spatial index: '"
					+ e.getLocalizedMessage() + "'");
		}

		try {
			testDeleteDataId(
					new File(
							TEST_BOX_TEMPORAL_FILTER_FILE).toURI().toURL(),
					TestUtils.DEFAULT_SPATIAL_TEMPORAL_INDEX);
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert.fail("Error occurred while testing deletion of an entry using spatial temporal index: '"
					+ e.getLocalizedMessage() + "'");
		}

		TestUtils.deleteAll(dataStore);
	}

	private void testSpatialTemporalLocalExportAndReingestWithCQL(
			final URL filterURL )
			throws CQLException,
			IOException {

		final SimpleFeature savedFilter = TestUtils.resourceToFeature(filterURL);

		final Geometry filterGeometry = (Geometry) savedFilter.getDefaultGeometry();
		final Object startObj = savedFilter.getAttribute(TestUtils.TEST_FILTER_START_TIME_ATTRIBUTE_NAME);
		final Object endObj = savedFilter.getAttribute(TestUtils.TEST_FILTER_END_TIME_ATTRIBUTE_NAME);
		Date startDate = null, endDate = null;
		if ((startObj != null) && (endObj != null)) {
			// if we can resolve start and end times, make it a spatial temporal
			// query
			if (startObj instanceof Calendar) {
				startDate = ((Calendar) startObj).getTime();
			}
			else if (startObj instanceof Date) {
				startDate = (Date) startObj;
			}
			if (endObj instanceof Calendar) {
				endDate = ((Calendar) endObj).getTime();
			}
			else if (endObj instanceof Date) {
				endDate = (Date) endObj;
			}
		}
		final AdapterStore adapterStore = dataStore.createAdapterStore();
		final VectorLocalExportCommand exportCommand = new VectorLocalExportCommand();
		final VectorLocalExportOptions options = exportCommand.getOptions();
		final File exportDir = new File(
				TestUtils.TEMP_DIR,
				TEST_EXPORT_DIRECTORY);
		exportDir.delete();
		exportDir.mkdirs();

		exportCommand.setInputStoreOptions(dataStore);
		options.setBatchSize(10000);
		final Envelope env = filterGeometry.getEnvelopeInternal();
		final double east = env.getMaxX();
		final double west = env.getMinX();
		final double south = env.getMinY();
		final double north = env.getMaxY();
		try (CloseableIterator<DataAdapter<?>> adapterIt = adapterStore.getAdapters()) {
			while (adapterIt.hasNext()) {
				final DataAdapter<?> adapter = adapterIt.next();
				final List<String> adapterIds = new ArrayList<String>();
				adapterIds.add(adapter.getAdapterId().getString());
				options.setAdapterIds(adapterIds);
				if (adapter instanceof GeotoolsFeatureDataAdapter) {
					final GeotoolsFeatureDataAdapter gtAdapter = (GeotoolsFeatureDataAdapter) adapter;
					final TimeDescriptors timeDesc = gtAdapter.getTimeDescriptors();

					String startTimeAttribute;
					if (timeDesc.getStartRange() != null) {
						startTimeAttribute = timeDesc.getStartRange().getLocalName();
					}
					else {
						startTimeAttribute = timeDesc.getTime().getLocalName();
					}
					final String endTimeAttribute;
					if (timeDesc.getEndRange() != null) {
						endTimeAttribute = timeDesc.getEndRange().getLocalName();
					}
					else {
						endTimeAttribute = timeDesc.getTime().getLocalName();
					}
					final String geometryAttribute = gtAdapter.getFeatureType().getGeometryDescriptor().getLocalName();

					final String cqlPredicate = String.format(
							"BBOX(\"%s\",%f,%f,%f,%f) AND \"%s\" <= '%s' AND \"%s\" >= '%s'",
							geometryAttribute,
							west,
							south,
							east,
							north,
							startTimeAttribute,
							CQL_DATE_FORMAT.format(endDate),
							endTimeAttribute,
							CQL_DATE_FORMAT.format(startDate));
					options.setOutputFile(new File(
							exportDir,
							adapter.getAdapterId().getString() + TEST_BASE_EXPORT_FILE_NAME));
					options.setCqlFilter(cqlPredicate);
					exportCommand.setParameters(null);
					exportCommand.execute(new ManualOperationParams());
				}
			}
		}
		TestUtils.deleteAll(dataStore);
		TestUtils.testLocalIngest(
				dataStore,
				DimensionalityType.SPATIAL_TEMPORAL,
				exportDir.getAbsolutePath(),
				"avro",
				4);
		try {
			testQuery(
					new File(
							TEST_BOX_TEMPORAL_FILTER_FILE).toURI().toURL(),
					new URL[] {
						new File(
								HAIL_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL(),
						new File(
								TORNADO_TRACKS_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL()
					},
					"reingested bounding box and time range");
		}
		catch (final Exception e) {
			e.printStackTrace();
			TestUtils.deleteAll(dataStore);
			Assert
					.fail("Error occurred on reingested dataset while testing a bounding box and time range query of spatial temporal index: '"
							+ e.getLocalizedMessage() + "'");
		}
	}

	@Override
	protected DataStorePluginOptions getDataStorePluginOptions() {
		return dataStore;
	}
}
