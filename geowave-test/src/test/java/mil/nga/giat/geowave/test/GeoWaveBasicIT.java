package mil.nga.giat.geowave.test;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.ingest.IngestMain;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.store.query.DistributableQuery;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Geometry;

public class GeoWaveBasicIT extends
		GeoWaveTestEnvironment
{
	private final static Logger LOGGER = Logger.getLogger(GeoWaveBasicIT.class);
	private static final String TEST_DATA_ZIP_RESOURCE_PATH = TEST_RESOURCE_PACKAGE + "basic-testdata.zip";
	private static final String TEST_FILTER_PACKAGE = TEST_CASE_BASE + "filter/";
	private static final String HAIL_TEST_CASE_PACKAGE = TEST_CASE_BASE + "hail_test_case/";
	private static final String HAIL_SHAPEFILE_FILE = HAIL_TEST_CASE_PACKAGE + "hail.shp";
	private static final String HAIL_EXPECTED_BOX_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE + "hail-box-filter.shp";
	private static final String HAIL_EXPECTED_POLYGON_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE + "hail-polygon-filter.shp";
	private static final String HAIL_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE + "hail-box-temporal-filter.shp";
	private static final String HAIL_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE = HAIL_TEST_CASE_PACKAGE + "hail-polygon-temporal-filter.shp";
	private static final String TORNADO_TRACKS_TEST_CASE_PACKAGE = TEST_CASE_BASE + "tornado_tracks_test_case/";
	private static final String TORNADO_TRACKS_SHAPEFILE_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE + "tornado_tracks.shp";
	private static final String TORNADO_TRACKS_EXPECTED_BOX_FILTER_RESULTS_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE + "tornado_tracks-box-filter.shp";
	private static final String TORNADO_TRACKS_EXPECTED_POLYGON_FILTER_RESULTS_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE + "tornado_tracks-polygon-filter.shp";
	private static final String TORNADO_TRACKS_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE + "tornado_tracks-box-temporal-filter.shp";
	private static final String TORNADO_TRACKS_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE = TORNADO_TRACKS_TEST_CASE_PACKAGE + "tornado_tracks-polygon-temporal-filter.shp";

	private static final String TEST_BOX_FILTER_FILE = TEST_FILTER_PACKAGE + "Box-Filter.shp";
	private static final String TEST_POLYGON_FILTER_FILE = TEST_FILTER_PACKAGE + "Polygon-Filter.shp";
	private static final String TEST_BOX_TEMPORAL_FILTER_FILE = TEST_FILTER_PACKAGE + "Box-Temporal-Filter.shp";
	private static final String TEST_POLYGON_TEMPORAL_FILTER_FILE = TEST_FILTER_PACKAGE + "Polygon-Temporal-Filter.shp";

	@BeforeClass
	public static void extractTestFiles()
			throws URISyntaxException {
		GeoWaveTestEnvironment.unZipFile(
				new File(
						GeoWaveBasicIT.class.getClassLoader().getResource(
								TEST_DATA_ZIP_RESOURCE_PATH).toURI()),
				TEST_CASE_BASE);
	}

	@Test
	public void testIngestAndQuerySpatialPointsAndLines() {
		final Index spatialIndex = IndexType.SPATIAL_VECTOR.createDefaultIndex();
		// ingest both lines and points
		testLocalIngest(
				IndexType.SPATIAL_VECTOR,
				HAIL_SHAPEFILE_FILE);
		testLocalIngest(
				IndexType.SPATIAL_VECTOR,
				TORNADO_TRACKS_SHAPEFILE_FILE);

		try {
			testQuery(
					new File(
							TEST_BOX_FILTER_FILE).toURI().toURL(),
					new URL[] {
						new File(
								HAIL_EXPECTED_BOX_FILTER_RESULTS_FILE).toURI().toURL(),
						new File(
								TORNADO_TRACKS_EXPECTED_BOX_FILTER_RESULTS_FILE).toURI().toURL()
					},
					spatialIndex,
					"bounding box constraint only");
		}
		catch (final Exception e) {
			e.printStackTrace();
			try {
				accumuloOperations.deleteAll();
			}
			catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
				LOGGER.error("Unable to clear accumulo namespace", ex);
			}

			Assert.fail("Error occurred while testing a bounding box query of spatial index: '" + e.getLocalizedMessage() + "'");
		}
		try {
			testQuery(
					new File(
							TEST_POLYGON_FILTER_FILE).toURI().toURL(),
					new URL[] {
						new File(
								HAIL_EXPECTED_POLYGON_FILTER_RESULTS_FILE).toURI().toURL(),
						new File(
								TORNADO_TRACKS_EXPECTED_POLYGON_FILTER_RESULTS_FILE).toURI().toURL()
					},
					spatialIndex,
					"polygon constraint only");
		}
		catch (final Exception e) {
			e.printStackTrace();
			try {
				accumuloOperations.deleteAll();
			}
			catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
				LOGGER.error("Unable to clear accumulo namespace", ex);
			}
			Assert.fail("Error occurred while testing a polygon query of spatial index: '" + e.getLocalizedMessage() + "'");
		}

		try {
			testDelete(
					new File(
							TEST_POLYGON_FILTER_FILE).toURI().toURL(),
					IndexType.SPATIAL_VECTOR);
		}
		catch (final Exception e) {
			e.printStackTrace();
			try {
				accumuloOperations.deleteAll();
			}
			catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
				LOGGER.error("Unable to clear accumulo namespace", ex);
			}
			Assert.fail("Error occurred while testing deletion of an entry using spatial index: '" + e.getLocalizedMessage() + "'");
		}
		try {
			accumuloOperations.deleteAll();
		}
		catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
			LOGGER.error("Unable to clear accumulo namespace", ex);
		}
	}

	@Test
	public void testIngestAndQuerySpatialTemporalPointsAndLines() {
		// ingest both lines and points
		testLocalIngest(
				IndexType.SPATIAL_TEMPORAL_VECTOR,
				HAIL_SHAPEFILE_FILE);
		testLocalIngest(
				IndexType.SPATIAL_TEMPORAL_VECTOR,
				TORNADO_TRACKS_SHAPEFILE_FILE);
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
			try {
				accumuloOperations.deleteAll();
			}
			catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
				LOGGER.error("Unable to clear accumulo namespace", ex);
			}
			Assert.fail("Error occurred while testing a bounding box and time range query of spatial temporal index: '" + e.getLocalizedMessage() + "'");
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
			try {
				accumuloOperations.deleteAll();
			}
			catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
				LOGGER.error("Unable to clear accumulo namespace", ex);
			}
			Assert.fail("Error occurred while testing a polygon and time range query of spatial temporal index: '" + e.getLocalizedMessage() + "'");
		}

		try {
			testDelete(
					new File(
							TEST_POLYGON_TEMPORAL_FILTER_FILE).toURI().toURL(),
					IndexType.SPATIAL_TEMPORAL_VECTOR);
		}
		catch (final Exception e) {
			e.printStackTrace();
			try {
				accumuloOperations.deleteAll();
			}
			catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
				LOGGER.error("Unable to clear accumulo namespace", ex);
			}
			Assert.fail("Error occurred while testing deletion of an entry using spatial temporal index: '" + e.getLocalizedMessage() + "'");
		}
		try {
			accumuloOperations.deleteAll();
		}
		catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
			LOGGER.error("Unable to clear accumulo namespace", ex);
		}
	}

	private void testQuery(
			final URL savedFilterResource,
			final URL[] expectedResultsResources,
			final String queryDescription )
			throws Exception {
		// test the query with an unspecified index
		testQuery(
				savedFilterResource,
				expectedResultsResources,
				null,
				queryDescription);
	}

	private void testQuery(
			final URL savedFilterResource,
			final URL[] expectedResultsResources,
			final Index index,
			final String queryDescription )
			throws Exception {
		LOGGER.info("querying " + queryDescription);
		System.out.println("querying " + queryDescription);
		final mil.nga.giat.geowave.store.DataStore geowaveStore = new AccumuloDataStore(
				new AccumuloIndexStore(
						accumuloOperations),
				new AccumuloAdapterStore(
						accumuloOperations),
				new AccumuloDataStatisticsStore(
						accumuloOperations),
				accumuloOperations);
		// this file is the filtered dataset (using the previous file as a
		// filter) so use it to ensure the query worked
		final DistributableQuery query = resourceToQuery(savedFilterResource);
		final CloseableIterator<?> actualResults;
		if (index == null) {
			actualResults = geowaveStore.query(query);
		}
		else {
			actualResults = geowaveStore.query(
					index,
					query);
		}
		final ExpectedResults expectedResults = getExpectedResults(expectedResultsResources);
		int totalResults = 0;
		while (actualResults.hasNext()) {
			final Object obj = actualResults.next();
			if (obj instanceof SimpleFeature) {
				final SimpleFeature result = (SimpleFeature) obj;
				Assert.assertTrue(
						"Actual result '" + result.toString() + "' not found in expected result set",
						expectedResults.hashedCentroids.contains(hashCentroid((Geometry) result.getDefaultGeometry())));
				totalResults++;
			}
			else {
				try {
					accumuloOperations.deleteAll();
				}
				catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
					LOGGER.error("Unable to clear accumulo namespace", ex);
				}
				Assert.fail("Actual result '" + obj.toString() + "' is not of type Simple Feature.");
			}
		}
		if (expectedResults.count != totalResults) {
			try {
				accumuloOperations.deleteAll();
			}
			catch (TableNotFoundException | AccumuloSecurityException | AccumuloException ex) {
				LOGGER.error("Unable to clear accumulo namespace", ex);
				Assert.fail("Unable to clear accumulo namespace");
			}
		}
		Assert.assertEquals(
				expectedResults.count,
				totalResults);
		actualResults.close();
	}

	private void testDelete(
			final URL savedFilterResource,
			final IndexType indexType )
			throws Exception {
		LOGGER.info("deleting from " + indexType.toString() + " index");
		System.out.println("deleting from " + indexType.toString() + " index");
		boolean success = false;
		final mil.nga.giat.geowave.store.DataStore geowaveStore = new AccumuloDataStore(
				new AccumuloIndexStore(
						accumuloOperations),
				new AccumuloAdapterStore(
						accumuloOperations),
				new AccumuloDataStatisticsStore(
						accumuloOperations),
				accumuloOperations);
		final DistributableQuery query = resourceToQuery(savedFilterResource);
		final Index index = indexType.createDefaultIndex();
		final CloseableIterator<?> actualResults;

		actualResults = geowaveStore.query(
				index,
				query);

		SimpleFeature testFeature = null;
		while (actualResults.hasNext()) {
			final Object obj = actualResults.next();
			if ((testFeature == null) && (obj instanceof SimpleFeature)) {
				testFeature = (SimpleFeature) obj;
			}
		}
		actualResults.close();

		if (testFeature != null) {
			final ByteArrayId dataId = new ByteArrayId(
					testFeature.getID());
			final ByteArrayId adapterId = new ByteArrayId(
					testFeature.getFeatureType().getTypeName());

			if (geowaveStore.deleteEntry(
					index,
					dataId,
					adapterId)) {

				if (geowaveStore.getEntry(
						index,
						dataId,
						adapterId) == null) {
					success = true;
				}
			}
		}
		Assert.assertTrue(
				"Unable to delete entry by data ID and adapter ID",
				success);
	}

}
