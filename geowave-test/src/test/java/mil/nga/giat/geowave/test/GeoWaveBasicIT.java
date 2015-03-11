package mil.nga.giat.geowave.test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.ingest.IngestMain;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.GeometryUtils;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.store.query.DistributableQuery;
import mil.nga.giat.geowave.store.query.SpatialQuery;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

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
		testIngest(
				IndexType.SPATIAL_VECTOR,
				HAIL_SHAPEFILE_FILE);
		testIngest(
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
		testIngest(
				IndexType.SPATIAL_TEMPORAL_VECTOR,
				HAIL_SHAPEFILE_FILE);
		testIngest(
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

    @Test
    public void testFeatureSerialization() {

        final Map<Class, Object> args = new HashMap<>();
        args.put(Geometry.class, GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(123.4, 567.8)).buffer(1));
        args.put(Integer.class, 23);
        args.put(Long.class, 473874387l);
        args.put(Boolean.class, Boolean.TRUE);
        args.put(Byte.class, (byte) 0xa);
        args.put(Short.class, Short.valueOf("2"));
        args.put(Float.class, 34.23434f);
        args.put(Double.class, 85.3498394839d);
        args.put(byte[].class, new byte[]{(byte) 1, (byte) 2, (byte) 3});
        args.put(Byte[].class, new Byte[]{(byte) 4, (byte) 5, (byte) 6});
        args.put(Date.class, new Date(8675309l));
        args.put(BigInteger.class, BigInteger.valueOf(893489348343423l));
        args.put(BigDecimal.class, new BigDecimal("939384.93840238409237483617837483"));
        args.put(Calendar.class, Calendar.getInstance());
        args.put(String.class, "TThis is my string. There are many like it, but this one is mine.\n" +
                "My string is my best friend. It is my life. I must master it as I must master my life.");
        args.put(long[].class, new long[]{12345l, 6789l, 1011l, 1213111111111111l});
        args.put(int[].class, new int[]{-55, -44, -33, -934839, 55});
        args.put(double[].class, new double[]{1.125d, 2.25d});
        args.put(float[].class, new float[]{1.5f, 1.75f});
        args.put(short[].class, new short[]{(short) 8, (short) 9, (short) 10});

        final SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        final AttributeTypeBuilder ab = new AttributeTypeBuilder();
        builder.setName("featureserializationtest");

        for (Map.Entry<Class, Object> arg : args.entrySet()) {
            builder.add(ab.binding(arg.getKey()).nillable(false).buildDescriptor(arg.getKey().getName().toString()));
        }

        final SimpleFeatureType serTestType = builder.buildFeatureType();
        final SimpleFeatureBuilder serBuilder = new SimpleFeatureBuilder(serTestType);
        final FeatureDataAdapter serAdapter = new FeatureDataAdapter(serTestType);
        final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();

        for (Map.Entry<Class, Object> arg : args.entrySet()) {
            serBuilder.set(arg.getKey().getName(), arg.getValue());
        }

        final mil.nga.giat.geowave.store.DataStore geowaveStore = new AccumuloDataStore(
                new AccumuloIndexStore(
                        accumuloOperations),
                new AccumuloAdapterStore(
                        accumuloOperations),
                new AccumuloDataStatisticsStore(
                        accumuloOperations),
                accumuloOperations);

        SimpleFeature sf = serBuilder.buildFeature("343");
        geowaveStore.ingest(serAdapter, index, sf);
        DistributableQuery q = new SpatialQuery(((Geometry) args.get(Geometry.class)).buffer(0.5d));
        CloseableIterator<?> iter = geowaveStore.query(q);
        boolean foundFeat = false;
        while (iter.hasNext()) {
            Object maybeFeat = iter.next();
            Assert.assertTrue("Iterator should return simple feature in this test",
                    maybeFeat instanceof SimpleFeature);
            foundFeat = true;
            SimpleFeature isFeat = (SimpleFeature) maybeFeat;
            for (Property p : isFeat.getProperties()) {
                Object before = args.get(p.getType().getBinding());
                Object after = isFeat.getAttribute(p.getType().getName().toString());

                if (before instanceof double[]) {
                    Assert.assertArrayEquals((double[]) before, (double[]) after, 1e-12d);
                    break;
                } else if (before instanceof boolean[]) {
                    boolean[] b = (boolean[]) before;
                    boolean[] a = (boolean[]) after;
                    Assert.assertTrue(a.length == b.length);
                    for (int i = 0; i < b.length; i++) {
                        Assert.assertTrue(b[i] == a[i]);
                    }
                } else if (before instanceof byte[]) {
                    Assert.assertArrayEquals((byte[]) before, (byte[]) after);
                } else if (before instanceof char[]) {
                    Assert.assertArrayEquals((char[]) before, (char[]) after);
                } else if (before instanceof float[]) {
                    Assert.assertArrayEquals((float[]) before, (float[]) after, 1e-12f);
                } else if (before instanceof int[]) {
                    Assert.assertArrayEquals((int[]) before, (int[]) after);
                } else if (before instanceof long[]) {
                    Assert.assertArrayEquals((long[]) before, (long[]) after);
                } else if (before instanceof short[]) {
                    Assert.assertArrayEquals((short[]) before, (short[]) after);
                } else if (before.getClass().isArray()) {
                    Assert.assertArrayEquals(returnArray(p.getType().getBinding(), before), returnArray(p.getType().getBinding(), after));
                } else {
                    Assert.assertTrue(before.equals(after));
                }
            }
        }
        IOUtils.closeQuietly(iter);
        Assert.assertTrue("One feature should be found", foundFeat);
    }

    public <T> T[] returnArray(Class<T> clazz, Object o){
        return (T[])o;
    }


	private void testIngest(
			final IndexType indexType,
			final String ingestFilePath ) {
		// ingest a shapefile (geotools type) directly into GeoWave using the
		// ingest framework's main method and pre-defined commandline arguments
		LOGGER.warn("Ingesting '" + ingestFilePath + "' - this may take several minutes...");
		final String[] args = StringUtils.split(
				"-localingest -t geotools-vector -b " + ingestFilePath + " -z " + zookeeper + " -i " + accumuloInstance + " -u " + accumuloUser + " -p " + accumuloPassword + " -n " + TEST_NAMESPACE + " -dim " + (indexType.equals(IndexType.SPATIAL_VECTOR) ? "spatial" : "spatial-temporal"),
				' ');
		IngestMain.main(args);
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
