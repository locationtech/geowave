package mil.nga.giat.geowave.test.mapreduce;

import mil.nga.giat.geowave.test.GeoWaveTestEnvironment;
import org.junit.BeforeClass;

import java.io.File;
import java.net.URISyntaxException;

public class MapReduceTestBase extends
		MapReduceTestEnvironment
{
	protected static final String TEST_DATA_ZIP_RESOURCE_PATH = TEST_RESOURCE_PACKAGE + "mapreduce-testdata.zip";
	protected static final String TEST_CASE_GENERAL_GPX_BASE = TEST_CASE_BASE + "general_gpx_test_case/";
	protected static final String GENERAL_GPX_FILTER_PACKAGE = TEST_CASE_GENERAL_GPX_BASE + "filter/";
	protected static final String GENERAL_GPX_FILTER_FILE = GENERAL_GPX_FILTER_PACKAGE + "filter.shp";
	protected static final String GENERAL_GPX_INPUT_GPX_DIR = TEST_CASE_GENERAL_GPX_BASE + "input_gpx/";
	protected static final String GENERAL_GPX_EXPECTED_RESULTS_DIR = TEST_CASE_GENERAL_GPX_BASE + "filter_results/";
	protected static final String OSM_GPX_INPUT_DIR = TEST_CASE_BASE + "osm_gpx_test_case/";

	@BeforeClass
	public static void extractTestFiles()
			throws URISyntaxException {
		GeoWaveTestEnvironment.unZipFile(
				new File(
						MapReduceTestEnvironment.class.getClassLoader().getResource(
								TEST_DATA_ZIP_RESOURCE_PATH).toURI()),
				TEST_CASE_BASE);
	}

}
