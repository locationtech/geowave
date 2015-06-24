package mil.nga.giat.geowave.test.kafka;

import java.io.File;
import java.net.URISyntaxException;

import mil.nga.giat.geowave.test.GeoWaveTestEnvironment;

import org.junit.BeforeClass;

public class KafkaTestBase<I> extends
		KafkaTestEnvironment<I>
{
	protected static final String TEST_DATA_ZIP_RESOURCE_PATH = TEST_RESOURCE_PACKAGE + "mapreduce-testdata.zip";
	protected static final String OSM_GPX_INPUT_DIR = TEST_CASE_BASE + "osm_gpx_test_case/";

	@BeforeClass
	public static void extractTestFiles()
			throws URISyntaxException {
		GeoWaveTestEnvironment.unZipFile(
				new File(
						KafkaTestBase.class.getClassLoader().getResource(
								TEST_DATA_ZIP_RESOURCE_PATH).toURI()),
				TEST_CASE_BASE);
	}

}
