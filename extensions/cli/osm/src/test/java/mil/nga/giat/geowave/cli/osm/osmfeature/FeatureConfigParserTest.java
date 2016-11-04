package mil.nga.giat.geowave.cli.osm.osmfeature;

import mil.nga.giat.geowave.cli.osm.osmfeature.FeatureConfigParser;
import mil.nga.giat.geowave.cli.osm.osmfeature.types.features.FeatureDefinitionSet;

import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class FeatureConfigParserTest
{

	protected static final String TEST_RESOURCE_DIR = new File(
			"./src/test/data/").getAbsolutePath().toString();
	protected static final String TEST_DATA_CONFIG = TEST_RESOURCE_DIR + "/" + "test_mapping.json";

	@Test
	public void testFeatureConfigParser()
			throws IOException {
		FeatureConfigParser fcp = new FeatureConfigParser();

		try (FileInputStream fis = new FileInputStream(
				new File(
						TEST_DATA_CONFIG))) {
			fcp.parseConfig(fis);
		}

	}

}