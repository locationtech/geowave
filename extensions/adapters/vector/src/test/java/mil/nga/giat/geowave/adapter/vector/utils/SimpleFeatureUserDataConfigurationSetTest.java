package mil.nga.giat.geowave.adapter.vector.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;

import org.geotools.feature.SchemaException;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeatureType;

public class SimpleFeatureUserDataConfigurationSetTest
{

	@Test
	public void testNoConfig()
			throws SchemaException {
		SimpleFeatureType type = FeatureDataUtils.decodeType(
				"http://somens.org",
				"type1",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,whennot:Date,pid:String",
				"east");
		SimpleFeatureUserDataConfigurationSet.configureType(type);
		assertFalse(type.getDescriptor(
				"pop").getUserData().containsKey(
				"stats"));
	}

	@Test
	public void testConfig()
			throws SchemaException {
		System.setProperty(
				SimpleFeatureUserDataConfigurationSet.SIMPLE_FEATURE_CONFIG_FILE_PROP,
				"src/test/resources/statsFile.json");
		SimpleFeatureType type = FeatureDataUtils.decodeType(
				"http://somens.org",
				"type1",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,whennot:Date,pid:String",
				"east");
		SimpleFeatureUserDataConfigurationSet.configureType(type);
		assertTrue(type.getDescriptor(
				"pop").getUserData().containsKey(
				"stats"));
	}
}
