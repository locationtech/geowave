package mil.nga.giat.geowave.adapter.vector.utils;

import static org.junit.Assert.assertEquals;
import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;
import mil.nga.giat.geowave.core.index.StringUtils;

import org.geotools.feature.SchemaException;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeatureType;

public class FeatureDataUtilsTest
{

	@Test
	public void testWithSRID()
			throws SchemaException {
		SimpleFeatureType type = FeatureDataUtils.decodeType(
				"http://somens.org",
				"type1",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,whennot:Date,pid:String",
				"east");
		assertEquals(
				"type1",
				type.getName().getLocalPart());
	}

	/**
	 * 
	 * This test only works in some versions. So, comment out for now.
	 * 
	 * public void testWithSRIDAndMisMatch() throws SchemaException {
	 * SimpleFeatureType type = FeatureDataUtils.decodeType("http://somens.org",
	 * "type1",
	 * "geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,whennot:Date,pid:String"
	 * , "north"); assertEquals("type1",type.getName().getLocalPart());
	 * assertEquals
	 * ("NORTH",type.getCoordinateReferenceSystem().getCoordinateSystem
	 * ().getAxis(0).getDirection().name()); }
	 */

	@Test
	public void testWithoutSRID()
			throws SchemaException {
		SimpleFeatureType type = FeatureDataUtils.decodeType(
				"http://somens.org",
				"type1",
				"geometry:Geometry,pop:java.lang.Long,when:Date,whennot:Date,pid:String",
				StringUtils.stringFromBinary(new byte[0]));
		assertEquals(
				"type1",
				type.getName().getLocalPart());
	}

}
