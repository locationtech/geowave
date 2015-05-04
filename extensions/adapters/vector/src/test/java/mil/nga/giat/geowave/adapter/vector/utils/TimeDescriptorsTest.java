package mil.nga.giat.geowave.adapter.vector.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import mil.nga.giat.geowave.adapter.vector.utils.TimeDescriptors;

import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeatureType;

public class TimeDescriptorsTest
{

	@Test
	public void testOneTime()
			throws SchemaException {
		SimpleFeatureType schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,whennot:Date,pid:String");
		TimeDescriptors td = new TimeDescriptors();
		td.inferType(schema);
		assertEquals(
				"when",
				td.getTime().getLocalName());
		assertNull(td.getStartRange());
		assertNull(td.getEndRange());
		assertTrue(td.hasTime());
		testSerialization(
				schema,
				td);

	}

	private void testSerialization(
			SimpleFeatureType schema,
			TimeDescriptors td ) {
		TimeDescriptors tdnew = new TimeDescriptors();
		tdnew.fromBinary(
				schema,
				td.toBinary());
		assertEquals(
				tdnew,
				td);
	}

	@Test
	public void testRangeTime()
			throws SchemaException {
		SimpleFeatureType schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,start:Date,end:Date,pid:String");
		TimeDescriptors td = new TimeDescriptors();
		td.inferType(schema);
		assertEquals(
				"start",
				td.getStartRange().getLocalName());
		assertEquals(
				"end",
				td.getEndRange().getLocalName());
		assertNull(td.getTime());
		assertTrue(td.hasTime());
		testSerialization(
				schema,
				td);
	}

	@Test
	public void testMixedTime()
			throws SchemaException {
		SimpleFeatureType schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,start:Date,end:Date,pid:String");
		TimeDescriptors td = new TimeDescriptors();
		td.inferType(schema);
		assertEquals(
				"start",
				td.getStartRange().getLocalName());
		assertEquals(
				"end",
				td.getEndRange().getLocalName());
		assertNull(td.getTime());
		assertTrue(td.hasTime());
		testSerialization(
				schema,
				td);
	}

	@Test
	public void testJustStartTime()
			throws SchemaException {
		SimpleFeatureType schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,start:Date,pid:String");
		TimeDescriptors td = new TimeDescriptors();
		td.inferType(schema);
		assertEquals(
				"start",
				td.getTime().getLocalName());
		assertNull(td.getStartRange());
		assertNull(td.getEndRange());
		assertTrue(td.hasTime());
		testSerialization(
				schema,
				td);
	}

	@Test
	public void testJustEndTime()
			throws SchemaException {
		SimpleFeatureType schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,end:Date,pid:String");
		TimeDescriptors td = new TimeDescriptors();
		td.inferType(schema);
		assertEquals(
				"end",
				td.getTime().getLocalName());
		assertNull(td.getStartRange());
		assertNull(td.getEndRange());
		assertTrue(td.hasTime());
		testSerialization(
				schema,
				td);
	}

	@Test
	public void testWhenAndEndTime()
			throws SchemaException {
		SimpleFeatureType schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,end:Date,pid:String");
		TimeDescriptors td = new TimeDescriptors();
		td.inferType(schema);
		assertEquals(
				"when",
				td.getTime().getLocalName());
		assertNull(td.getStartRange());
		assertNull(td.getEndRange());
		assertTrue(td.hasTime());
		testSerialization(
				schema,
				td);
	}

	@Test
	public void testWhenAndStartTime()
			throws SchemaException {
		SimpleFeatureType schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,start:Date,pid:String");
		TimeDescriptors td = new TimeDescriptors();
		td.inferType(schema);
		assertEquals(
				"when",
				td.getTime().getLocalName());
		assertNull(td.getStartRange());
		assertNull(td.getEndRange());
		assertTrue(td.hasTime());
		testSerialization(
				schema,
				td);
	}

}
