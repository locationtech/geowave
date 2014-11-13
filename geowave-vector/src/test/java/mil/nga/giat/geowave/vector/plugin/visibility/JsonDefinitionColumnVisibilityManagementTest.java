package mil.nga.giat.geowave.vector.plugin.visibility;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.vector.plugin.visibility.FieldLevelVisibilityHandler;
import mil.nga.giat.geowave.vector.plugin.visibility.JsonDefinitionColumnVisibilityManagement;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

public class JsonDefinitionColumnVisibilityManagementTest
{

	SimpleFeatureType type;
	List<AttributeDescriptor> descriptors;
	Object[] defaults;
	SimpleFeature newFeature;
	final JsonDefinitionColumnVisibilityManagement<SimpleFeature> manager = new JsonDefinitionColumnVisibilityManagement<SimpleFeature>();
	final GeometryFactory factory = new GeometryFactory(
			new PrecisionModel(
					PrecisionModel.FIXED));
	final FieldLevelVisibilityHandler<SimpleFeature, Object> simplePIDHandler = new FieldLevelVisibilityHandler<SimpleFeature, Object>(
			"pid",
			new GlobalVisibilityHandler<SimpleFeature, Object>("default"),
			"vis",
			manager);

	final FieldLevelVisibilityHandler<SimpleFeature, Object> simplePOPHandler = new FieldLevelVisibilityHandler<SimpleFeature, Object>(
			"pop",
			new GlobalVisibilityHandler<SimpleFeature, Object>("default"),
			"vis",
			manager);

	final FieldLevelVisibilityHandler<SimpleFeature, Object> simpleGEOHandler = new FieldLevelVisibilityHandler<SimpleFeature, Object>(
			"geometry",
			new GlobalVisibilityHandler<SimpleFeature, Object>("default"),
			"vis",
			manager);

	@Before
	public void setup()
			throws AccumuloException,
			AccumuloSecurityException,
			SchemaException,
			CQLException {
		type = DataUtilities.createType(
				"geostuff",
				"geometry:Geometry:srid=4326,vis:java.lang.String,pop:java.lang.Long,pid:String");
		descriptors = type.getAttributeDescriptors();
		defaults = new Object[descriptors.size()];
		int p = 0;
		for (AttributeDescriptor descriptor : descriptors) {
			defaults[p++] = descriptor.getDefaultValue();
		}

		newFeature = SimpleFeatureBuilder.build(
				type,
				defaults,
				UUID.randomUUID().toString());
		newFeature.setAttribute(
				"pop",
				new Long(
						100));
		newFeature.setAttribute(
				"pid",
				UUID.randomUUID().toString());
		newFeature.setAttribute(
				"vis",
				"{\"pid\":\"TS\", \"geo.*\":\"S\"}");
		newFeature.setAttribute(
				"geometry",
				factory.createPoint(new Coordinate(
						43.454,
						128.232)));
	}

	@Test
	public void testPIDNonDefault() {

		assertTrue(Arrays.equals(
				"TS".getBytes(),
				simplePIDHandler.getVisibility(
						newFeature,
						new ByteArrayId(
								"pid".getBytes()),
						"pid")));
	}

	@Test
	public void testPOPNonDefault() {
		assertTrue(Arrays.equals(
				"default".getBytes(),
				simplePOPHandler.getVisibility(
						newFeature,
						new ByteArrayId(
								"pop".getBytes()),
						"pop")));

	}

	@Test
	public void testGEORegexDefault() {
		assertTrue(Arrays.equals(
				"S".getBytes(),
				simpleGEOHandler.getVisibility(
						newFeature,
						new ByteArrayId(
								"geometry".getBytes()),
						"geometry")));

	}

	@Test
	public void testCatchAllRegexDefault() {
		newFeature.setAttribute(
				"vis",
				"{\"pid\":\"TS\", \".*\":\"U\"}");
		assertTrue(Arrays.equals(
				"U".getBytes(),
				simplePOPHandler.getVisibility(
						newFeature,
						new ByteArrayId(
								"pop".getBytes()),
						"pop")));

	}

}
