package mil.nga.giat.geowave.vector.adapter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import mil.nga.giat.geowave.store.adapter.IndexFieldHandler;
import mil.nga.giat.geowave.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.store.index.CommonIndexValue;
import mil.nga.giat.geowave.vector.utils.DateUtilities;

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

public class FeatureDataAdapterTest
{

	private SimpleFeatureType schema;
	private SimpleFeature newFeature;
	private Date time1;
	private Date time2;

	GeometryFactory factory = new GeometryFactory(
			new PrecisionModel(
					PrecisionModel.FIXED));

	@Before
	public void setup()
			throws AccumuloException,
			AccumuloSecurityException,
			SchemaException,
			CQLException,
			ParseException {

		time1 = DateUtilities.parseISO("2005-05-19T18:33:55Z");
		time2 = DateUtilities.parseISO("2005-05-19T19:33:55Z");

		schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,whennot:Date,pid:String");

		List<AttributeDescriptor> descriptors = schema.getAttributeDescriptors();
		Object[] defaults = new Object[descriptors.size()];
		int p = 0;
		for (AttributeDescriptor descriptor : descriptors) {
			defaults[p++] = descriptor.getDefaultValue();
		}

		newFeature = SimpleFeatureBuilder.build(
				schema,
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
				"when",
				time1);
		newFeature.setAttribute(
				"whennot",
				time2);
		newFeature.setAttribute(
				"geometry",
				factory.createPoint(new Coordinate(
						27.25,
						41.25)));

	}

	@Test
	public void testSingleTime() {
		schema.getDescriptor(
				"when").getUserData().clear();
		schema.getDescriptor(
				"whennot").getUserData().put(
				"time",
				Boolean.TRUE);

		FeatureDataAdapter dataAdapter = new FeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
		byte[] binary = dataAdapter.toBinary();

		FeatureDataAdapter dataAdapterCopy = new FeatureDataAdapter();
		dataAdapterCopy.fromBinary(binary);

		assertEquals(
				dataAdapterCopy.getAdapterId(),
				dataAdapter.getAdapterId());
		assertEquals(
				dataAdapterCopy.getType(),
				dataAdapter.getType());
		assertEquals(
				Boolean.TRUE,
				dataAdapterCopy.getType().getDescriptor(
						"whennot").getUserData().get(
						"time"));

		List<IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> handlers = dataAdapterCopy.getDefaultTypeMatchingHandlers(schema);
		boolean found = false;
		for (IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object> handler : handlers) {
			found |= (handler instanceof FeatureTimestampHandler && (((FeatureTimestampHandler) handler).toIndexValue(
					newFeature).toNumericData().getMin() - (double) time2.getTime() < 0.001));
		}

		assertTrue(found);
	}

	@Test
	public void testVisibility() {
		schema.getDescriptor(
				"pid").getUserData().clear();
		schema.getDescriptor(
				"pid").getUserData().put(
				"visibility",
				Boolean.TRUE);

		FeatureDataAdapter dataAdapter = new FeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
		byte[] binary = dataAdapter.toBinary();

		FeatureDataAdapter dataAdapterCopy = new FeatureDataAdapter();
		dataAdapterCopy.fromBinary(binary);

		assertEquals(
				dataAdapterCopy.getAdapterId(),
				dataAdapter.getAdapterId());
		assertEquals(
				dataAdapterCopy.getType(),
				dataAdapter.getType());
		assertEquals(
				Boolean.TRUE,
				dataAdapterCopy.getType().getDescriptor(
						"pid").getUserData().get(
						"visibility"));
		assertEquals(
				"pid",
				dataAdapterCopy.getVisibilityAttributeName());

	}

	@Test
	public void testNoTime() {
		schema.getDescriptor(
				"when").getUserData().clear();
		schema.getDescriptor(
				"whennot").getUserData().clear();
		schema.getDescriptor(
				"when").getUserData().put(
				"time",
				Boolean.FALSE);
		schema.getDescriptor(
				"whennot").getUserData().put(
				"time",
				Boolean.FALSE);

		FeatureDataAdapter dataAdapter = new FeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));

		List<IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> handlers = dataAdapter.getDefaultTypeMatchingHandlers(schema);
		boolean found = false;
		for (IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object> handler : handlers) {
			found |= (handler instanceof FeatureTimestampHandler);
		}

		assertFalse(found);
	}

	@Test
	public void testInferredTime() {

		schema.getDescriptor(
				"when").getUserData().clear();
		schema.getDescriptor(
				"whennot").getUserData().clear();

		FeatureDataAdapter dataAdapter = new FeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
		byte[] binary = dataAdapter.toBinary();

		FeatureDataAdapter dataAdapterCopy = new FeatureDataAdapter();
		dataAdapterCopy.fromBinary(binary);

		assertEquals(
				dataAdapterCopy.getAdapterId(),
				dataAdapter.getAdapterId());
		assertEquals(
				dataAdapterCopy.getType(),
				dataAdapter.getType());
		assertEquals(
				Boolean.TRUE,
				dataAdapterCopy.getType().getDescriptor(
						"when").getUserData().get(
						"time"));

		List<IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> handlers = dataAdapterCopy.getDefaultTypeMatchingHandlers(schema);
		boolean found = false;
		for (IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object> handler : handlers) {
			found |= (handler instanceof FeatureTimestampHandler && (((FeatureTimestampHandler) handler).toIndexValue(
					newFeature).toNumericData().getMin() - (double) time1.getTime() < 0.001));
		}

		assertTrue(found);
	}

	@Test
	public void testRange() {

		schema.getDescriptor(
				"when").getUserData().clear();
		schema.getDescriptor(
				"whennot").getUserData().clear();

		schema.getDescriptor(
				"when").getUserData().put(
				"start",
				Boolean.TRUE);
		schema.getDescriptor(
				"whennot").getUserData().put(
				"end",
				Boolean.TRUE);

		FeatureDataAdapter dataAdapter = new FeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
		byte[] binary = dataAdapter.toBinary();

		FeatureDataAdapter dataAdapterCopy = new FeatureDataAdapter();
		dataAdapterCopy.fromBinary(binary);

		assertEquals(
				dataAdapterCopy.getAdapterId(),
				dataAdapter.getAdapterId());
		assertEquals(
				dataAdapterCopy.getType(),
				dataAdapter.getType());
		assertEquals(
				Boolean.TRUE,
				dataAdapterCopy.getType().getDescriptor(
						"whennot").getUserData().get(
						"end"));
		assertEquals(
				Boolean.TRUE,
				dataAdapterCopy.getType().getDescriptor(
						"when").getUserData().get(
						"start"));

		List<IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> handlers = dataAdapterCopy.getDefaultTypeMatchingHandlers(schema);
		boolean found = false;
		for (IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object> handler : handlers) {
			found |= (handler instanceof FeatureTimeRangeHandler && (((FeatureTimeRangeHandler) handler).toIndexValue(
					newFeature).toNumericData().getMin() - (double) time1.getTime() < 0.001) && (((FeatureTimeRangeHandler) handler).toIndexValue(
					newFeature).toNumericData().getMax() - (double) time2.getTime() < 0.001));
		}

		assertTrue(found);
	}

	@Test
	public void testInferredRange()
			throws SchemaException {

		final SimpleFeatureType schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,start:Date,end:Date,pid:String");

		List<AttributeDescriptor> descriptors = schema.getAttributeDescriptors();
		Object[] defaults = new Object[descriptors.size()];
		int p = 0;
		for (AttributeDescriptor descriptor : descriptors) {
			defaults[p++] = descriptor.getDefaultValue();
		}

		final SimpleFeature newFeature = SimpleFeatureBuilder.build(
				schema,
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
				"start",
				time1);
		newFeature.setAttribute(
				"end",
				time2);
		newFeature.setAttribute(
				"geometry",
				factory.createPoint(new Coordinate(
						27.25,
						41.25)));

		FeatureDataAdapter dataAdapter = new FeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
		byte[] binary = dataAdapter.toBinary();

		FeatureDataAdapter dataAdapterCopy = new FeatureDataAdapter();
		dataAdapterCopy.fromBinary(binary);

		assertEquals(
				dataAdapterCopy.getAdapterId(),
				dataAdapter.getAdapterId());
		assertEquals(
				dataAdapterCopy.getType(),
				dataAdapter.getType());
		assertEquals(
				Boolean.TRUE,
				dataAdapterCopy.getType().getDescriptor(
						"end").getUserData().get(
						"end"));
		assertEquals(
				Boolean.TRUE,
				dataAdapterCopy.getType().getDescriptor(
						"start").getUserData().get(
						"start"));

		List<IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> handlers = dataAdapterCopy.getDefaultTypeMatchingHandlers(schema);
		boolean found = false;
		for (IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object> handler : handlers) {
			found |= (handler instanceof FeatureTimeRangeHandler && (((FeatureTimeRangeHandler) handler).toIndexValue(
					newFeature).toNumericData().getMin() - (double) time1.getTime() < 0.001) && (((FeatureTimeRangeHandler) handler).toIndexValue(
					newFeature).toNumericData().getMax() - (double) time2.getTime() < 0.001));
		}

		assertTrue(found);
	}

}
