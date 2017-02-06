package mil.nga.giat.geowave.adapter.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.PrecisionModel;

import mil.nga.giat.geowave.adapter.vector.index.NumericSecondaryIndexConfiguration;
import mil.nga.giat.geowave.adapter.vector.index.TemporalSecondaryIndexConfiguration;
import mil.nga.giat.geowave.adapter.vector.index.TextSecondaryIndexConfiguration;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveGTDataStore;
import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;
import mil.nga.giat.geowave.adapter.vector.utils.DateUtilities;
import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfiguration;
import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfigurationSet;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.IndexFieldHandler;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexType;

public class FeatureDataAdapterTest
{

	private SimpleFeatureType schema;
	private SimpleFeature newFeature;
	private Date time1;
	private Date time2;

	GeometryFactory factory = new GeometryFactory(
			new PrecisionModel(
					PrecisionModel.FIXED));

	@SuppressWarnings("unchecked")
	@Before
	public void setup()
			throws SchemaException,
			CQLException,
			ParseException {

		time1 = DateUtilities.parseISO("2005-05-19T18:33:55Z");
		time2 = DateUtilities.parseISO("2005-05-19T19:33:55Z");

		schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,whennot:Date,pid:String");

		newFeature = FeatureDataUtils.buildFeature(
				schema,
				new Pair[] {
					Pair.of(
							"geometry",
							factory.createPoint(new Coordinate(
									27.25,
									41.25))),
					Pair.of(
							"pop",
							Long.valueOf(100)),
					Pair.of(
							"when",
							time1),
					Pair.of(
							"whennot",
							time2)

				});

	}

	@Test
	public void testDifferentProjection()
			throws SchemaException {
		final SimpleFeatureType schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=3005,pop:java.lang.Long");
		final FeatureDataAdapter dataAdapter = new FeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
		final CoordinateReferenceSystem crs = dataAdapter.getFeatureType().getCoordinateReferenceSystem();
		assertTrue(crs.getIdentifiers().toString().contains(
				"EPSG:4326"));
		@SuppressWarnings("unchecked")
		final SimpleFeature newFeature = FeatureDataUtils.buildFeature(
				schema,
				new Pair[] {
					Pair.of(
							"geometry",
							factory.createPoint(new Coordinate(
									27.25,
									41.25))),
					Pair.of(
							"pop",
							Long.valueOf(100))
				});
		final AdapterPersistenceEncoding persistenceEncoding = dataAdapter.encode(
				newFeature,
				new SpatialDimensionalityTypeProvider().createPrimaryIndex().getIndexModel());

		GeometryWrapper wrapper = null;
		for (final PersistentValue<?> pv : persistenceEncoding.getCommonData().getValues()) {
			if (pv.getValue() instanceof GeometryWrapper) {
				wrapper = (GeometryWrapper) pv.getValue();
			}
		}
		assertNotNull(wrapper);

		assertEquals(
				new Coordinate(
						-138.0,
						44.0),
				wrapper.getGeometry().getCentroid().getCoordinate());
	}

	@Test
	public void testSingleTime() {
		schema.getDescriptor(
				"when").getUserData().clear();
		schema.getDescriptor(
				"whennot").getUserData().put(
				"time",
				Boolean.TRUE);

		final FeatureDataAdapter dataAdapter = new FeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
		final byte[] binary = dataAdapter.toBinary();

		final FeatureDataAdapter dataAdapterCopy = new FeatureDataAdapter();
		dataAdapterCopy.fromBinary(binary);

		assertEquals(
				dataAdapterCopy.getAdapterId(),
				dataAdapter.getAdapterId());
		assertEquals(
				dataAdapterCopy.getFeatureType(),
				dataAdapter.getFeatureType());
		assertEquals(
				Boolean.TRUE,
				dataAdapterCopy.getFeatureType().getDescriptor(
						"whennot").getUserData().get(
						"time"));

		final List<IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> handlers = dataAdapterCopy
				.getDefaultTypeMatchingHandlers(schema);
		boolean found = false;
		for (final IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object> handler : handlers) {
			found |= ((handler instanceof FeatureTimestampHandler) && ((((FeatureTimestampHandler) handler)
					.toIndexValue(
							newFeature)
					.toNumericData()
					.getMin() - time2.getTime()) < 0.001));
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

		final FeatureDataAdapter dataAdapter = new FeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
		final byte[] binary = dataAdapter.toBinary();

		final FeatureDataAdapter dataAdapterCopy = new FeatureDataAdapter();
		dataAdapterCopy.fromBinary(binary);

		assertEquals(
				dataAdapterCopy.getAdapterId(),
				dataAdapter.getAdapterId());
		assertEquals(
				dataAdapterCopy.getFeatureType(),
				dataAdapter.getFeatureType());
		assertEquals(
				Boolean.TRUE,
				dataAdapterCopy.getFeatureType().getDescriptor(
						"pid").getUserData().get(
						"visibility"));

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

		final FeatureDataAdapter dataAdapter = new FeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));

		final List<IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> handlers = dataAdapter
				.getDefaultTypeMatchingHandlers(schema);
		boolean found = false;
		for (final IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object> handler : handlers) {
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

		final FeatureDataAdapter dataAdapter = new FeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
		final byte[] binary = dataAdapter.toBinary();

		final FeatureDataAdapter dataAdapterCopy = new FeatureDataAdapter();
		dataAdapterCopy.fromBinary(binary);

		assertEquals(
				dataAdapterCopy.getAdapterId(),
				dataAdapter.getAdapterId());
		assertEquals(
				dataAdapterCopy.getFeatureType(),
				dataAdapter.getFeatureType());
		assertEquals(
				Boolean.TRUE,
				dataAdapterCopy.getFeatureType().getDescriptor(
						"when").getUserData().get(
						"time"));

		final List<IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> handlers = dataAdapterCopy
				.getDefaultTypeMatchingHandlers(schema);
		boolean found = false;
		for (final IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object> handler : handlers) {
			found |= ((handler instanceof FeatureTimestampHandler) && ((((FeatureTimestampHandler) handler)
					.toIndexValue(
							newFeature)
					.toNumericData()
					.getMin() - time1.getTime()) < 0.001));
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

		final FeatureDataAdapter dataAdapter = new FeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
		final byte[] binary = dataAdapter.toBinary();

		final FeatureDataAdapter dataAdapterCopy = new FeatureDataAdapter();
		dataAdapterCopy.fromBinary(binary);

		assertEquals(
				dataAdapterCopy.getAdapterId(),
				dataAdapter.getAdapterId());
		assertEquals(
				dataAdapterCopy.getFeatureType(),
				dataAdapter.getFeatureType());
		assertEquals(
				Boolean.TRUE,
				dataAdapterCopy.getFeatureType().getDescriptor(
						"whennot").getUserData().get(
						"end"));
		assertEquals(
				Boolean.TRUE,
				dataAdapterCopy.getFeatureType().getDescriptor(
						"when").getUserData().get(
						"start"));

		final List<IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> handlers = dataAdapterCopy
				.getDefaultTypeMatchingHandlers(schema);
		boolean found = false;
		for (final IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object> handler : handlers) {
			found |= ((handler instanceof FeatureTimeRangeHandler)
					&& ((((FeatureTimeRangeHandler) handler).toIndexValue(
							newFeature).toNumericData().getMin() - time1.getTime()) < 0.001) && ((((FeatureTimeRangeHandler) handler)
					.toIndexValue(
							newFeature)
					.toNumericData()
					.getMax() - time2.getTime()) < 0.001));
		}

		assertTrue(found);
	}

	@Test
	public void testInferredRange()
			throws SchemaException {

		final SimpleFeatureType schema = DataUtilities.createType(
				"http://foo",
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,start:Date,end:Date,pid:String");

		final List<AttributeDescriptor> descriptors = schema.getAttributeDescriptors();
		final Object[] defaults = new Object[descriptors.size()];
		int p = 0;
		for (final AttributeDescriptor descriptor : descriptors) {
			defaults[p++] = descriptor.getDefaultValue();
		}

		final SimpleFeature newFeature = SimpleFeatureBuilder.build(
				schema,
				defaults,
				UUID.randomUUID().toString());

		newFeature.setAttribute(
				"pop",
				Long.valueOf(100));
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

		final FeatureDataAdapter dataAdapter = new FeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
		final byte[] binary = dataAdapter.toBinary();

		final FeatureDataAdapter dataAdapterCopy = new FeatureDataAdapter();
		dataAdapterCopy.fromBinary(binary);

		assertEquals(
				"http://foo",
				dataAdapterCopy.getFeatureType().getName().getNamespaceURI());

		assertEquals(
				dataAdapterCopy.getAdapterId(),
				dataAdapter.getAdapterId());
		assertEquals(
				dataAdapterCopy.getFeatureType(),
				dataAdapter.getFeatureType());
		assertEquals(
				Boolean.TRUE,
				dataAdapterCopy.getFeatureType().getDescriptor(
						"end").getUserData().get(
						"end"));
		assertEquals(
				Boolean.TRUE,
				dataAdapterCopy.getFeatureType().getDescriptor(
						"start").getUserData().get(
						"start"));

		final List<IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> handlers = dataAdapterCopy
				.getDefaultTypeMatchingHandlers(schema);
		boolean found = false;
		for (final IndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object> handler : handlers) {
			found |= ((handler instanceof FeatureTimeRangeHandler)
					&& ((((FeatureTimeRangeHandler) handler).toIndexValue(
							newFeature).toNumericData().getMin() - time1.getTime()) < 0.001) && ((((FeatureTimeRangeHandler) handler)
					.toIndexValue(
							newFeature)
					.toNumericData()
					.getMax() - time2.getTime()) < 0.001));
		}

		assertTrue(found);
	}

	@Test
	public void testCRSProjecttioin() {

		final SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
		typeBuilder.setName("test");
		typeBuilder.setCRS(GeoWaveGTDataStore.DEFAULT_CRS); // <- Coordinate
		// reference
		// add attributes in order
		typeBuilder.add(
				"geom",
				Point.class);
		typeBuilder.add(
				"name",
				String.class);
		typeBuilder.add(
				"count",
				Long.class);

		// build the type
		final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
				typeBuilder.buildFeatureType());

		final FeatureDataAdapter dataAdapter = new FeatureDataAdapter(
				builder.getFeatureType(),
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));

		final byte[] binary = dataAdapter.toBinary();

		final FeatureDataAdapter dataAdapterCopy = new FeatureDataAdapter();
		dataAdapterCopy.fromBinary(binary);

		assertEquals(
				dataAdapterCopy.getFeatureType().getCoordinateReferenceSystem().getCoordinateSystem(),
				GeoWaveGTDataStore.DEFAULT_CRS.getCoordinateSystem());
	}

	@Test
	public void testSecondaryIndicies()
			throws SchemaException {
		final SimpleFeatureType sfType = DataUtilities.createType(
				"stateCapitalData",
				"location:Geometry," + "city:String," + "state:String," + "since:Date," + "landArea:Double,"
						+ "munincipalPop:Integer," + "notes:String");
		final List<SimpleFeatureUserDataConfiguration> secondaryIndexConfigs = new ArrayList<>();
		secondaryIndexConfigs.add(new NumericSecondaryIndexConfiguration(
				"landArea",
				SecondaryIndexType.JOIN));
		secondaryIndexConfigs.add(new TextSecondaryIndexConfiguration(
				"notes",
				SecondaryIndexType.JOIN));
		secondaryIndexConfigs.add(new TemporalSecondaryIndexConfiguration(
				"since",
				SecondaryIndexType.JOIN));
		final SimpleFeatureUserDataConfigurationSet config = new SimpleFeatureUserDataConfigurationSet(
				sfType,
				secondaryIndexConfigs);
		config.updateType(sfType);
		final FeatureDataAdapter dataAdapter = new FeatureDataAdapter(
				sfType);
		Assert.assertTrue(dataAdapter.getSupportedSecondaryIndices().size() == 3);
	}

}
