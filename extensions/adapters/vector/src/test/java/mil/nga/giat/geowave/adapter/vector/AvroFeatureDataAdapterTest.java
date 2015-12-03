package mil.nga.giat.geowave.adapter.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveGTDataStore;
import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;
import mil.nga.giat.geowave.adapter.vector.utils.DateUtilities;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.IndexFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.lang3.tuple.Pair;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.google.common.io.Files;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.PrecisionModel;

public class AvroFeatureDataAdapterTest
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
			throws AccumuloException,
			AccumuloSecurityException,
			SchemaException,
			CQLException,
			ParseException {

		try {
			time1 = DateUtilities.parseISO("2005-05-19T18:33:55Z");
			time2 = DateUtilities.parseISO("2005-05-19T19:33:55Z");

			schema = DataUtilities.createType(
					"sp.geostuff",
					"geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,whennot:Date,pid:String");// typeBuilder.buildFeatureType();

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
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void basicTest()
			throws Exception {
		final String password = "password";

		final MiniAccumuloConfig config = new MiniAccumuloConfig(
				Files.createTempDir(),
				password);
		config.setNumTservers(1);
		MiniAccumuloCluster miniAccumulo = new MiniAccumuloCluster(
				config);
		miniAccumulo.start();

		final String zookeeperUrl = miniAccumulo.getZooKeepers();
		final String instancename = miniAccumulo.getInstanceName();
		final String username = "root";
		final String tableNamespace = "geowave";

		final AccumuloOperations operations = new BasicAccumuloOperations(
				zookeeperUrl,
				instancename,
				username,
				password,
				tableNamespace);

		final AccumuloDataStore dataStore = new AccumuloDataStore(
				operations);

		final AvroFeatureDataAdapter adapter = new AvroFeatureDataAdapter(
				schema);

		final GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
		final SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(
				schema);
		final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();

		final int numFeatures = 10;

		// Write data using the whole feature data adapter
		for (int i = 0; i < numFeatures; i++) {

			final Point point = geometryFactory.createPoint(new Coordinate(
					i,
					i));

			featureBuilder.set(
					"geometry",
					point);
			featureBuilder.set(
					"pop",
					i);
			featureBuilder.set(
					"when",
					new Date(
							0));
			featureBuilder.set(
					"whennot",
					new Date());

			dataStore.ingest(
					adapter,
					index,
					featureBuilder.buildFeature(Integer.toString(i)));
		}

		final Coordinate[] coordArray = new Coordinate[5];
		coordArray[0] = new Coordinate(
				-180,
				-90);
		coordArray[1] = new Coordinate(
				180,
				-90);
		coordArray[2] = new Coordinate(
				180,
				90);
		coordArray[3] = new Coordinate(
				-180,
				90);
		coordArray[4] = new Coordinate(
				-180,
				-90);

		// read data using the whole feature data adapter
		final CloseableIterator<SimpleFeature> itr = dataStore.query(
				adapter,
				index,
				new SpatialQuery(
						new GeometryFactory().createPolygon(coordArray)));

		int numReturned = 0;
		while (itr.hasNext()) {
			final SimpleFeature feat = itr.next();

			assertTrue(Integer.parseInt(feat.getID()) == numReturned);
			assertTrue(((Point) feat.getAttribute("geometry")).getX() == numReturned);
			assertTrue(((Point) feat.getAttribute("geometry")).getY() == numReturned);
			assertTrue((Long) feat.getAttribute("pop") == numReturned);
			assertTrue(((Date) feat.getAttribute("when")).equals(new Date(
					0)));

			numReturned++;
		}

		assertTrue(numReturned == numFeatures);

		miniAccumulo.stop();
	}

	@Test
	public void testDifferentProjection()
			throws SchemaException {
		final SimpleFeatureType schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long");

		final AvroFeatureDataAdapter dataAdapter = new AvroFeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
		CoordinateReferenceSystem crs = dataAdapter.getType().getCoordinateReferenceSystem();
		assertTrue(crs.getIdentifiers().toString().contains(
				"EPSG:4326"));
		@SuppressWarnings("unchecked")
		SimpleFeature originalFeature = FeatureDataUtils.buildFeature(
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

		AdapterPersistenceEncoding persistenceEncoding = dataAdapter.encode(
				originalFeature,
				IndexType.SPATIAL_VECTOR.getDefaultIndexModel());

		final IndexedAdapterPersistenceEncoding encoding = new IndexedAdapterPersistenceEncoding(
				dataAdapter.getAdapterId(),
				persistenceEncoding.getDataId(),
				null,
				1,
				persistenceEncoding.getCommonData(),
				persistenceEncoding.getAdapterExtendedData());

		final SimpleFeature decodedFeature = dataAdapter.decode(
				encoding,
				new Index(
						null, // because we know the feature data adapter
						// doesn't use the numeric index strategy
						// and only the common index model to decode
						// the simple feature, we pass along a null
						// strategy to eliminate the necessity to
						// send a serialization of the strategy in
						// the options of this iterator
						IndexType.SPATIAL_VECTOR.getDefaultIndexModel()));

		assertTrue(originalFeature.getID().equals(
				decodedFeature.getID()));
		assertTrue(originalFeature.getAttributeCount() == decodedFeature.getAttributeCount());
		for (int i = 0; i < originalFeature.getAttributes().size(); i++) {
			assertTrue(originalFeature.getAttribute(
					i).equals(
					decodedFeature.getAttribute(i)));
		}
	}

	@Test
	public void testSingleTime() {
		schema.getDescriptor(
				"when").getUserData().clear();
		schema.getDescriptor(
				"whennot").getUserData().put(
				"time",
				Boolean.TRUE);

		AvroFeatureDataAdapter dataAdapter = new AvroFeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
		byte[] binary = dataAdapter.toBinary();

		AvroFeatureDataAdapter dataAdapterCopy = new AvroFeatureDataAdapter();
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

		AvroFeatureDataAdapter dataAdapter = new AvroFeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
		byte[] binary = dataAdapter.toBinary();

		AvroFeatureDataAdapter dataAdapterCopy = new AvroFeatureDataAdapter();
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

		AvroFeatureDataAdapter dataAdapter = new AvroFeatureDataAdapter(
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

		AvroFeatureDataAdapter dataAdapter = new AvroFeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
		byte[] binary = dataAdapter.toBinary();

		AvroFeatureDataAdapter dataAdapterCopy = new AvroFeatureDataAdapter();
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

		AvroFeatureDataAdapter dataAdapter = new AvroFeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
		byte[] binary = dataAdapter.toBinary();

		AvroFeatureDataAdapter dataAdapterCopy = new AvroFeatureDataAdapter();
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

		AvroFeatureDataAdapter dataAdapter = new AvroFeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
		byte[] binary = dataAdapter.toBinary();

		AvroFeatureDataAdapter dataAdapterCopy = new AvroFeatureDataAdapter();
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
		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
				typeBuilder.buildFeatureType());

		AvroFeatureDataAdapter dataAdapter = new AvroFeatureDataAdapter(
				builder.getFeatureType(),
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));

		byte[] binary = dataAdapter.toBinary();

		AvroFeatureDataAdapter dataAdapterCopy = new AvroFeatureDataAdapter();
		dataAdapterCopy.fromBinary(binary);

		assertEquals(
				dataAdapterCopy.getType().getCoordinateReferenceSystem().getCoordinateSystem(),
				GeoWaveGTDataStore.DEFAULT_CRS.getCoordinateSystem());
	}
}