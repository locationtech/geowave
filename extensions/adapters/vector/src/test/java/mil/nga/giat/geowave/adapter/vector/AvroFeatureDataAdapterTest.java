package mil.nga.giat.geowave.adapter.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.PrecisionModel;

import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveGTDataStore;
import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;
import mil.nga.giat.geowave.adapter.vector.utils.DateUtilities;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider.SpatialIndexBuilder;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.IndexFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.MemoryStoreFactoryFamily;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

public class AvroFeatureDataAdapterTest
{

	private SimpleFeatureType schema;
	private SimpleFeature newFeature;
	private Date time1;
	private Date time2;

	private static DataStore dataStore;
	GeometryFactory factory = new GeometryFactory(
			new PrecisionModel(
					PrecisionModel.FIXED));

	@SuppressWarnings("unchecked")
	@Before
	public void setup()
			throws SchemaException,
			CQLException,
			ParseException {

		final StoreFactoryFamilySpi storeFactoryFamily = new MemoryStoreFactoryFamily();
		StoreFactoryOptions opts = storeFactoryFamily.getDataStoreFactory().createOptionsInstance();
		opts.setGeowaveNamespace("test_avro");
		dataStore = storeFactoryFamily.getDataStoreFactory().createStore(
				opts);

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
		catch (final Exception e) {
			e.printStackTrace();
		}
	}

	private static void ingestCannedData(
			final FeatureDataAdapter adapter,
			final List<SimpleFeature> data )
			throws IOException {

		final PrimaryIndex index = new SpatialIndexBuilder().createIndex();

		try (IndexWriter indexWriter = dataStore.createWriter(
				adapter,
				index)) {
			for (final SimpleFeature sf : data) {
				indexWriter.write(
						sf,
						DataStoreUtils.UNCONSTRAINED_VISIBILITY);

			}
		}

		System.out.println("Ingest complete.");
	}

	@Test
	public void basicTest()
			throws Exception {

		final AvroFeatureDataAdapter adapter = new AvroFeatureDataAdapter(
				schema);

		final GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
		final SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(
				schema);

		final int numFeatures = 10;

		final List<SimpleFeature> data = new ArrayList<SimpleFeature>();
		final Map<Integer, SimpleFeature> dataMap = new HashMap<Integer, SimpleFeature>();
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

			data.add(featureBuilder.buildFeature(Integer.toString(i)));
			dataMap.put(
					i,
					data.get(data.size() - 1));
		}

		ingestCannedData(
				adapter,
				data);

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
		try (final CloseableIterator<SimpleFeature> itr = dataStore.query(
				new QueryOptions(
						adapter,
						new SpatialIndexBuilder().createIndex()),
				new SpatialQuery(
						new GeometryFactory().createPolygon(coordArray)))) {

			while (itr.hasNext()) {
				final SimpleFeature feat = itr.next();

				final SimpleFeature feature = dataMap.remove(Integer.parseInt(feat.getID()));
				assertEquals(
						feature,
						feat);

			}

			assertTrue(dataMap.isEmpty());
		}

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
		final CoordinateReferenceSystem crs = dataAdapter.getFeatureType().getCoordinateReferenceSystem();
		assertTrue(crs.getIdentifiers().toString().contains(
				"EPSG:4326"));
		@SuppressWarnings("unchecked")
		final SimpleFeature originalFeature = FeatureDataUtils.buildFeature(
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
				originalFeature,
				new SpatialIndexBuilder().createIndex().getIndexModel());

		final IndexedAdapterPersistenceEncoding encoding = new IndexedAdapterPersistenceEncoding(
				dataAdapter.getAdapterId(),
				persistenceEncoding.getDataId(),
				null,
				1,
				persistenceEncoding.getCommonData(),
				new PersistentDataset<byte[]>(),
				persistenceEncoding.getAdapterExtendedData());

		final SimpleFeature decodedFeature = dataAdapter.decode(
				encoding,
				new PrimaryIndex(
						null, // because we know the feature data adapter
						// doesn't use the numeric index strategy
						// and only the common index model to decode
						// the simple feature, we pass along a null
						// strategy to eliminate the necessity to
						// send a serialization of the strategy in
						// the options of this iterator
						new SpatialIndexBuilder().createIndex().getIndexModel()));

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

		final AvroFeatureDataAdapter dataAdapter = new AvroFeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
		final byte[] binary = dataAdapter.toBinary();

		final AvroFeatureDataAdapter dataAdapterCopy = new AvroFeatureDataAdapter();
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

		final AvroFeatureDataAdapter dataAdapter = new AvroFeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
		final byte[] binary = dataAdapter.toBinary();

		final AvroFeatureDataAdapter dataAdapterCopy = new AvroFeatureDataAdapter();
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

		final AvroFeatureDataAdapter dataAdapter = new AvroFeatureDataAdapter(
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

		final AvroFeatureDataAdapter dataAdapter = new AvroFeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
		final byte[] binary = dataAdapter.toBinary();

		final AvroFeatureDataAdapter dataAdapterCopy = new AvroFeatureDataAdapter();
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

		final AvroFeatureDataAdapter dataAdapter = new AvroFeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
		final byte[] binary = dataAdapter.toBinary();

		final AvroFeatureDataAdapter dataAdapterCopy = new AvroFeatureDataAdapter();
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

		final AvroFeatureDataAdapter dataAdapter = new AvroFeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
		final byte[] binary = dataAdapter.toBinary();

		final AvroFeatureDataAdapter dataAdapterCopy = new AvroFeatureDataAdapter();
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

		final AvroFeatureDataAdapter dataAdapter = new AvroFeatureDataAdapter(
				builder.getFeatureType(),
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));

		final byte[] binary = dataAdapter.toBinary();

		final AvroFeatureDataAdapter dataAdapterCopy = new AvroFeatureDataAdapter();
		dataAdapterCopy.fromBinary(binary);

		assertEquals(
				dataAdapterCopy.getFeatureType().getCoordinateReferenceSystem().getCoordinateSystem(),
				GeoWaveGTDataStore.DEFAULT_CRS.getCoordinateSystem());
	}
}