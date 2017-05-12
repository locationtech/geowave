package mil.nga.giat.geowave.examples.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider.SpatialIndexBuilder;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is intended to provide a few examples on running Geowave queries
 * of different types: 1- Querying by polygon a set of points. 2- Filtering on
 * attributes of features using CQL queries 3- Ingesting polygons, and running
 * polygon intersect queries. You can check all points, geometries and query
 * accuracy in a more visual manner @ http://geojson.io/
 */
public class SpatialQueryExample
{
	private static Logger log = LoggerFactory.getLogger(SpatialQueryExample.class);

	// We'll use GeoWave's VectorDataStore, which allows to run CQL rich queries
	private static DataStore dataStore;
	// We need the AccumuloAdapterStore, which keeps a registry of adapter-ids,
	// used to be able to query specific "tables" or "types" of features.
	private static AdapterStore adapterStore;

	public static void main(
			String[] args )
			throws AccumuloSecurityException,
			AccumuloException,
			ParseException,
			CQLException,
			IOException {
		SpatialQueryExample example = new SpatialQueryExample();
		log.info("Setting up datastores");
		example.setupDataStores();
		log.info("Running point query examples");
		example.runPointExamples();
		log.info("Running polygon query examples");
		example.runPolygonExamples();
	}

	private static void setupDataStores()
			throws AccumuloSecurityException,
			AccumuloException {
		// Initialize VectorDataStore and AccumuloAdapterStore
		MockInstance instance = new MockInstance();
		// For the MockInstance we can user "user" - "password" as our
		// connection tokens
		Connector connector = instance.getConnector(
				"user",
				new PasswordToken(
						"password"));
		BasicAccumuloOperations operations = new BasicAccumuloOperations(
				connector);
		dataStore = new AccumuloDataStore(
				operations);
		adapterStore = new AccumuloAdapterStore(
				operations);
	}

	/**
	 * We'll run our point related operations. The data ingested and queried is
	 * single point based, meaning the index constructed will be based on a
	 * point.
	 */
	private void runPointExamples()
			throws ParseException,
			CQLException,
			IOException {
		ingestPointData();
		pointQueryCase1();
		pointQueryCase2();
		pointQueryCase3();
		pointQueryCase4();
	}

	private void ingestPointData() {
		log.info("Ingesting point data");
		ingestPointBasicFeature();
		ingestPointComplexFeature();
		log.info("Point data ingested");
	}

	private void ingest(
			FeatureDataAdapter adapter,
			PrimaryIndex index,
			List<SimpleFeature> features ) {
		try (IndexWriter indexWriter = dataStore.createWriter(
				adapter,
				index)) {
			for (SimpleFeature sf : features) {
				//
				indexWriter.write(sf);

			}
		}
		catch (IOException e) {
			log.error(
					"Could not create writter",
					e);
		}
	}

	private void ingestPointBasicFeature() {
		// First, we'll build our first kind of SimpleFeature, which we'll call
		// "basic-feature"
		// We need the type builder to build the feature type
		SimpleFeatureTypeBuilder sftBuilder = new SimpleFeatureTypeBuilder();
		// AttributeTypeBuilder for the attributes of the SimpleFeature
		AttributeTypeBuilder attrBuilder = new AttributeTypeBuilder();
		// Here we're setting the SimpleFeature name. Later on, we'll be able to
		// query GW just by this particular feature.
		sftBuilder.setName("basic-feature");
		// Add the attributes to the feature
		// Add the geometry attribute, which is mandatory for GeoWave to be able
		// to construct an index out of the SimpleFeature
		sftBuilder.add(attrBuilder.binding(
				Point.class).nillable(
				false).buildDescriptor(
				"geometry"));
		// Add another attribute just to be able to filter by it in CQL
		sftBuilder.add(attrBuilder.binding(
				String.class).nillable(
				false).buildDescriptor(
				"filter"));

		// Create the SimpleFeatureType
		SimpleFeatureType sfType = sftBuilder.buildFeatureType();
		// We need the adapter for all our operations with GeoWave
		FeatureDataAdapter sfAdapter = new FeatureDataAdapter(
				sfType);

		// Now we build the actual features. We'll create two points.
		// First point
		SimpleFeatureBuilder sfBuilder = new SimpleFeatureBuilder(
				sfType);
		sfBuilder.set(
				"geometry",
				GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
						-80.211181640625,
						25.848101000701597)));
		sfBuilder.set(
				"filter",
				"Basic-Stadium");
		// When calling buildFeature, we need to pass an unique id for that
		// feature, or it will be overwritten.
		SimpleFeature basicPoint1 = sfBuilder.buildFeature("1");

		// Construct the second feature.
		sfBuilder.set(
				"geometry",
				GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
						-80.191360,
						25.777804)));
		sfBuilder.set(
				"filter",
				"Basic-College");
		SimpleFeature basicPoint2 = sfBuilder.buildFeature("2");

		ArrayList<SimpleFeature> features = new ArrayList<SimpleFeature>();
		features.add(basicPoint1);
		features.add(basicPoint2);

		// Ingest the data. For that purpose, we need the feature adapter,
		// the index type (the default spatial index is used here),
		// and an iterator of SimpleFeature
		ingest(
				sfAdapter,
				new SpatialIndexBuilder().createIndex(),
				features);
	}

	/**
	 * We're going to ingest a more complete simple feature.
	 */
	private void ingestPointComplexFeature() {
		// First, we'll build our second kind of SimpleFeature, which we'll call
		// "complex-feature"
		// We need the type builder to build the feature type
		SimpleFeatureTypeBuilder sftBuilder = new SimpleFeatureTypeBuilder();
		// AttributeTypeBuilder for the attributes of the SimpleFeature
		AttributeTypeBuilder attrBuilder = new AttributeTypeBuilder();
		// Here we're setting the SimpleFeature name. Later on, we'll be able to
		// query GW just by this particular feature.
		sftBuilder.setName("complex-feature");
		// Add the attributes to the feature
		// Add the geometry attribute, which is mandatory for GeoWave to be able
		// to construct an index out of the SimpleFeature
		sftBuilder.add(attrBuilder.binding(
				Point.class).nillable(
				false).buildDescriptor(
				"geometry"));
		// Add another attribute just to be able to filter by it in CQL
		sftBuilder.add(attrBuilder.binding(
				String.class).nillable(
				false).buildDescriptor(
				"filter"));
		// Add more attributes to use with CQL filtering later on.
		sftBuilder.add(attrBuilder.binding(
				Double.class).nillable(
				false).buildDescriptor(
				"latitude"));
		sftBuilder.add(attrBuilder.binding(
				Double.class).nillable(
				false).buildDescriptor(
				"longitude"));

		// Create the SimpleFeatureType
		SimpleFeatureType sfType = sftBuilder.buildFeatureType();
		// We need the adapter for all our operations with GeoWave
		FeatureDataAdapter sfAdapter = new FeatureDataAdapter(
				sfType);

		// Now we build the actual features. We'll create two more points.
		// First point
		SimpleFeatureBuilder sfBuilder = new SimpleFeatureBuilder(
				sfType);
		sfBuilder.set(
				"geometry",
				GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
						-80.193388,
						25.780538)));
		sfBuilder.set(
				"filter",
				"Complex-Station");
		sfBuilder.set(
				"latitude",
				25.780538);
		sfBuilder.set(
				"longitude",
				-80.193388);
		// When calling buildFeature, we need to pass an unique id for that
		// feature, or it will be overwritten.
		SimpleFeature basicPoint1 = sfBuilder.buildFeature("1");

		// Construct the second feature.
		sfBuilder.set(
				"geometry",
				GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
						-118.26713562011719,
						33.988349152677955)));
		sfBuilder.set(
				"filter",
				"Complex-LA");
		sfBuilder.set(
				"latitude",
				33.988349152677955);
		sfBuilder.set(
				"longitude",
				-118.26713562011719);
		SimpleFeature basicPoint2 = sfBuilder.buildFeature("2");

		ArrayList<SimpleFeature> features = new ArrayList<SimpleFeature>();
		features.add(basicPoint1);
		features.add(basicPoint2);

		// Ingest the data. For that purpose, we need the feature adapter,
		// the index type (the default spatial index is used here),
		// and an iterator of SimpleFeature
		ingest(
				sfAdapter,
				new SpatialIndexBuilder().createIndex(),
				features);

		/**
		 * After ingest, a single point might look like this in Accumulo.
		 */
		// \x1F\x11\xCB\xFC\xB6\xEFT\x00\xFFcomplex_feature4\x00\x00\x00\x0E\x00\x00\x00\x01\x00\x00\x00\x00
		// complex_feature:filter [] Complex-LA
		// \x1F\x11\xCB\xFC\xB6\xEFT\x00\xFFcomplex_feature4\x00\x00\x00\x0E\x00\x00\x00\x01\x00\x00\x00\x00
		// complex_feature:geom\x00\x00 []
		// \x00\x00\x00\x00\x01\xC0]\x91\x18\xC0\x00\x00\x00@@\xFE\x829\x9B\xE3\xFC
		// \x1F\x11\xCB\xFC\xB6\xEFT\x00\xFFcomplex_feature4\x00\x00\x00\x0E\x00\x00\x00\x01\x00\x00\x00\x00
		// complex_feature:latitude [] @@\xFE\x829\x9B\xE3\xFC
		// \x1F\x11\xCB\xFC\xB6\xEFT\x00\xFFcomplex_feature\x00\x00\x00\x0E\x00\x00\x00\x01\x00\x00\x00\x00
		// complex_feature:longitude [] \xC0]\x91\x18\xC0\x00\x00\x
	}

	/**
	 * This query will search all points using the world's Bounding Box
	 */
	private void pointQueryCase1()
			throws ParseException,
			IOException {
		log.info("Running Point Query Case 1");
		// First, we need to obtain the adapter for the SimpleFeature we want to
		// query.
		// We'll query basic-feature in this example.
		// Obtain adapter for our "basic-feature" type
		ByteArrayId bfAdId = new ByteArrayId(
				"basic-feature");
		FeatureDataAdapter bfAdapter = (FeatureDataAdapter) adapterStore.getAdapter(bfAdId);

		// Define the geometry to query. We'll find all points that fall inside
		// that geometry
		String queryPolygonDefinition = "POLYGON (( " + "-180 -90, " + "-180 90, " + "180 90, " + "180 -90, "
				+ "-180 -90" + "))";
		Geometry queryPolygon = new WKTReader(
				JTSFactoryFinder.getGeometryFactory()).read(queryPolygonDefinition);

		// Perform the query.Parameters are
		/**
		 * 1- Adapter previously obtained from the feature name. 2- Default
		 * spatial index. 3- A SpatialQuery, which takes the query geometry -
		 * aka Bounding box 4- Filters. For this example, no filter is used. 5-
		 * Limit. Same as standard SQL limit. 0 is no limits. 6- Accumulo
		 * authorizations. For our mock instances, "root" works. In a real
		 * Accumulo setting, whatever authorization is associated to the user in
		 * question.
		 */

		final QueryOptions options = new QueryOptions(
				bfAdapter,
				new SpatialIndexBuilder().createIndex());
		options.setAuthorizations(new String[] {
			"root"
		});
		int count = 0;
		try (final CloseableIterator<SimpleFeature> iterator = dataStore.query(
				options,
				new SpatialQuery(
						queryPolygon))) {

			while (iterator.hasNext()) {
				SimpleFeature sf = iterator.next();
				log.info("Obtained SimpleFeature " + sf.getName().toString() + " - " + sf.getAttribute("filter"));
				count++;
				System.out.println("Query match: " + iterator.next().getID());
			}
			log.info("Should have obtained 2 features. -> " + (count == 2));
		}
	}

	/**
	 * This query will use a specific Bounding Box, and will find only 1 point.
	 */
	private void pointQueryCase2()
			throws ParseException,
			IOException {
		log.info("Running Point Query Case 2");
		// First, we need to obtain the adapter for the SimpleFeature we want to
		// query.
		// We'll query complex-feature in this example.
		// Obtain adapter for our "complex-feature" type
		ByteArrayId bfAdId = new ByteArrayId(
				"complex-feature");
		FeatureDataAdapter bfAdapter = (FeatureDataAdapter) adapterStore.getAdapter(bfAdId);

		// Define the geometry to query. We'll find all points that fall inside
		// that geometry.
		String queryPolygonDefinition = "POLYGON (( " + "-118.50059509277344 33.75688594085081, "
				+ "-118.50059509277344 34.1521587488017, " + "-117.80502319335938 34.1521587488017, "
				+ "-117.80502319335938 33.75688594085081, " + "-118.50059509277344 33.75688594085081" + "))";

		Geometry queryPolygon = new WKTReader(
				JTSFactoryFinder.getGeometryFactory()).read(queryPolygonDefinition);

		// Perform the query.Parameters are
		/**
		 * 1- Adapter previously obtained from the feature name. 2- Default
		 * spatial index. 3- A SpatialQuery, which takes the query geometry -
		 * aka Bounding box 4- Filters. For this example, no filter is used. 5-
		 * Limit. Same as standard SQL limit. 0 is no limits. 6- Accumulo
		 * authorizations. For our mock instances, "root" works. In a real
		 * Accumulo setting, whatever authorization is associated to the user in
		 * question.
		 */
		final QueryOptions options = new QueryOptions(
				bfAdapter,
				new SpatialIndexBuilder().createIndex(),
				new String[] {
					"root"
				});

		int count = 0;
		try (final CloseableIterator<SimpleFeature> iterator = dataStore.query(
				options,
				new SpatialQuery(
						queryPolygon))) {

			while (iterator.hasNext()) {
				SimpleFeature sf = iterator.next();
				log.info("Obtained SimpleFeature " + sf.getName().toString() + " - " + sf.getAttribute("filter"));
				count++;
				System.out.println("Query match: " + sf.getID());
			}
			log.info("Should have obtained 1 feature. -> " + (count == 1));
		}
	}

	/**
	 * This query will use the world's Bounding Box together with a CQL filter.
	 */
	private void pointQueryCase3()
			throws ParseException,
			CQLException,
			IOException {
		log.info("Running Point Query Case 3");
		// First, we need to obtain the adapter for the SimpleFeature we want to
		// query.
		// We'll query basic-feature in this example.
		// Obtain adapter for our "basic-feature" type
		ByteArrayId bfAdId = new ByteArrayId(
				"basic-feature");
		FeatureDataAdapter bfAdapter = (FeatureDataAdapter) adapterStore.getAdapter(bfAdId);

		String CQLFilter = "filter = 'Basic-Stadium'";
		// Perform the query.Parameters are
		/**
		 * 1- Adapter previously obtained from the feature name. 2- Default
		 * spatial index. 3- A SpatialQuery, which takes the query geometry -
		 * aka Bounding box 4- Filters. For this example, we reduce all returned
		 * points (2) by using a filter. 5- Limit. Same as standard SQL limit. 0
		 * is no limits. 6- Accumulo authorizations. For our mock instances,
		 * "root" works. In a real Accumulo setting, whatever authorization is
		 * associated to the user in question.
		 */
		final QueryOptions options = new QueryOptions(
				bfAdapter,
				new SpatialIndexBuilder().createIndex(),
				new String[] {
					"root"
				});

		int count = 0;
		try (final CloseableIterator<SimpleFeature> iterator = dataStore.query(
				options,
				CQLQuery.createOptimalQuery(
						CQLFilter,
						bfAdapter,
						options.getIndex()))) {

			// Our query would have found 2 points based only on the Bounding
			// Box, but using the
			// filter to match a particular attribute will reduce our result set
			// size to 1
			while (iterator.hasNext()) {
				SimpleFeature sf = iterator.next();
				log.info("Obtained SimpleFeature " + sf.getName().toString() + " - " + sf.getAttribute("filter"));
				count++;
				System.out.println("Query match: " + sf.getID());
			}
			log.info("Should have obtained 1 feature. " + (count == 1));
		}

	}

	/**
	 * This query will use the world's Bounding Box together with a more complex
	 * CQL filter.
	 */
	private void pointQueryCase4()
			throws ParseException,
			CQLException,
			IOException {
		log.info("Running Point Query Case 4");
		// First, we need to obtain the adapter for the SimpleFeature we want to
		// query.
		// We'll query complex-feature in this example.
		// Obtain adapter for our "complex-feature" type
		ByteArrayId bfAdId = new ByteArrayId(
				"complex-feature");
		FeatureDataAdapter bfAdapter = (FeatureDataAdapter) adapterStore.getAdapter(bfAdId);

		// This CQL query will yield a single point - Complex-LA
		String CQLFilter = "latitude > 25 AND longitude < -118";
		// Perform the query.Parameters are
		/**
		 * 1- Adapter previously obtained from the feature name. 2- Default
		 * spatial index. 3- A SpatialQuery, which takes the query geometry -
		 * aka Bounding box 4- Filters. For this example, we reduce all returned
		 * points (2) by using a filter. 5- Limit. Same as standard SQL limit. 0
		 * is no limits. 6- Accumulo authorizations. For our mock instances,
		 * "root" works. In a real Accumulo setting, whatever authorization is
		 * associated to the user in question.
		 */
		final QueryOptions options = new QueryOptions(
				bfAdapter,
				new SpatialIndexBuilder().createIndex(),
				new String[] {
					"root"
				});
		int count = 0;
		try (final CloseableIterator<SimpleFeature> iterator = dataStore.query(
				options,
				CQLQuery.createOptimalQuery(
						CQLFilter,
						bfAdapter,
						options.getIndex()))) {

			// Our query would have found 2 points based only on the Bounding
			// Box, but using the
			// filter to match a particular attribute will reduce our result set
			// size to 1
			while (iterator.hasNext()) {
				SimpleFeature sf = iterator.next();
				log.info("Obtained SimpleFeature " + sf.getName().toString() + " - " + sf.getAttribute("filter"));
				count++;
				System.out.println("Query match: " + sf.getID());
			}
			log.info("Should have obtained 1 feature. -> " + (count == 1));
		}
	}

	/**
	 * We'll run our polygon related operations. The data ingested and queried
	 * is single polygon based, meaning the index constructed will be based on a
	 * Geometry.
	 */
	private void runPolygonExamples()
			throws ParseException,
			IOException {
		ingestPolygonFeature();
		polygonQueryCase1();
	}

	private void ingestPolygonFeature()
			throws ParseException {
		log.info("Ingesting polygon data");
		// First, we'll build our third kind of SimpleFeature, which we'll call
		// "polygon-feature"
		// We need the type builder to build the feature type
		SimpleFeatureTypeBuilder sftBuilder = new SimpleFeatureTypeBuilder();
		// AttributeTypeBuilder for the attributes of the SimpleFeature
		AttributeTypeBuilder attrBuilder = new AttributeTypeBuilder();
		// Here we're setting the SimpleFeature name. Later on, we'll be able to
		// query GW just by this particular feature.
		sftBuilder.setName("polygon-feature");
		// Add the attributes to the feature
		// Add the geometry attribute, which is mandatory for GeoWave to be able
		// to construct an index out of the SimpleFeature
		// Will be any arbitrary geometry; in this case, a polygon.
		sftBuilder.add(attrBuilder.binding(
				Geometry.class).nillable(
				false).buildDescriptor(
				"geometry"));
		// Add another attribute just to be able to filter by it in CQL
		sftBuilder.add(attrBuilder.binding(
				String.class).nillable(
				false).buildDescriptor(
				"filter"));

		// Create the SimpleFeatureType
		SimpleFeatureType sfType = sftBuilder.buildFeatureType();
		// We need the adapter for all our operations with GeoWave
		FeatureDataAdapter sfAdapter = new FeatureDataAdapter(
				sfType);

		// Now we build the actual features. We'll create one polygon.
		// First point
		SimpleFeatureBuilder sfBuilder = new SimpleFeatureBuilder(
				sfType);

		// For ease of use, we'll create the polygon geometry with WKT format.
		String polygonDefinition = "POLYGON (( " + "-80.3045654296875 25.852426562716428, "
				+ "-80.123291015625 25.808545671771615, " + "-80.19195556640625 25.7244467526159, "
				+ "-80.34233093261719 25.772068899816585, " + "-80.3045654296875 25.852426562716428" + "))";
		Geometry geom = new WKTReader(
				JTSFactoryFinder.getGeometryFactory()).read(polygonDefinition);
		sfBuilder.set(
				"geometry",
				geom);
		sfBuilder.set(
				"filter",
				"Polygon");
		// When calling buildFeature, we need to pass an unique id for that
		// feature, or it will be overwritten.
		SimpleFeature polygon = sfBuilder.buildFeature("1");

		ArrayList<SimpleFeature> features = new ArrayList<SimpleFeature>();
		features.add(polygon);

		// Ingest the data. For that purpose, we need the feature adapter,
		// the index type (the default spatial index is used here),
		// and an iterator of SimpleFeature
		ingest(
				sfAdapter,
				new SpatialIndexBuilder().createIndex(),
				features);
		log.info("Polygon data ingested");
	}

	/**
	 * This query will find a polygon/polygon intersection, returning one match.
	 */
	private void polygonQueryCase1()
			throws ParseException,
			IOException {
		log.info("Running Point Query Case 4");
		// First, we need to obtain the adapter for the SimpleFeature we want to
		// query.
		// We'll query polygon-feature in this example.
		// Obtain adapter for our "polygon-feature" type
		ByteArrayId bfAdId = new ByteArrayId(
				"polygon-feature");
		FeatureDataAdapter bfAdapter = (FeatureDataAdapter) adapterStore.getAdapter(bfAdId);

		// Define the geometry to query. We'll find all polygons that intersect
		// with this geometry.
		String queryPolygonDefinition = "POLYGON (( " + "-80.4037857055664 25.81596330265488, "
				+ "-80.27915954589844 25.788144792391982, " + "-80.34370422363281 25.8814655232439, "
				+ "-80.44567108154297 25.896291175546626, " + "-80.4037857055664  25.81596330265488" + "))";

		Geometry queryPolygon = new WKTReader(
				JTSFactoryFinder.getGeometryFactory()).read(queryPolygonDefinition);

		// Perform the query.Parameters are
		/**
		 * 1- Adapter previously obtained from the feature name. 2- Default
		 * spatial index. 3- A SpatialQuery, which takes the query geometry -
		 * aka Bounding box 4- Filters. For this example, we don't use filters
		 * 5- Limit. Same as standard SQL limit. 0 is no limits. 6- Accumulo
		 * authorizations. For our mock instances, "root" works. In a real
		 * Accumulo setting, whatever authorization is associated to the user in
		 * question.
		 */

		final QueryOptions options = new QueryOptions(
				bfAdapter,
				new SpatialIndexBuilder().createIndex(),
				new String[] {
					"root"
				});
		int count = 0;
		try (final CloseableIterator<SimpleFeature> iterator = dataStore.query(
				options,
				new SpatialQuery(
						queryPolygon))) {

			while (iterator.hasNext()) {
				SimpleFeature sf = iterator.next();
				log.info("Obtained SimpleFeature " + sf.getName().toString() + " - " + sf.getAttribute("filter"));
				count++;
				System.out.println("Query match: " + sf.getID());
			}
			log.info("Should have obtained 1 feature. -> " + (count == 1));
		}
	}
}