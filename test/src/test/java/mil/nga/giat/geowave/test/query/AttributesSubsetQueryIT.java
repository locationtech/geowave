package mil.nga.giat.geowave.test.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.test.GeoWaveTestEnvironment;

import org.apache.log4j.Logger;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;

public class AttributesSubsetQueryIT extends
		GeoWaveTestEnvironment
{
	private static final Logger LOGGER = Logger.getLogger(AttributesSubsetQueryIT.class);

	private static SimpleFeatureType simpleFeatureType;
	private static FeatureDataAdapter dataAdapter;
	private static DataStore dataStore;

	private static final Index INDEX = IndexType.SPATIAL_VECTOR.createDefaultIndex();

	// constants for attributes of SimpleFeatureType
	private static final String CITY_ATTRIBUTE = "city";
	private static final String STATE_ATTRIBUTE = "state";
	private static final String POPULATION_ATTRIBUTE = "population";
	private static final String LAND_AREA_ATTRIBUTE = "landArea";
	private static final String GEOMETRY_ATTRIBUTE = "geometry";

	private static final Collection<String> ALL_ATTRIBUTES = Arrays.asList(
			CITY_ATTRIBUTE,
			STATE_ATTRIBUTE,
			POPULATION_ATTRIBUTE,
			LAND_AREA_ATTRIBUTE,
			GEOMETRY_ATTRIBUTE);

	// points used to construct bounding box for queries
	private static final Coordinate GUADALAJARA = new Coordinate(
			-103.3500,
			20.6667);
	private static final Coordinate ATLANTA = new Coordinate(
			-84.3900,
			33.7550);

	@BeforeClass
	public static void setup()
			throws IOException {

		GeoWaveTestEnvironment.setup();

		simpleFeatureType = getSimpleFeatureType();

		dataAdapter = new FeatureDataAdapter(
				simpleFeatureType);

		dataStore = new AccumuloDataStore(
				accumuloOperations);

		ingestSampleData();
	}

	@Test
	public void testResultsContainAllAttributes()
			throws IOException {

		final CloseableIterator<SimpleFeature> matches = dataStore.query(
				INDEX,
				new SpatialQuery(
						GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(
								GUADALAJARA,
								ATLANTA))));

		// query expects to match 3 cities from Texas, which should each contain
		// non-null values for each SimpleFeature attribute
		verifyResults(
				matches,
				3,
				ALL_ATTRIBUTES);
	}

	@Test
	public void testResultsContainCityOnly()
			throws IOException {

		final List<String> attributesSubset = Arrays.asList(CITY_ATTRIBUTE);

		final CloseableIterator<SimpleFeature> results = dataStore.query(
				INDEX,
				new SpatialQuery(
						GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(
								GUADALAJARA,
								ATLANTA))),
				new QueryOptions(
						attributesSubset));

		// query expects to match 3 cities from Texas, which should each contain
		// non-null values for a subset of attributes (city) and nulls for the
		// rest
		verifyResults(
				results,
				3,
				attributesSubset);
	}

	@Test
	public void testResultsContainCityAndPopulation()
			throws IOException {

		final List<String> attributesSubset = Arrays.asList(
				CITY_ATTRIBUTE,
				POPULATION_ATTRIBUTE);

		final CloseableIterator<SimpleFeature> results = dataStore.query(
				INDEX,
				new SpatialQuery(
						GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(
								GUADALAJARA,
								ATLANTA))),
				new QueryOptions(
						attributesSubset));

		// query expects to match 3 cities from Texas, which should each contain
		// non-null values for a subset of attributes (city, population) and
		// nulls for the rest
		verifyResults(
				results,
				3,
				attributesSubset);
	}

	private void verifyResults(
			final CloseableIterator<SimpleFeature> results,
			final int numExpectedResults,
			final Collection<String> attributesExpected )
			throws IOException {

		int numResults = 0;
		SimpleFeature currentFeature;
		Object currentAttributeValue;

		while (results.hasNext()) {

			currentFeature = results.next();
			numResults++;

			for (String currentAttribute : ALL_ATTRIBUTES) {

				currentAttributeValue = currentFeature.getAttribute(currentAttribute);

				if (attributesExpected.contains(currentAttribute) || currentAttribute.equals(GEOMETRY_ATTRIBUTE)) {
					// geometry will always be included since indexed by spatial
					// dimensionality
					Assert.assertNotNull(
							"Expected non-null " + currentAttribute + " value!",
							currentAttributeValue);
				}
				else {
					Assert.assertNull(
							"Expected null " + currentAttribute + " value!",
							currentAttributeValue);
				}
			}
		}

		results.close();

		Assert.assertEquals(
				"Unexpected number of query results",
				numExpectedResults,
				numResults);
	}

	private static SimpleFeatureType getSimpleFeatureType() {

		SimpleFeatureType type = null;

		try {
			type = DataUtilities.createType(
					"testCityData",
					CITY_ATTRIBUTE + ":String," + STATE_ATTRIBUTE + ":String," + POPULATION_ATTRIBUTE + ":Double," + LAND_AREA_ATTRIBUTE + ":Double," + GEOMETRY_ATTRIBUTE + ":Geometry");
		}
		catch (SchemaException e) {
			LOGGER.error(
					"Unable to create SimpleFeatureType",
					e);
		}

		return type;
	}

	private static void ingestSampleData() {

		LOGGER.info("Ingesting canned data...");

		dataStore.ingest(
				dataAdapter,
				INDEX,
				buildCityDataSet().iterator());

		LOGGER.info("Ingest complete.");
	}

	private static List<SimpleFeature> buildCityDataSet() {

		final List<SimpleFeature> points = new ArrayList<>();

		// http://en.wikipedia.org/wiki/List_of_United_States_cities_by_population
		points.add(buildSimpleFeature(
				"New York",
				"New York",
				8405837,
				302.6,
				new Coordinate(
						-73.9385,
						40.6643)));
		points.add(buildSimpleFeature(
				"Los Angeles",
				"California",
				3884307,
				468.7,
				new Coordinate(
						-118.4108,
						34.0194)));
		points.add(buildSimpleFeature(
				"Chicago",
				"Illinois",
				2718782,
				227.6,
				new Coordinate(
						-87.6818,
						41.8376)));
		points.add(buildSimpleFeature(
				"Houston",
				"Texas",
				2195914,
				599.6,
				new Coordinate(
						-95.3863,
						29.7805)));
		points.add(buildSimpleFeature(
				"Philadelphia",
				"Pennsylvania",
				1553165,
				134.1,
				new Coordinate(
						-75.1333,
						40.0094)));
		points.add(buildSimpleFeature(
				"Phoenix",
				"Arizona",
				1513367,
				516.7,
				new Coordinate(
						-112.088,
						33.5722)));
		points.add(buildSimpleFeature(
				"San Antonio",
				"Texas",
				1409019,
				460.9,
				new Coordinate(
						-98.5251,
						29.4724)));
		points.add(buildSimpleFeature(
				"San Diego",
				"California",
				1355896,
				325.2,
				new Coordinate(
						-117.135,
						32.8153)));
		points.add(buildSimpleFeature(
				"Dallas",
				"Texas",
				1257676,
				340.5,
				new Coordinate(
						-96.7967,
						32.7757)));
		points.add(buildSimpleFeature(
				"San Jose",
				"California",
				998537,
				176.5,
				new Coordinate(
						-121.8193,
						37.2969)));

		return points;
	}

	private static SimpleFeature buildSimpleFeature(
			String city,
			String state,
			double population,
			double landArea,
			Coordinate coordinate ) {

		final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
				simpleFeatureType);

		builder.set(
				CITY_ATTRIBUTE,
				city);
		builder.set(
				STATE_ATTRIBUTE,
				state);
		builder.set(
				POPULATION_ATTRIBUTE,
				population);
		builder.set(
				LAND_AREA_ATTRIBUTE,
				landArea);
		builder.set(
				GEOMETRY_ATTRIBUTE,
				GeometryUtils.GEOMETRY_FACTORY.createPoint(coordinate));

		return builder.buildFeature(UUID.randomUUID().toString());
	}

}
