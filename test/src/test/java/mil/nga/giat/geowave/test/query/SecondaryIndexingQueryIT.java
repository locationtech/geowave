package mil.nga.giat.geowave.test.query;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.index.NumericSecondaryIndexConfiguration;
import mil.nga.giat.geowave.adapter.vector.index.TemporalSecondaryIndexConfiguration;
import mil.nga.giat.geowave.adapter.vector.index.TextSecondaryIndexConfiguration;
import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfiguration;
import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfigurationSet;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.index.FilterableConstraints;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexQueryManager;
import mil.nga.giat.geowave.core.store.index.numeric.NumericGreaterThanOrEqualToConstraint;
import mil.nga.giat.geowave.core.store.index.numeric.NumericIndexStrategy;
import mil.nga.giat.geowave.core.store.index.temporal.TemporalIndexStrategy;
import mil.nga.giat.geowave.core.store.index.text.TextIndexStrategy;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.test.GeoWaveTestEnvironment;

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

public class SecondaryIndexingQueryIT extends
		GeoWaveTestEnvironment
{
	private static String BASE_DIR = "/src/test/resources/mil/nga/giat/geowave/test/query/";
	private static String FILE = "stateCapitals.csv";
	private static String TYPE_NAME = "stateCapitalData";
	private static SimpleFeatureType schema;
	private static FeatureDataAdapter dataAdapter;
	private static DataStore dataStore;
	private static PrimaryIndex index;
	private static Coordinate CHARLESTON = new Coordinate(
			-79.9704779,
			32.8210454);
	private static Coordinate MILWAUKEE = new Coordinate(
			-87.96743,
			43.0578914);

	@BeforeClass
	public static void initTest()
			throws SchemaException,
			IOException {

		// create SimpleFeatureType
		schema = DataUtilities.createType(
				TYPE_NAME,
				"location:Geometry," + "city:String," + "state:String," + "since:Date," + "landArea:Double," + "munincipalPop:Integer," + "notes:String");

		// mark attributes for secondary indexing:
		final List<SimpleFeatureUserDataConfiguration> secondaryIndexingConfigs = new ArrayList<>();
		// numeric 2nd-idx on attributes "landArea", "munincipalPop", "metroPop"
		secondaryIndexingConfigs.add(new NumericSecondaryIndexConfiguration(
				new HashSet<String>(
						Arrays.asList(
								"landArea",
								"munincipalPop"))));
		// temporal 2nd-idx on attribute "since"
		secondaryIndexingConfigs.add(new TemporalSecondaryIndexConfiguration(
				"since"));
		// text-based 2nd-idx on attribute "notes"
		secondaryIndexingConfigs.add(new TextSecondaryIndexConfiguration(
				"notes"));
		// update schema with 2nd-idx configs
		final SimpleFeatureUserDataConfigurationSet config = new SimpleFeatureUserDataConfigurationSet(
				schema,
				secondaryIndexingConfigs);
		config.updateType(schema);

		dataAdapter = new FeatureDataAdapter(
				schema);
		dataStore = new AccumuloDataStore(
				new AccumuloIndexStore(
						accumuloOperations),
				new AccumuloAdapterStore(
						accumuloOperations),
				new AccumuloDataStatisticsStore(
						accumuloOperations),
				new AccumuloSecondaryIndexDataStore(
						accumuloOperations),
				accumuloOperations);
		index = DEFAULT_SPATIAL_INDEX;

		final List<SimpleFeature> features = loadStateCapitalData();

		try (IndexWriter writer = dataStore.createIndexWriter(
				index,
				DataStoreUtils.DEFAULT_VISIBILITY)) {
			for (final SimpleFeature aFeature : features) {
				writer.write(
						dataAdapter,
						aFeature);
			}
		}

		System.out.println("Data ingest complete.");
	}

	@Test
	public void testWithNoAdditionalConstraints()
			throws IOException {
		final Query query = new SpatialQuery(
				GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(
						CHARLESTON,
						MILWAUKEE)));
		int numMatches = 0;
		final CloseableIterator<SimpleFeature> matches = dataStore.query(
				new QueryOptions(
						index),
				query);
		while (matches.hasNext()) {
			final SimpleFeature currFeature = matches.next();
			if (currFeature.getFeatureType().getTypeName().equals(
					TYPE_NAME)) {
				numMatches++;
			}
		}
		matches.close();
		Assert.assertTrue(numMatches == 8);
	}

	@Test
	public void testWithAdditionalConstraints()
			throws IOException {
		final Map<ByteArrayId, FilterableConstraints> additionalConstraints = new HashMap<>();
		final ByteArrayId byteArray = new ByteArrayId(
				StringUtils.stringToBinary("landArea"));
		additionalConstraints.put(
				byteArray,
				new NumericGreaterThanOrEqualToConstraint(
						byteArray,
						100));
		final Query query = new SpatialQuery(
				GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(
						CHARLESTON,
						MILWAUKEE)),
				additionalConstraints);
		final SecondaryIndexQueryManager secondaryIndexQueryManager = new SecondaryIndexQueryManager(
				new AccumuloSecondaryIndexDataStore(
						accumuloOperations));
		for (final SecondaryIndex<?> secondaryIndex : dataAdapter.getSupportedSecondaryIndices()) {
			final CloseableIterator<ByteArrayId> matches = secondaryIndexQueryManager.query(
					(BasicQuery) query,
					secondaryIndex,
					index,
					new String[0]);
			System.out.println("Iterating matches for " + StringUtils.stringFromBinary(secondaryIndex.getId().getBytes()));
			int numMatches = 0;
			while (matches.hasNext()) {
				numMatches++;
				System.out.println(matches.next());
			}
			matches.close();
			System.out.println("Found " + numMatches + " matches");
			// TEMPORARY
			if (secondaryIndex.getIndexStrategy() instanceof NumericIndexStrategy) {
				Assert.assertTrue(numMatches == 16);
			}
			else if (secondaryIndex.getIndexStrategy() instanceof TemporalIndexStrategy) {
				Assert.assertTrue(numMatches == 0);
			}
			else if (secondaryIndex.getIndexStrategy() instanceof TextIndexStrategy) {
				Assert.assertTrue(numMatches == 0);
			}
		}
	}

	public static List<SimpleFeature> loadStateCapitalData()
			throws FileNotFoundException,
			IOException {
		final List<SimpleFeature> features = new ArrayList<>();
		final String fileName = System.getProperty("user.dir") + BASE_DIR + FILE;
		try (final BufferedReader br = new BufferedReader(
				new FileReader(
						fileName))) {
			for (String line = br.readLine(); line != null; line = br.readLine()) {
				final String[] vals = line.split(",");
				final String state = vals[0];
				final String city = vals[1];
				final double lng = Double.parseDouble(vals[2]);
				final double lat = Double.parseDouble(vals[3]);
				@SuppressWarnings("deprecation")
				final Date since = new Date(
						Integer.parseInt(vals[4]) - 1900,
						0,
						1);
				final double landArea = Double.parseDouble(vals[5]);
				final int munincipalPop = Integer.parseInt(vals[6]);
				final String notes = (vals.length > 7) ? vals[7] : null; // FIXME
																			// weird
																			// chars
																			// in
																			// notes
																			// (clean
																			// CSV
																			// text)
				features.add(buildSimpleFeature(
						state,
						city,
						lng,
						lat,
						since,
						landArea,
						munincipalPop,
						notes));
			}
		}
		return features;
	}

	private static SimpleFeature buildSimpleFeature(
			final String state,
			final String city,
			final double lng,
			final double lat,
			final Date since,
			final double landArea,
			final int munincipalPop,
			final String notes ) {
		final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
				schema);
		builder.set(
				"location",
				GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
						lng,
						lat)));
		builder.set(
				"state",
				state);
		builder.set(
				"city",
				city);
		builder.set(
				"since",
				since);
		builder.set(
				"landArea",
				landArea);
		builder.set(
				"munincipalPop",
				munincipalPop);
		builder.set(
				"notes",
				notes);
		return builder.buildFeature(UUID.randomUUID().toString());
	}

}