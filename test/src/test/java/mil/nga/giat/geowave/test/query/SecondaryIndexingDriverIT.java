package mil.nga.giat.geowave.test.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.index.NumericSecondaryIndexConfiguration;
import mil.nga.giat.geowave.adapter.vector.index.TemporalSecondaryIndexConfiguration;
import mil.nga.giat.geowave.adapter.vector.index.TextSecondaryIndexConfiguration;
import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfiguration;
import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfigurationSet;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.plugins.DataStorePluginOptions;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

/**
 * This class is currently a dirty test harness used to sanity check changes as
 * I go. It will likely be rewritten/replaced by a much more sophisticated
 * integration test for secondary indexing once the capability matures
 * 
 */
@RunWith(GeoWaveITRunner.class)
public class SecondaryIndexingDriverIT

{
	private static SimpleFeatureType schema;
	private static Random random = new Random();
	private static int NUM_FEATURES = 10;

	@GeoWaveTestStore({
		GeoWaveStoreType.ACCUMULO
	})
	protected DataStorePluginOptions dataStoreOptions;

	@Before
	public void initializeTest()
			throws IOException,
			SchemaException,
			AccumuloException,
			AccumuloSecurityException {

		// see https://github.com/ngageoint/geowave/wiki/Secondary-Indexing
		schema = DataUtilities.createType(
				"cannedData",
				"location:Geometry,persons:Double,record_date:Date,income_category:String,affiliation:String");

		// mark attributes for secondary indexing
		final List<SimpleFeatureUserDataConfiguration> secondaryIndexingConfigs = new ArrayList<>();
		secondaryIndexingConfigs.add(new NumericSecondaryIndexConfiguration(
				"persons"));
		secondaryIndexingConfigs.add(new TemporalSecondaryIndexConfiguration(
				"record_date"));
		secondaryIndexingConfigs.add(new TextSecondaryIndexConfiguration(
				"affiliation"));
		// update schema with 2nd-idx configs
		final SimpleFeatureUserDataConfigurationSet config = new SimpleFeatureUserDataConfigurationSet(
				schema,
				secondaryIndexingConfigs);
		config.updateType(schema);

		final FeatureDataAdapter dataAdapter = new FeatureDataAdapter(
				schema);

		final DataStore dataStore = dataStoreOptions.createDataStore();

		final PrimaryIndex index = TestUtils.DEFAULT_SPATIAL_INDEX;

		final List<SimpleFeature> features = new ArrayList<>();
		for (int x = 0; x < NUM_FEATURES; x++) {
			features.add(buildSimpleFeature());
		}

		try (IndexWriter writer = dataStore.createWriter(
				dataAdapter,
				index)) {
			for (final SimpleFeature aFeature : features) {
				writer.write(aFeature);
			}
		}

		System.out.println("Feature(s) ingested");

		// TODO query

	}

	@After
	public void deleteTestData()
			throws IOException {
		TestUtils.deleteAll(dataStoreOptions);
	}

	@Test
	public void test()
			throws AccumuloException,
			AccumuloSecurityException,
			TableNotFoundException {
		// text "a few words" produces 33 unique n-grams of length 2-4
		// 12 bi-grams, 11 tri-grams, 10 quadrigrams
		final int numTrigrams = 33;
		final int indexEntriesPerKey = 2;
		final int numNumericEntries = countNumberOfEntriesInIndexTable("GEOWAVE_2ND_IDX_NUMERIC");
		final int numTemporalEntries = countNumberOfEntriesInIndexTable("GEOWAVE_2ND_IDX_TEMPORAL");
		final int numTextEntries = countNumberOfEntriesInIndexTable("GEOWAVE_2ND_IDX_NGRAM_2_4");
		// all features have the same affiliation text
		Assert.assertTrue(numNumericEntries == (NUM_FEATURES * indexEntriesPerKey));
		Assert.assertTrue(numTemporalEntries == (NUM_FEATURES * indexEntriesPerKey));
		Assert.assertTrue(numTextEntries == (NUM_FEATURES * numTrigrams * indexEntriesPerKey));
	}

	private int countNumberOfEntriesInIndexTable(
			final String tableName )
			throws TableNotFoundException {
		// TODO can't use accumulo scanner directly, this test must be
		// refactored

		// final Scanner scanner = accumuloOperations.createScanner(
		// tableName);
		// int numEntries = 0;
		// for (@SuppressWarnings("unused")
		// final Entry<Key, Value> kv : scanner) {
		// numEntries++;
		// }
		// scanner.close();
		// return numEntries;
		return -1;
	}

	private static SimpleFeature buildSimpleFeature() {

		final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
				schema);

		final int randomLng = random.nextInt(361) - 180; // generate random #
															// between
		// -180, 180 inclusive
		final int randomLat = random.nextInt(181) - 90; // generate random #
														// between
		// -90, 90 inclusive
		final int randomPersons = random.nextInt(2000000);

		builder.set(
				"location",
				GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
						randomLng,
						randomLat)));
		builder.set(
				"persons",
				randomPersons);
		builder.set(
				"record_date",
				new Date());
		builder.set(
				"income_category",
				"10-15");
		builder.set(
				"affiliation",
				"a few words");

		return builder.buildFeature(UUID.randomUUID().toString());
	}

}
