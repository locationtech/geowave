package mil.nga.giat.geowave.test.query;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

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
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryAdapter;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexType;
import mil.nga.giat.geowave.core.store.index.writer.IndexWriter;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

@RunWith(GeoWaveITRunner.class)
public class SecondaryIndexIT
{

	@GeoWaveTestStore({
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStoreOptions;

	@Before
	public void initTest()
			throws SchemaException,
			MismatchedIndexToAdapterMapping,
			IOException,
			ParseException {

		// mark attributes for secondary indexing
		final List<SimpleFeatureUserDataConfiguration> configs = new ArrayList<>();

		// JOIN
		configs.add(new NumericSecondaryIndexConfiguration(
				"doubleField",
				SecondaryIndexType.JOIN));
		configs.add(new TemporalSecondaryIndexConfiguration(
				"dateField",
				SecondaryIndexType.JOIN));
		configs.add(new TextSecondaryIndexConfiguration(
				"stringField",
				SecondaryIndexType.JOIN));

		// FULL
		// configs.add(new NumericSecondaryIndexConfiguration(
		// "doubleField",
		// SecondaryIndexType.FULL));
		// configs.add(new TemporalSecondaryIndexConfiguration(
		// "dateField",
		// SecondaryIndexType.FULL));
		// configs.add(new TextSecondaryIndexConfiguration(
		// "stringField",
		// SecondaryIndexType.FULL));

		// PARTIAL
		// configs.add(new NumericSecondaryIndexConfiguration(
		// "doubleField",
		// SecondaryIndexType.PARTIAL,
		// Arrays.asList(
		// StringUtils.stringFromBinary(
		// GeometryAdapter.DEFAULT_GEOMETRY_FIELD_ID.getBytes()))));
		// configs.add(new TemporalSecondaryIndexConfiguration(
		// "dateField",
		// SecondaryIndexType.PARTIAL,
		// Arrays.asList(
		// StringUtils.stringFromBinary(
		// GeometryAdapter.DEFAULT_GEOMETRY_FIELD_ID.getBytes()))));
		// configs.add(new TextSecondaryIndexConfiguration(
		// "stringField",
		// SecondaryIndexType.PARTIAL,
		// Arrays.asList(
		// StringUtils.stringFromBinary(
		// GeometryAdapter.DEFAULT_GEOMETRY_FIELD_ID.getBytes()))));

		// update schema with configs
		final SimpleFeatureType schema = DataUtilities.createType(
				"record",
				"geometryField:Geometry,doubleField:Double,dateField:Date,stringField:String");
		final SimpleFeatureUserDataConfigurationSet config = new SimpleFeatureUserDataConfigurationSet(
				schema,
				configs);
		config.updateType(schema);

		// build features
		final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
				schema);
		final List<SimpleFeature> features = new ArrayList<>();
		final DateFormat dateFormat = new SimpleDateFormat(
				"dd-MM-yyyy");
		features.add(buildSimpleFeature(
				builder,
				-180d,
				-90d,
				dateFormat.parse("11-11-2012"),
				1d,
				"aaa"));
		features.add(buildSimpleFeature(
				builder,
				0d,
				0d,
				dateFormat.parse("11-30-2013"),
				10d,
				"bbb"));
		features.add(buildSimpleFeature(
				builder,
				180d,
				90d,
				dateFormat.parse("12-25-2015"),
				100d,
				"ccc"));

		// ingest data
		final DataStore dataStore = dataStoreOptions.createDataStore();
		final FeatureDataAdapter dataAdapter = new FeatureDataAdapter(
				schema);
		final PrimaryIndex index = TestUtils.DEFAULT_SPATIAL_INDEX;
		try (@SuppressWarnings("unchecked")
		final IndexWriter<SimpleFeature> writer = dataStore.createWriter(
				dataAdapter,
				index)) {
			for (final SimpleFeature aFeature : features) {
				writer.write(aFeature);
			}
		}
	}

	@Test
	public void test() {
		final SecondaryIndexDataStore secondaryIndexDataStore = dataStoreOptions.createSecondaryIndexStore();
		// TODO ...
		Assert.assertTrue(true);
	}

	@After
	public void deleteTestData()
			throws IOException {
		TestUtils.deleteAll(dataStoreOptions);
	}

	private SimpleFeature buildSimpleFeature(
			final SimpleFeatureBuilder builder,
			final double lng,
			final double lat,
			final Date dateField,
			final double doubleField,
			final String stringField ) {
		builder.set(
				"geometryField",
				GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
						lng,
						lat)));
		builder.set(
				"dateField",
				dateField);
		builder.set(
				"doubleField",
				doubleField);
		builder.set(
				"stringField",
				stringField);
		return builder.buildFeature(UUID.randomUUID().toString());
	}

}
