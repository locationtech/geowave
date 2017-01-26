package mil.nga.giat.geowave.test.query;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.io.Text;
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
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.index.NumericSecondaryIndexConfiguration;
import mil.nga.giat.geowave.adapter.vector.index.TemporalSecondaryIndexConfiguration;
import mil.nga.giat.geowave.adapter.vector.index.TextSecondaryIndexConfiguration;
import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfiguration;
import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfigurationSet;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryAdapter;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.lexicoder.Lexicoders;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.core.store.index.FilterableConstraints;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexType;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexUtils;
import mil.nga.giat.geowave.core.store.index.numeric.NumericGreaterThanConstraint;
import mil.nga.giat.geowave.core.store.index.temporal.TemporalQueryConstraint;
import mil.nga.giat.geowave.core.store.index.text.TextQueryConstraint;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloRequiredOptions;
import mil.nga.giat.geowave.datastore.accumulo.util.ConnectorPool;
import mil.nga.giat.geowave.datastore.hbase.index.secondary.HBaseSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseRequiredOptions;
import mil.nga.giat.geowave.datastore.hbase.util.ConnectionPool;
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
	private FeatureDataAdapter dataAdapter;
	private PrimaryIndex index;
	private DataStore dataStore;
	private SecondaryIndexDataStore secondaryDataStore;
	private List<ByteArrayId> allPrimaryIndexIds = new ArrayList<>();
	private List<String> allDataIds = new ArrayList<>();
	private int numAttributes;
	private List<SecondaryIndex<SimpleFeature>> allSecondaryIndices;
	private DistributableQuery query;
	private Point expectedPoint;
	private String expectedDataId;

	@Test
	public void testSecondaryIndicesManually()
			throws AccumuloException,
			AccumuloSecurityException,
			TableNotFoundException,
			ParseException,
			IOException {

		Assert.assertTrue(allPrimaryIndexIds.size() == 3);

		if (dataStoreOptions.getType().equals(
				"accumulo")) {

			final AccumuloRequiredOptions options = (AccumuloRequiredOptions) dataStoreOptions.getFactoryOptions();

			final Connector connector = ConnectorPool.getInstance().getConnector(
					options.getZookeeper(),
					options.getInstance(),
					options.getUser(),
					options.getPassword());

			numericJoinAccumulo(connector);
			textJoinAccumulo(connector);
			temporalJoinAccumulo(connector);
			numericFullAccumulo(connector);
			textFullAccumulo(connector);
			temporalFullAccumulo(connector);
			numericPartialAccumulo(connector);
			textPartialAccumulo(connector);
			temporalPartialAccumulo(connector);
		}

		else if (dataStoreOptions.getType().equals(
				"hbase")) {

			final HBaseRequiredOptions options = (HBaseRequiredOptions) dataStoreOptions.getFactoryOptions();

			final Connection connection = ConnectionPool.getInstance().getConnection(
					options.getZookeeper());

			numericJoinHBase(connection);
			textJoinHBase(connection);
			temporalJoinHBase(connection);
			numericFullHBase(connection);
			textFullHBase(connection);
			temporalFullHBase(connection);
			numericPartialHBase(connection);
			textPartialHBase(connection);
			temporalPartialHBase(connection);
		}

	}

	@Test
	public void testSecondaryIndicesViaDirectQuery()
			throws IOException {

		Assert.assertTrue(secondaryDataStore != null);

		if (dataStoreOptions.getType().equals(
				"accumulo")) {
			Assert.assertTrue(secondaryDataStore instanceof AccumuloSecondaryIndexDataStore);
		}
		else if (dataStoreOptions.getType().equals(
				"hbase")) {
			Assert.assertTrue(secondaryDataStore instanceof HBaseSecondaryIndexDataStore);
		}

		Assert.assertTrue(allSecondaryIndices.size() == 9);

		for (final SecondaryIndex<SimpleFeature> secondaryIndex : allSecondaryIndices) {

			final List<SimpleFeature> queryResults = new ArrayList<>();
			try (final CloseableIterator<SimpleFeature> results = secondaryDataStore.query(
					secondaryIndex,
					secondaryIndex.getFieldId(),
					dataAdapter,
					index,
					query,
					DEFAULT_AUTHORIZATIONS)) {

				while (results.hasNext()) {
					queryResults.add(results.next());
				}
			}

			final int numResults = queryResults.size();
			Assert.assertTrue(
					secondaryIndex.getId().getString() + " returned " + numResults + " results (should have been 1)",
					numResults == 1);

			final SimpleFeature result = queryResults.get(0);

			// verify dataId match
			Assert.assertTrue(result.getID().equals(
					expectedDataId));

			// verify geometry match
			Assert.assertTrue(result.getAttribute(
					GEOMETRY_FIELD).equals(
					expectedPoint));
		}

		// test delete
		final Query deleteQuery = new DataIdQuery(
				dataAdapter.getAdapterId(),
				new ByteArrayId(
						expectedDataId));
		final QueryOptions queryOptions = new QueryOptions(
				dataAdapter,
				index);
		dataStore.delete(
				queryOptions,
				deleteQuery);

		for (final SecondaryIndex<SimpleFeature> secondaryIndex : allSecondaryIndices) {

			int numResults = 0;
			try (final CloseableIterator<SimpleFeature> results = secondaryDataStore.query(
					secondaryIndex,
					secondaryIndex.getFieldId(),
					dataAdapter,
					index,
					query,
					DEFAULT_AUTHORIZATIONS)) {

				while (results.hasNext()) {
					numResults++;
				}
			}
			Assert.assertTrue(
					secondaryIndex.getId().getString() + " returned " + numResults + " results (should have been 0)",
					numResults == 0);
		}
	}

	private static final String NUMERIC_JOIN_TABLE = TestUtils.TEST_NAMESPACE + "_GEOWAVE_2ND_IDX_NUMERIC_JOIN";
	private static final String TEXT_JOIN_TABLE = TestUtils.TEST_NAMESPACE + "_GEOWAVE_2ND_IDX_TEXT_JOIN";
	private static final String TEMPORAL_JOIN_TABLE = TestUtils.TEST_NAMESPACE + "_GEOWAVE_2ND_IDX_TEMPORAL_JOIN";
	private static final String NUMERIC_FULL_TABLE = TestUtils.TEST_NAMESPACE + "_GEOWAVE_2ND_IDX_NUMERIC_FULL";
	private static final String TEXT_FULL_TABLE = TestUtils.TEST_NAMESPACE + "_GEOWAVE_2ND_IDX_TEXT_FULL";
	private static final String TEMPORAL_FULL_TABLE = TestUtils.TEST_NAMESPACE + "_GEOWAVE_2ND_IDX_TEMPORAL_FULL";
	private static final String NUMERIC_PARTIAL_TABLE = TestUtils.TEST_NAMESPACE + "_GEOWAVE_2ND_IDX_NUMERIC_PARTIAL";
	private static final String TEXT_PARTIAL_TABLE = TestUtils.TEST_NAMESPACE + "_GEOWAVE_2ND_IDX_TEXT_PARTIAL";
	private static final String TEMPORAL_PARTIAL_TABLE = TestUtils.TEST_NAMESPACE + "_GEOWAVE_2ND_IDX_TEMPORAL_PARTIAL";
	private static final String GEOMETRY_FIELD = "geometryField";
	private static final String NUMERIC_JOIN_FIELD = "doubleField";
	private static final ByteArrayId NUMERIC_JOIN_FIELD_ID = new ByteArrayId(
			NUMERIC_JOIN_FIELD);
	private static final String TEMPORAL_JOIN_FIELD = "dateField";
	private static final ByteArrayId TEMPORAL_JOIN_FIELD_ID = new ByteArrayId(
			TEMPORAL_JOIN_FIELD);
	private static final String TEXT_JOIN_FIELD = "stringField";
	private static final ByteArrayId TEXT_JOIN_FIELD_ID = new ByteArrayId(
			TEXT_JOIN_FIELD);
	private static final String NUMERIC_FULL_FIELD = "doubleField2";
	private static final ByteArrayId NUMERIC_FULL_FIELD_ID = new ByteArrayId(
			NUMERIC_FULL_FIELD);
	private static final String TEMPORAL_FULL_FIELD = "dateField2";
	private static final ByteArrayId TEMPORAL_FULL_FIELD_ID = new ByteArrayId(
			TEMPORAL_FULL_FIELD);
	private static final String TEXT_FULL_FIELD = "stringField2";
	private static final ByteArrayId TEXT_FULL_FIELD_ID = new ByteArrayId(
			TEXT_FULL_FIELD);
	private static final String NUMERIC_PARTIAL_FIELD = "doubleField3";
	private static final ByteArrayId NUMERIC_PARTIAL_FIELD_ID = new ByteArrayId(
			NUMERIC_PARTIAL_FIELD);
	private static final String TEMPORAL_PARTIAL_FIELD = "dateField3";
	private static final ByteArrayId TEMPORAL_PARTIAL_FIELD_ID = new ByteArrayId(
			TEMPORAL_PARTIAL_FIELD);
	private static final String TEXT_PARTIAL_FIELD = "stringField3";
	private static final ByteArrayId TEXT_PARTIAL_FIELD_ID = new ByteArrayId(
			TEXT_PARTIAL_FIELD);
	private static final DateFormat DATE_FORMAT = new SimpleDateFormat(
			"dd-MM-yyyy");
	private static final ByteArrayId GEOMETRY_FIELD_ID = new ByteArrayId(
			GeometryAdapter.DEFAULT_GEOMETRY_FIELD_ID.getBytes());
	private static final String[] DEFAULT_AUTHORIZATIONS = new String[] {};
	private static final Authorizations DEFAULT_ACCUMULO_AUTHORIZATIONS = new Authorizations(
			DEFAULT_AUTHORIZATIONS);
	private static final List<String> PARTIAL_LIST = Arrays.asList(StringUtils.stringFromBinary(GEOMETRY_FIELD_ID
			.getBytes()));

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
				NUMERIC_JOIN_FIELD,
				SecondaryIndexType.JOIN));
		configs.add(new TemporalSecondaryIndexConfiguration(
				TEMPORAL_JOIN_FIELD,
				SecondaryIndexType.JOIN));
		configs.add(new TextSecondaryIndexConfiguration(
				TEXT_JOIN_FIELD,
				SecondaryIndexType.JOIN));

		// FULL
		configs.add(new NumericSecondaryIndexConfiguration(
				NUMERIC_FULL_FIELD,
				SecondaryIndexType.FULL));
		configs.add(new TemporalSecondaryIndexConfiguration(
				TEMPORAL_FULL_FIELD,
				SecondaryIndexType.FULL));
		configs.add(new TextSecondaryIndexConfiguration(
				TEXT_FULL_FIELD,
				SecondaryIndexType.FULL));

		// PARTIAL
		configs.add(new NumericSecondaryIndexConfiguration(
				NUMERIC_PARTIAL_FIELD,
				SecondaryIndexType.PARTIAL,
				PARTIAL_LIST));
		configs.add(new TemporalSecondaryIndexConfiguration(
				TEMPORAL_PARTIAL_FIELD,
				SecondaryIndexType.PARTIAL,
				PARTIAL_LIST));
		configs.add(new TextSecondaryIndexConfiguration(
				TEXT_PARTIAL_FIELD,
				SecondaryIndexType.PARTIAL,
				PARTIAL_LIST));

		// update schema with configs
		final SimpleFeatureType schema = DataUtilities.createType(
				"record",
				GEOMETRY_FIELD + ":Geometry," + NUMERIC_JOIN_FIELD + ":Double," + NUMERIC_FULL_FIELD + ":Double,"
						+ NUMERIC_PARTIAL_FIELD + ":Double," + TEMPORAL_JOIN_FIELD + ":Date," + TEMPORAL_FULL_FIELD
						+ ":Date," + TEMPORAL_PARTIAL_FIELD + ":Date," + TEXT_JOIN_FIELD + ":String," + TEXT_FULL_FIELD
						+ ":String," + TEXT_PARTIAL_FIELD + ":String");
		numAttributes = schema.getAttributeCount();
		final SimpleFeatureUserDataConfigurationSet config = new SimpleFeatureUserDataConfigurationSet(
				schema,
				configs);
		config.updateType(schema);

		// build features
		final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
				schema);
		final List<SimpleFeature> features = new ArrayList<>();

		features.add(buildSimpleFeature(
				builder,
				-180d,
				-90d,
				DATE_FORMAT.parse("11-11-2012"),
				1d,
				"aaa"));
		features.add(buildSimpleFeature(
				builder,
				0d,
				0d,
				DATE_FORMAT.parse("11-30-2013"),
				10d,
				"bbb"));

		// Create a feature and collect verification data from it
		final SimpleFeature feat = buildSimpleFeature(
				builder,
				180d,
				90d,
				DATE_FORMAT.parse("12-25-2015"),
				100d,
				"ccc");
		expectedPoint = (Point) feat.getAttribute(GEOMETRY_FIELD);
		expectedDataId = feat.getID();
		features.add(feat);

		// ingest data
		dataStore = dataStoreOptions.createDataStore();
		secondaryDataStore = dataStoreOptions.createSecondaryIndexStore();
		secondaryDataStore.setDataStore(dataStore);
		dataAdapter = new FeatureDataAdapter(
				schema);
		index = TestUtils.DEFAULT_SPATIAL_INDEX;
		try (@SuppressWarnings("unchecked")
		final IndexWriter<SimpleFeature> writer = dataStore.createWriter(
				dataAdapter,
				index)) {
			for (final SimpleFeature aFeature : features) {
				allPrimaryIndexIds.addAll(writer.write(aFeature));
			}
		}

		allSecondaryIndices = dataAdapter.getSupportedSecondaryIndices();

		final Map<ByteArrayId, FilterableConstraints> additionalConstraints = new HashMap<>();

		final Number number = 25d;
		additionalConstraints.put(
				NUMERIC_JOIN_FIELD_ID,
				new NumericGreaterThanConstraint(
						NUMERIC_JOIN_FIELD_ID,
						number));
		additionalConstraints.put(
				NUMERIC_FULL_FIELD_ID,
				new NumericGreaterThanConstraint(
						NUMERIC_FULL_FIELD_ID,
						number));
		additionalConstraints.put(
				NUMERIC_PARTIAL_FIELD_ID,
				new NumericGreaterThanConstraint(
						NUMERIC_PARTIAL_FIELD_ID,
						number));

		final String matchValue = "ccc";
		additionalConstraints.put(
				TEXT_JOIN_FIELD_ID,
				new TextQueryConstraint(
						TEXT_JOIN_FIELD_ID,
						matchValue,
						true));
		additionalConstraints.put(
				TEXT_FULL_FIELD_ID,
				new TextQueryConstraint(
						TEXT_FULL_FIELD_ID,
						matchValue,
						true));
		additionalConstraints.put(
				TEXT_PARTIAL_FIELD_ID,
				new TextQueryConstraint(
						TEXT_PARTIAL_FIELD_ID,
						matchValue,
						true));

		final Date start = DATE_FORMAT.parse("12-24-2015");
		final Date end = DATE_FORMAT.parse("12-26-2015");
		additionalConstraints.put(
				TEMPORAL_JOIN_FIELD_ID,
				new TemporalQueryConstraint(
						TEMPORAL_JOIN_FIELD_ID,
						start,
						end));
		additionalConstraints.put(
				TEMPORAL_FULL_FIELD_ID,
				new TemporalQueryConstraint(
						TEMPORAL_FULL_FIELD_ID,
						start,
						end));
		additionalConstraints.put(
				TEMPORAL_PARTIAL_FIELD_ID,
				new TemporalQueryConstraint(
						TEMPORAL_PARTIAL_FIELD_ID,
						start,
						end));

		query = new SpatialQuery(
				GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(
						-180d,
						180d,
						-90d,
						90d)),
				additionalConstraints);
	}

	/**
	 * 
	 * @throws IOException
	 */

	@After
	public void deleteTestData()
			throws IOException {
		TestUtils.deleteAll(dataStoreOptions);
	}

	/**
	 * 
	 * @param connector
	 * @throws TableNotFoundException
	 */

	private void numericJoinAccumulo(
			final Connector connector )
			throws TableNotFoundException {
		final Scanner scanner = connector.createScanner(
				NUMERIC_JOIN_TABLE,
				DEFAULT_ACCUMULO_AUTHORIZATIONS);

		scanner.setRange(new Range(
				new Text(
						Lexicoders.DOUBLE.toByteArray(0d)),
				new Text(
						Lexicoders.DOUBLE.toByteArray(20d))));

		scanner.fetchColumnFamily(new Text(
				SecondaryIndexUtils.constructColumnFamily(
						dataAdapter.getAdapterId(),
						NUMERIC_JOIN_FIELD_ID)));
		int numResults = 0;

		for (final Entry<Key, Value> entry : scanner) {
			numResults += 1;
			final ByteArrayId primaryRowId = SecondaryIndexUtils.getPrimaryRowId(entry
					.getKey()
					.getColumnQualifierData()
					.getBackingArray());
			if (numResults == 1) {
				Assert.assertTrue(primaryRowId.equals(allPrimaryIndexIds.get(0)));
			}
			else if (numResults == 2) {
				Assert.assertTrue(primaryRowId.equals(allPrimaryIndexIds.get(1)));
			}
		}
		scanner.close();
		Assert.assertTrue(numResults == 2);
	}

	private void numericJoinHBase(
			final Connection connection )
			throws IOException {
		final Table table = connection.getTable(TableName.valueOf((NUMERIC_JOIN_TABLE)));
		final Scan scan = new Scan();
		final byte[] columnFamily = SecondaryIndexUtils.constructColumnFamily(
				dataAdapter.getAdapterId(),
				NUMERIC_JOIN_FIELD_ID);
		scan.addFamily(columnFamily);
		scan.setStartRow(Lexicoders.DOUBLE.toByteArray(0d));
		scan.setStopRow(Lexicoders.DOUBLE.toByteArray(20d));
		final ResultScanner results = table.getScanner(scan);
		int numResults = 0;
		for (final Result result : results) {
			numResults += 1;
			final NavigableMap<byte[], byte[]> qualifierToValueMap = result.getFamilyMap(columnFamily);
			final Set<Entry<byte[], byte[]>> entries = qualifierToValueMap.entrySet();
			Assert.assertTrue(entries.size() == 1);
			for (final Entry<byte[], byte[]> entry : entries) {
				final ByteArrayId primaryRowId = SecondaryIndexUtils.getPrimaryRowId(entry.getKey());
				if (numResults == 1) {
					Assert.assertTrue(primaryRowId.equals(allPrimaryIndexIds.get(0)));
				}
				else if (numResults == 2) {
					Assert.assertTrue(primaryRowId.equals(allPrimaryIndexIds.get(1)));
				}
			}
		}
		table.close();
		Assert.assertTrue(numResults == 2);
	}

	private void textJoinAccumulo(
			final Connector connector )
			throws TableNotFoundException {
		final Scanner scanner = connector.createScanner(
				TEXT_JOIN_TABLE,
				DEFAULT_ACCUMULO_AUTHORIZATIONS);
		scanner.setRange(new Range(
				new Text(
						new ByteArrayId(
								"bbb").getBytes())));
		scanner.fetchColumnFamily(new Text(
				SecondaryIndexUtils.constructColumnFamily(
						dataAdapter.getAdapterId(),
						TEXT_JOIN_FIELD_ID)));
		int numResults = 0;
		for (final Entry<Key, Value> entry : scanner) {
			numResults += 1;
			final ByteArrayId primaryRowId = SecondaryIndexUtils.getPrimaryRowId(entry
					.getKey()
					.getColumnQualifierData()
					.getBackingArray());
			Assert.assertTrue(primaryRowId.equals(allPrimaryIndexIds.get(1)));
		}
		scanner.close();
		Assert.assertTrue(numResults == 1);
	}

	private void textJoinHBase(
			final Connection connection )
			throws IOException {
		final Table table = connection.getTable(TableName.valueOf((TEXT_JOIN_TABLE)));
		final Scan scan = new Scan();
		final byte[] columnFamily = SecondaryIndexUtils.constructColumnFamily(
				dataAdapter.getAdapterId(),
				TEXT_JOIN_FIELD_ID);
		scan.addFamily(columnFamily);
		scan.setStartRow(new ByteArrayId(
				"bbb").getBytes());
		scan.setStopRow(new ByteArrayId(
				"bbb").getBytes());
		final ResultScanner results = table.getScanner(scan);
		int numResults = 0;
		for (final Result result : results) {
			numResults += 1;
			final NavigableMap<byte[], byte[]> qualifierToValueMap = result.getFamilyMap(columnFamily);
			final Set<Entry<byte[], byte[]>> entries = qualifierToValueMap.entrySet();
			Assert.assertTrue(entries.size() == 1);
			for (final Entry<byte[], byte[]> entry : entries) {
				final ByteArrayId primaryRowId = SecondaryIndexUtils.getPrimaryRowId(entry.getKey());
				Assert.assertTrue(primaryRowId.equals(allPrimaryIndexIds.get(1)));
			}
		}
		table.close();
		Assert.assertTrue(numResults == 1);
	}

	private void temporalJoinAccumulo(
			final Connector connector )
			throws TableNotFoundException,
			ParseException {
		final Scanner scanner = connector.createScanner(
				TEMPORAL_JOIN_TABLE,
				DEFAULT_ACCUMULO_AUTHORIZATIONS);
		Text startText = new Text(
				Lexicoders.LONG.toByteArray(DATE_FORMAT.parse(
						"11-30-2012").getTime()));
		Text stopText = new Text(
				Lexicoders.LONG.toByteArray(DATE_FORMAT.parse(
						"11-30-2014").getTime()));

		scanner.setRange(new Range(
				startText,
				stopText));
		byte[] colFam = SecondaryIndexUtils.constructColumnFamily(
				dataAdapter.getAdapterId(),
				TEMPORAL_JOIN_FIELD_ID);

		scanner.fetchColumnFamily(new Text(
				colFam));
		int numResults = 0;
		for (final Entry<Key, Value> entry : scanner) {
			numResults += 1;
			final ByteArrayId primaryRowId = SecondaryIndexUtils.getPrimaryRowId(entry
					.getKey()
					.getColumnQualifierData()
					.getBackingArray());
			Assert.assertTrue(primaryRowId.equals(allPrimaryIndexIds.get(1)));
		}
		scanner.close();
		Assert.assertTrue(numResults == 1);
	}

	private void temporalJoinHBase(
			final Connection connection )
			throws IOException,
			ParseException {
		final Table table = connection.getTable(TableName.valueOf((TEMPORAL_JOIN_TABLE)));
		final Scan scan = new Scan();
		final byte[] columnFamily = SecondaryIndexUtils.constructColumnFamily(
				dataAdapter.getAdapterId(),
				TEMPORAL_JOIN_FIELD_ID);
		scan.addFamily(columnFamily);
		scan.setStartRow(Lexicoders.LONG.toByteArray(DATE_FORMAT.parse(
				"11-30-2012").getTime()));
		scan.setStopRow(Lexicoders.LONG.toByteArray(DATE_FORMAT.parse(
				"11-30-2014").getTime()));
		final ResultScanner results = table.getScanner(scan);
		int numResults = 0;
		for (final Result result : results) {
			numResults += 1;
			final NavigableMap<byte[], byte[]> qualifierToValueMap = result.getFamilyMap(columnFamily);
			final Set<Entry<byte[], byte[]>> entries = qualifierToValueMap.entrySet();
			Assert.assertTrue(entries.size() == 1);
			for (final Entry<byte[], byte[]> entry : entries) {
				final ByteArrayId primaryRowId = SecondaryIndexUtils.getPrimaryRowId(entry.getKey());
				Assert.assertTrue(primaryRowId.equals(allPrimaryIndexIds.get(1)));
			}
		}
		table.close();
		Assert.assertTrue(numResults == 1);
	}

	private void numericFullAccumulo(
			final Connector connector )
			throws TableNotFoundException {
		final Scanner scanner = connector.createScanner(
				NUMERIC_FULL_TABLE,
				DEFAULT_ACCUMULO_AUTHORIZATIONS);
		scanner.setRange(new Range(
				new Text(
						Lexicoders.DOUBLE.toByteArray(0d)),
				new Text(
						Lexicoders.DOUBLE.toByteArray(20d))));
		scanner.fetchColumnFamily(new Text(
				SecondaryIndexUtils.constructColumnFamily(
						dataAdapter.getAdapterId(),
						NUMERIC_FULL_FIELD_ID)));
		int numResults = 0;
		for (final Entry<Key, Value> entry : scanner) {
			final String dataId = SecondaryIndexUtils.getDataId(entry
					.getKey()
					.getColumnQualifierData()
					.getBackingArray());
			if ((numResults / numAttributes) == 0) {
				Assert.assertTrue(dataId.equals(allDataIds.get(0)));
			}
			else if ((numResults / numAttributes) == 1) {
				Assert.assertTrue(dataId.equals(allDataIds.get(1)));
			}
			numResults += 1;
		}
		scanner.close();
		Assert.assertTrue(numResults == (2 * numAttributes));
	}

	private void numericFullHBase(
			final Connection connection )
			throws IOException {
		final Table table = connection.getTable(TableName.valueOf((NUMERIC_FULL_TABLE)));
		final Scan scan = new Scan();
		final byte[] columnFamily = SecondaryIndexUtils.constructColumnFamily(
				dataAdapter.getAdapterId(),
				NUMERIC_FULL_FIELD_ID);
		scan.addFamily(columnFamily);
		scan.setStartRow(Lexicoders.DOUBLE.toByteArray(0d));
		scan.setStopRow(Lexicoders.DOUBLE.toByteArray(20d));
		final ResultScanner results = table.getScanner(scan);
		int numResults = 0;
		for (final Result result : results) {
			numResults += 1;
			final NavigableMap<byte[], byte[]> qualifierToValueMap = result.getFamilyMap(columnFamily);
			final Set<Entry<byte[], byte[]>> entries = qualifierToValueMap.entrySet();
			Assert.assertTrue(entries.size() == numAttributes);
			for (final Entry<byte[], byte[]> entry : entries) {
				final String dataId = SecondaryIndexUtils.getDataId(entry.getKey());
				Assert.assertTrue((dataId.equals(allDataIds.get(0))) || (dataId.equals(allDataIds.get(1))));
			}
		}
		table.close();
		Assert.assertTrue(numResults == 2);
	}

	private void textFullAccumulo(
			final Connector connector )
			throws TableNotFoundException {
		final Scanner scanner = connector.createScanner(
				TEXT_FULL_TABLE,
				DEFAULT_ACCUMULO_AUTHORIZATIONS);
		scanner.setRange(new Range(
				new Text(
						new ByteArrayId(
								"bbb").getBytes())));
		scanner.fetchColumnFamily(new Text(
				SecondaryIndexUtils.constructColumnFamily(
						dataAdapter.getAdapterId(),
						TEXT_FULL_FIELD_ID)));
		int numResults = 0;
		for (final Entry<Key, Value> entry : scanner) {
			numResults += 1;
			final String dataId = SecondaryIndexUtils.getDataId(entry
					.getKey()
					.getColumnQualifierData()
					.getBackingArray());
			Assert.assertTrue(dataId.equals(allDataIds.get(1)));
		}
		scanner.close();
		Assert.assertTrue(numResults == numAttributes);
	}

	private void textFullHBase(
			final Connection connection )
			throws IOException {
		final Table table = connection.getTable(TableName.valueOf((TEXT_FULL_TABLE)));
		final Scan scan = new Scan();
		final byte[] columnFamily = SecondaryIndexUtils.constructColumnFamily(
				dataAdapter.getAdapterId(),
				TEXT_FULL_FIELD_ID);
		scan.addFamily(columnFamily);
		scan.setStartRow(new ByteArrayId(
				"bbb").getBytes());
		scan.setStopRow(new ByteArrayId(
				"bbb").getBytes());
		final ResultScanner results = table.getScanner(scan);
		int numResults = 0;
		for (final Result result : results) {
			numResults += 1;
			final NavigableMap<byte[], byte[]> qualifierToValueMap = result.getFamilyMap(columnFamily);
			final Set<Entry<byte[], byte[]>> entries = qualifierToValueMap.entrySet();
			Assert.assertTrue(entries.size() == numAttributes);
			for (final Entry<byte[], byte[]> entry : entries) {
				final String dataId = SecondaryIndexUtils.getDataId(entry.getKey());
				Assert.assertTrue(dataId.equals(allDataIds.get(1)));
			}
		}
		table.close();
		Assert.assertTrue(numResults == 1);
	}

	private void temporalFullAccumulo(
			final Connector connector )
			throws TableNotFoundException,
			ParseException {
		final Scanner scanner = connector.createScanner(
				TEMPORAL_FULL_TABLE,
				DEFAULT_ACCUMULO_AUTHORIZATIONS);
		scanner.setRange(new Range(
				new Text(
						Lexicoders.LONG.toByteArray(DATE_FORMAT.parse(
								"11-30-2012").getTime())),
				new Text(
						Lexicoders.LONG.toByteArray(DATE_FORMAT.parse(
								"11-30-2014").getTime()))));
		scanner.fetchColumnFamily(new Text(
				SecondaryIndexUtils.constructColumnFamily(
						dataAdapter.getAdapterId(),
						TEMPORAL_FULL_FIELD_ID)));
		int numResults = 0;
		for (final Entry<Key, Value> entry : scanner) {
			numResults += 1;
			final String dataId = SecondaryIndexUtils.getDataId(entry
					.getKey()
					.getColumnQualifierData()
					.getBackingArray());
			Assert.assertTrue(dataId.equals(allDataIds.get(1)));
		}
		scanner.close();
		Assert.assertTrue(numResults == numAttributes);
	}

	private void temporalFullHBase(
			final Connection connection )
			throws IOException,
			ParseException {
		final Table table = connection.getTable(TableName.valueOf((TEMPORAL_FULL_TABLE)));
		final Scan scan = new Scan();
		final byte[] columnFamily = SecondaryIndexUtils.constructColumnFamily(
				dataAdapter.getAdapterId(),
				TEMPORAL_FULL_FIELD_ID);
		scan.addFamily(columnFamily);
		scan.setStartRow(Lexicoders.LONG.toByteArray(DATE_FORMAT.parse(
				"11-30-2012").getTime()));
		scan.setStopRow(Lexicoders.LONG.toByteArray(DATE_FORMAT.parse(
				"11-30-2014").getTime()));
		final ResultScanner results = table.getScanner(scan);
		int numResults = 0;
		for (final Result result : results) {
			numResults += 1;
			final NavigableMap<byte[], byte[]> qualifierToValueMap = result.getFamilyMap(columnFamily);
			final Set<Entry<byte[], byte[]>> entries = qualifierToValueMap.entrySet();
			Assert.assertTrue(entries.size() == numAttributes);
			for (final Entry<byte[], byte[]> entry : entries) {
				final String dataId = SecondaryIndexUtils.getDataId(entry.getKey());
				Assert.assertTrue(dataId.equals(allDataIds.get(1)));
			}
		}
		table.close();
		Assert.assertTrue(numResults == 1);
	}

	private void numericPartialAccumulo(
			final Connector connector )
			throws TableNotFoundException {
		final Scanner scanner = connector.createScanner(
				NUMERIC_PARTIAL_TABLE,
				DEFAULT_ACCUMULO_AUTHORIZATIONS);
		scanner.setRange(new Range(
				new Text(
						Lexicoders.DOUBLE.toByteArray(0d)),
				new Text(
						Lexicoders.DOUBLE.toByteArray(20d))));
		scanner.fetchColumnFamily(new Text(
				SecondaryIndexUtils.constructColumnFamily(
						dataAdapter.getAdapterId(),
						NUMERIC_PARTIAL_FIELD_ID)));
		int numResults = 0;
		for (final Entry<Key, Value> entry : scanner) {
			numResults += 1;
			final byte[] cq = entry.getKey().getColumnQualifierData().getBackingArray();
			final ByteArrayId fieldId = SecondaryIndexUtils.getFieldId(cq);
			Assert.assertTrue(fieldId.equals(GEOMETRY_FIELD_ID));
			final String dataId = SecondaryIndexUtils.getDataId(entry
					.getKey()
					.getColumnQualifierData()
					.getBackingArray());
			if (numResults == 1) {
				Assert.assertTrue(dataId.equals(allDataIds.get(0)));
			}
			else if (numResults == 2) {
				Assert.assertTrue(dataId.equals(allDataIds.get(1)));
			}
		}
		scanner.close();
		Assert.assertTrue(numResults == 2);
	}

	private void numericPartialHBase(
			final Connection connection )
			throws IOException {
		final Table table = connection.getTable(TableName.valueOf((NUMERIC_PARTIAL_TABLE)));
		final Scan scan = new Scan();
		final byte[] columnFamily = SecondaryIndexUtils.constructColumnFamily(
				dataAdapter.getAdapterId(),
				NUMERIC_PARTIAL_FIELD_ID);
		scan.addFamily(columnFamily);
		scan.setStartRow(Lexicoders.DOUBLE.toByteArray(0d));
		scan.setStopRow(Lexicoders.DOUBLE.toByteArray(20d));
		final ResultScanner results = table.getScanner(scan);
		int numResults = 0;
		for (final Result result : results) {
			numResults += 1;
			final NavigableMap<byte[], byte[]> qualifierToValueMap = result.getFamilyMap(columnFamily);
			final Set<Entry<byte[], byte[]>> entries = qualifierToValueMap.entrySet();
			Assert.assertTrue(entries.size() == 1);
			for (final Entry<byte[], byte[]> entry : entries) {
				final byte[] cq = entry.getKey();
				final ByteArrayId fieldId = SecondaryIndexUtils.getFieldId(cq);
				Assert.assertTrue(fieldId.equals(GEOMETRY_FIELD_ID));
				final String dataId = SecondaryIndexUtils.getDataId(cq);
				Assert.assertTrue((dataId.equals(allDataIds.get(0))) || (dataId.equals(allDataIds.get(1))));
			}
		}
		table.close();
		Assert.assertTrue(numResults == 2);
	}

	private void textPartialAccumulo(
			final Connector connector )
			throws TableNotFoundException {
		final Scanner scanner = connector.createScanner(
				TEXT_PARTIAL_TABLE,
				DEFAULT_ACCUMULO_AUTHORIZATIONS);
		scanner.setRange(new Range(
				new Text(
						new ByteArrayId(
								"bbb").getBytes())));
		scanner.fetchColumnFamily(new Text(
				SecondaryIndexUtils.constructColumnFamily(
						dataAdapter.getAdapterId(),
						TEXT_PARTIAL_FIELD_ID)));
		int numResults = 0;
		for (final Entry<Key, Value> entry : scanner) {
			numResults += 1;
			final byte[] cq = entry.getKey().getColumnQualifierData().getBackingArray();
			final ByteArrayId fieldId = SecondaryIndexUtils.getFieldId(cq);
			Assert.assertTrue(fieldId.equals(GEOMETRY_FIELD_ID));
			final String dataId = SecondaryIndexUtils.getDataId(cq);
			Assert.assertTrue(dataId.equals(allDataIds.get(1)));
		}
		scanner.close();
		Assert.assertTrue(numResults == 1);
	}

	private void textPartialHBase(
			final Connection connection )
			throws IOException {
		final Table table = connection.getTable(TableName.valueOf((TEXT_PARTIAL_TABLE)));
		final Scan scan = new Scan();
		final byte[] columnFamily = SecondaryIndexUtils.constructColumnFamily(
				dataAdapter.getAdapterId(),
				TEXT_PARTIAL_FIELD_ID);
		scan.addFamily(columnFamily);
		scan.setStartRow(new ByteArrayId(
				"bbb").getBytes());
		scan.setStopRow(new ByteArrayId(
				"bbb").getBytes());
		final ResultScanner results = table.getScanner(scan);
		int numResults = 0;
		for (final Result result : results) {
			numResults += 1;
			final NavigableMap<byte[], byte[]> qualifierToValueMap = result.getFamilyMap(columnFamily);
			final Set<Entry<byte[], byte[]>> entries = qualifierToValueMap.entrySet();
			Assert.assertTrue(entries.size() == 1);
			for (final Entry<byte[], byte[]> entry : entries) {
				final byte[] cq = entry.getKey();
				final ByteArrayId fieldId = SecondaryIndexUtils.getFieldId(cq);
				Assert.assertTrue(fieldId.equals(GEOMETRY_FIELD_ID));
				final String dataId = SecondaryIndexUtils.getDataId(cq);
				Assert.assertTrue(dataId.equals(allDataIds.get(1)));
			}
		}
		table.close();
		Assert.assertTrue(numResults == 1);
	}

	private void temporalPartialAccumulo(
			final Connector connector )
			throws TableNotFoundException,
			ParseException {
		final Scanner scanner = connector.createScanner(
				TEMPORAL_PARTIAL_TABLE,
				DEFAULT_ACCUMULO_AUTHORIZATIONS);
		scanner.setRange(new Range(
				new Text(
						Lexicoders.LONG.toByteArray(DATE_FORMAT.parse(
								"11-30-2012").getTime())),
				new Text(
						Lexicoders.LONG.toByteArray(DATE_FORMAT.parse(
								"11-30-2014").getTime()))));
		scanner.fetchColumnFamily(new Text(
				SecondaryIndexUtils.constructColumnFamily(
						dataAdapter.getAdapterId(),
						TEMPORAL_PARTIAL_FIELD_ID)));
		int numResults = 0;
		for (final Entry<Key, Value> entry : scanner) {
			numResults += 1;
			final byte[] cq = entry.getKey().getColumnQualifierData().getBackingArray();
			final ByteArrayId fieldId = SecondaryIndexUtils.getFieldId(cq);
			Assert.assertTrue(fieldId.equals(GEOMETRY_FIELD_ID));
			final String dataId = SecondaryIndexUtils.getDataId(cq);
			Assert.assertTrue(dataId.equals(allDataIds.get(1)));
		}
		scanner.close();
		Assert.assertTrue(numResults == 1);
	}

	private void temporalPartialHBase(
			final Connection connection )
			throws IOException,
			ParseException {
		final Table table = connection.getTable(TableName.valueOf((TEMPORAL_PARTIAL_TABLE)));
		final Scan scan = new Scan();
		final byte[] columnFamily = SecondaryIndexUtils.constructColumnFamily(
				dataAdapter.getAdapterId(),
				TEMPORAL_PARTIAL_FIELD_ID);
		scan.addFamily(columnFamily);
		scan.setStartRow(Lexicoders.LONG.toByteArray(DATE_FORMAT.parse(
				"11-30-2012").getTime()));
		scan.setStopRow(Lexicoders.LONG.toByteArray(DATE_FORMAT.parse(
				"11-30-2014").getTime()));
		final ResultScanner results = table.getScanner(scan);
		int numResults = 0;
		for (final Result result : results) {
			numResults += 1;
			final NavigableMap<byte[], byte[]> qualifierToValueMap = result.getFamilyMap(columnFamily);
			final Set<Entry<byte[], byte[]>> entries = qualifierToValueMap.entrySet();
			Assert.assertTrue(entries.size() == 1);
			for (final Entry<byte[], byte[]> entry : entries) {
				final byte[] cq = entry.getKey();
				final ByteArrayId fieldId = SecondaryIndexUtils.getFieldId(cq);
				Assert.assertTrue(fieldId.equals(GEOMETRY_FIELD_ID));
				final String dataId = SecondaryIndexUtils.getDataId(cq);
				Assert.assertTrue(dataId.equals(allDataIds.get(1)));
			}
		}
		table.close();
		Assert.assertTrue(numResults == 1);
	}

	/**
	 * 
	 * @param builder
	 *            SimpleFeature builder to be used
	 * @param lng
	 *            - coordinate longitude
	 * @param lat
	 *            - coordinate latitude
	 * @param dateField
	 *            -
	 * @param doubleField
	 * @param stringField
	 * @return
	 */
	private SimpleFeature buildSimpleFeature(
			final SimpleFeatureBuilder builder,
			final double lng,
			final double lat,
			final Date dateField,
			final double doubleField,
			final String stringField ) {
		builder.set(
				GEOMETRY_FIELD,
				GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
						lng,
						lat)));
		builder.set(
				TEMPORAL_JOIN_FIELD,
				dateField);
		builder.set(
				TEMPORAL_FULL_FIELD,
				dateField);
		builder.set(
				TEMPORAL_PARTIAL_FIELD,
				dateField);
		builder.set(
				NUMERIC_JOIN_FIELD,
				doubleField);
		builder.set(
				NUMERIC_FULL_FIELD,
				doubleField);
		builder.set(
				NUMERIC_PARTIAL_FIELD,
				doubleField);
		builder.set(
				TEXT_JOIN_FIELD,
				stringField);
		builder.set(
				TEXT_FULL_FIELD,
				stringField);
		builder.set(
				TEXT_PARTIAL_FIELD,
				stringField);
		final String dataId = UUID.randomUUID().toString();
		allDataIds.add(dataId);
		return builder.buildFeature(dataId);
	}

}
