/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.test.query;

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
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.Connection;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.ResultScanner;
//import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.adapter.vector.index.NumericSecondaryIndexConfiguration;
import org.locationtech.geowave.adapter.vector.index.TemporalSecondaryIndexConfiguration;
import org.locationtech.geowave.adapter.vector.index.TextSecondaryIndexConfiguration;
import org.locationtech.geowave.adapter.vector.util.SimpleFeatureUserDataConfigurationSet;
import org.locationtech.geowave.core.geotime.store.dimension.GeometryWrapper;
import org.locationtech.geowave.core.geotime.store.query.SpatialQuery;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.SimpleFeatureUserDataConfiguration;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.lexicoder.Lexicoders;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapterWrapper;
import org.locationtech.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.FilterableConstraints;
import org.locationtech.geowave.core.store.index.SecondaryIndexDataStore;
import org.locationtech.geowave.core.store.index.SecondaryIndexImpl;
import org.locationtech.geowave.core.store.index.SecondaryIndexType;
import org.locationtech.geowave.core.store.index.SecondaryIndexUtils;
import org.locationtech.geowave.core.store.index.numeric.NumericGreaterThanConstraint;
import org.locationtech.geowave.core.store.index.temporal.TemporalQueryConstraint;
import org.locationtech.geowave.core.store.index.text.TextQueryConstraint;
import org.locationtech.geowave.core.store.query.constraints.DataIdQuery;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.datastore.accumulo.cli.config.AccumuloRequiredOptions;
import org.locationtech.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStore;
import org.locationtech.geowave.datastore.accumulo.util.ConnectorPool;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;

@RunWith(GeoWaveITRunner.class)
public class SecondaryIndexIT
{
	@GeoWaveTestStore({
		GeoWaveStoreType.ACCUMULO
	// HBase's VisibilityController isn't compatible with
	// this test. We'll leave HBase out until the *real*
	// secondary index implementation is complete.
	// GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStoreOptions;

	private FeatureDataAdapter dataAdapter;
	private Index index;
	private DataStore dataStore;
	private SecondaryIndexDataStore secondaryDataStore;
	private InternalAdapterStore internalAdapterStore;
	private final List<ByteArray> allIndexIds = new ArrayList<>();
	private final List<String> allDataIds = new ArrayList<>();
	private int numAttributes;
	private List<SecondaryIndexImpl<SimpleFeature>> allSecondaryIndices;
	private QueryConstraints query;
	private Point expectedPoint;
	private String expectedDataId;

	private final static Logger LOGGER = Logger.getLogger(SecondaryIndexIT.class);
	private static long startMillis;

	@BeforeClass
	public static void startTimer() {
		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*    RUNNING SecondaryIndexIT           *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTest() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*    FINISHED SecondaryIndexIT          *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@Test
	public void testSecondaryIndicesManually()
			throws AccumuloException,
			AccumuloSecurityException,
			TableNotFoundException,
			ParseException,
			IOException {
		Assert.assertTrue(allIndexIds.size() == 3);

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

			// final HBaseRequiredOptions options = (HBaseRequiredOptions)
			// dataStoreOptions.getFactoryOptions();
			//
			// final Connection connection =
			// ConnectionPool.getInstance().getConnection(
			// options.getZookeeper());

			// numericJoinHBase(connection);
			// textJoinHBase(connection);
			// temporalJoinHBase(connection);
			// numericFullHBase(connection);
			// textFullHBase(connection);
			// temporalFullHBase(connection);
			// numericPartialHBase(connection);
			// textPartialHBase(connection);
			// temporalPartialHBase(connection);
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
			// Assert.assertTrue(secondaryDataStore instanceof
			// HBaseSecondaryIndexDataStore);
		}

		Assert.assertTrue(allSecondaryIndices.size() == 9);

		final InternalDataAdapter<SimpleFeature> internalDataAdapter = new InternalDataAdapterWrapper(
				dataAdapter,
				internalAdapterStore.getAdapterId(dataAdapter.getTypeName()));

		for (final SecondaryIndexImpl<SimpleFeature> secondaryIndex : allSecondaryIndices) {

			final List<SimpleFeature> queryResults = new ArrayList<>();
			try (final CloseableIterator<SimpleFeature> results = secondaryDataStore.query(
					secondaryIndex,
					secondaryIndex.getFieldName(),
					internalDataAdapter,
					index,
					query,
					DEFAULT_AUTHORIZATIONS)) {

				while (results.hasNext()) {
					queryResults.add(results.next());
				}
			}

			final int numResults = queryResults.size();
			Assert.assertTrue(
					secondaryIndex.getName() + " returned " + numResults + " results (should have been 1)",
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
		final QueryConstraints deleteQuery = new DataIdQuery(
				new ByteArray(
						expectedDataId));
		dataStore.delete(QueryBuilder.newBuilder().addTypeName(
				dataAdapter.getTypeName()).indexName(
				index.getName()).constraints(
				deleteQuery).build());

		for (final SecondaryIndexImpl<SimpleFeature> secondaryIndex : allSecondaryIndices) {

			int numResults = 0;
			try (final CloseableIterator<SimpleFeature> results = secondaryDataStore.query(
					secondaryIndex,
					secondaryIndex.getFieldName(),
					internalDataAdapter,
					index,
					query,
					DEFAULT_AUTHORIZATIONS)) {

				while (results.hasNext()) {
					results.next();
					numResults++;
				}
			}
			Assert.assertTrue(
					secondaryIndex.getName() + " returned " + numResults + " results (should have been 0)",
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
	private static final String TEMPORAL_JOIN_FIELD = "dateField";
	private static final String TEXT_JOIN_FIELD = "stringField";
	private static final String NUMERIC_FULL_FIELD = "doubleField2";
	private static final String TEMPORAL_FULL_FIELD = "dateField2";
	private static final String TEXT_FULL_FIELD = "stringField2";
	private static final String NUMERIC_PARTIAL_FIELD = "doubleField3";
	private static final String TEMPORAL_PARTIAL_FIELD = "dateField3";
	private static final String TEXT_PARTIAL_FIELD = "stringField3";
	private static final DateFormat DATE_FORMAT = new SimpleDateFormat(
			"dd-MM-yyyy");
	private static final ByteArray GEOMETRY_FIELD_ID = new ByteArray(
			GeometryWrapper.DEFAULT_GEOMETRY_FIELD_NAME.getBytes());
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
		TestUtils.deleteAll(dataStoreOptions);

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
		dataAdapter.init(index);
		dataStore.addType(
				dataAdapter,
				index);
		try (@SuppressWarnings("unchecked")
		final Writer<SimpleFeature> writer = dataStore.createWriter(dataAdapter.getTypeName())) {
			for (final SimpleFeature aFeature : features) {
				allIndexIds.addAll(writer.write(
						aFeature).getCompositeInsertionIds());
			}
		}

		internalAdapterStore = dataStoreOptions.createInternalAdapterStore();
		allSecondaryIndices = dataAdapter.getSupportedSecondaryIndices();

		final Map<String, FilterableConstraints> additionalConstraints = new HashMap<>();

		final Number number = 25d;
		additionalConstraints.put(
				NUMERIC_JOIN_FIELD,
				new NumericGreaterThanConstraint(
						NUMERIC_JOIN_FIELD,
						number));
		additionalConstraints.put(
				NUMERIC_FULL_FIELD,
				new NumericGreaterThanConstraint(
						NUMERIC_FULL_FIELD,
						number));
		additionalConstraints.put(
				NUMERIC_PARTIAL_FIELD,
				new NumericGreaterThanConstraint(
						NUMERIC_PARTIAL_FIELD,
						number));

		final String matchValue = "ccc";
		additionalConstraints.put(
				TEXT_JOIN_FIELD,
				new TextQueryConstraint(
						TEXT_JOIN_FIELD,
						matchValue,
						true));
		additionalConstraints.put(
				TEXT_FULL_FIELD,
				new TextQueryConstraint(
						TEXT_FULL_FIELD,
						matchValue,
						true));
		additionalConstraints.put(
				TEXT_PARTIAL_FIELD,
				new TextQueryConstraint(
						TEXT_PARTIAL_FIELD,
						matchValue,
						true));

		final Date start = DATE_FORMAT.parse("12-24-2015");
		final Date end = DATE_FORMAT.parse("12-26-2015");
		additionalConstraints.put(
				TEMPORAL_JOIN_FIELD,
				new TemporalQueryConstraint(
						TEMPORAL_JOIN_FIELD,
						start,
						end));
		additionalConstraints.put(
				TEMPORAL_FULL_FIELD,
				new TemporalQueryConstraint(
						TEMPORAL_FULL_FIELD,
						start,
						end));
		additionalConstraints.put(
				TEMPORAL_PARTIAL_FIELD,
				new TemporalQueryConstraint(
						TEMPORAL_PARTIAL_FIELD,
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
						dataAdapter.getTypeName(),
						NUMERIC_JOIN_FIELD)));
		int numResults = 0;

		for (final Entry<Key, Value> entry : scanner) {
			numResults += 1;
			final ByteArray primaryRowId = SecondaryIndexUtils.getPrimaryRowId(entry
					.getKey()
					.getColumnQualifierData()
					.getBackingArray());
			if (numResults == 1) {
				Assert.assertTrue(primaryRowId.equals(allIndexIds.get(0)));
			}
			else if (numResults == 2) {
				Assert.assertTrue(primaryRowId.equals(allIndexIds.get(1)));
			}
		}
		scanner.close();
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
						new ByteArray(
								"bbb").getBytes())));
		scanner.fetchColumnFamily(new Text(
				SecondaryIndexUtils.constructColumnFamily(
						dataAdapter.getTypeName(),
						TEXT_JOIN_FIELD)));
		int numResults = 0;
		for (final Entry<Key, Value> entry : scanner) {
			numResults += 1;
			final ByteArray primaryRowId = SecondaryIndexUtils.getPrimaryRowId(entry
					.getKey()
					.getColumnQualifierData()
					.getBackingArray());
			Assert.assertTrue(primaryRowId.equals(allIndexIds.get(1)));
		}
		scanner.close();
		Assert.assertTrue(numResults == 1);
	}

	private void temporalJoinAccumulo(
			final Connector connector )
			throws TableNotFoundException,
			ParseException {
		final Scanner scanner = connector.createScanner(
				TEMPORAL_JOIN_TABLE,
				DEFAULT_ACCUMULO_AUTHORIZATIONS);
		final Text startText = new Text(
				Lexicoders.LONG.toByteArray(DATE_FORMAT.parse(
						"11-30-2012").getTime()));
		final Text stopText = new Text(
				Lexicoders.LONG.toByteArray(DATE_FORMAT.parse(
						"11-30-2014").getTime()));

		scanner.setRange(new Range(
				startText,
				stopText));
		final byte[] colFam = SecondaryIndexUtils.constructColumnFamily(
				dataAdapter.getTypeName(),
				TEMPORAL_JOIN_FIELD);

		scanner.fetchColumnFamily(new Text(
				colFam));
		int numResults = 0;
		for (final Entry<Key, Value> entry : scanner) {
			numResults += 1;
			final ByteArray primaryRowId = SecondaryIndexUtils.getPrimaryRowId(entry
					.getKey()
					.getColumnQualifierData()
					.getBackingArray());
			Assert.assertTrue(primaryRowId.equals(allIndexIds.get(1)));
		}
		scanner.close();
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
						dataAdapter.getTypeName(),
						NUMERIC_FULL_FIELD)));
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

	private void textFullAccumulo(
			final Connector connector )
			throws TableNotFoundException {
		final Scanner scanner = connector.createScanner(
				TEXT_FULL_TABLE,
				DEFAULT_ACCUMULO_AUTHORIZATIONS);
		scanner.setRange(new Range(
				new Text(
						new ByteArray(
								"bbb").getBytes())));
		scanner.fetchColumnFamily(new Text(
				SecondaryIndexUtils.constructColumnFamily(
						dataAdapter.getTypeName(),
						TEXT_FULL_FIELD)));
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
						dataAdapter.getTypeName(),
						TEMPORAL_FULL_FIELD)));
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
						dataAdapter.getTypeName(),
						NUMERIC_PARTIAL_FIELD)));
		int numResults = 0;
		for (final Entry<Key, Value> entry : scanner) {
			numResults += 1;
			final byte[] cq = entry.getKey().getColumnQualifierData().getBackingArray();
			final String fieldId = SecondaryIndexUtils.getFieldName(cq);
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

	private void textPartialAccumulo(
			final Connector connector )
			throws TableNotFoundException {
		final Scanner scanner = connector.createScanner(
				TEXT_PARTIAL_TABLE,
				DEFAULT_ACCUMULO_AUTHORIZATIONS);
		scanner.setRange(new Range(
				new Text(
						new ByteArray(
								"bbb").getBytes())));
		scanner.fetchColumnFamily(new Text(
				SecondaryIndexUtils.constructColumnFamily(
						dataAdapter.getTypeName(),
						TEXT_PARTIAL_FIELD)));
		int numResults = 0;
		for (final Entry<Key, Value> entry : scanner) {
			numResults += 1;
			final byte[] cq = entry.getKey().getColumnQualifierData().getBackingArray();
			final String fieldId = SecondaryIndexUtils.getFieldName(cq);
			Assert.assertTrue(fieldId.equals(GEOMETRY_FIELD_ID));
			final String dataId = SecondaryIndexUtils.getDataId(cq);
			Assert.assertTrue(dataId.equals(allDataIds.get(1)));
		}
		scanner.close();
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
						dataAdapter.getTypeName(),
						TEMPORAL_PARTIAL_FIELD)));
		int numResults = 0;
		for (final Entry<Key, Value> entry : scanner) {
			numResults += 1;
			final byte[] cq = entry.getKey().getColumnQualifierData().getBackingArray();
			final String fieldId = SecondaryIndexUtils.getFieldName(cq);
			Assert.assertTrue(fieldId.equals(GEOMETRY_FIELD_ID));
			final String dataId = SecondaryIndexUtils.getDataId(cq);
			Assert.assertTrue(dataId.equals(allDataIds.get(1)));
		}
		scanner.close();
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
