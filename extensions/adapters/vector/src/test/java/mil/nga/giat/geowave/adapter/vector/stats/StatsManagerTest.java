/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.adapter.vector.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureCountMinSketchStatistics.FeatureCountMinSketchConfig;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureFixedBinNumericStatistics.FeatureFixedBinConfig;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureHyperLogLogStatistics.FeatureHyperLogLogConfig;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureNumericHistogramStatistics.FeatureNumericHistogramConfig;
import mil.nga.giat.geowave.adapter.vector.stats.StatsConfigurationCollection.SimpleFeatureStatsConfigurationCollection;
import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfigurationSet;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;

import org.apache.commons.lang.ArrayUtils;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class StatsManagerTest
{

	private SimpleFeatureType schema;
	FeatureDataAdapter dataAdapter;

	@Before
	public void setup()
			throws SchemaException,
			CQLException,
			ParseException {
		schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,whennot:Date,somewhere:Polygon,pid:String");
		dataAdapter = new FeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
	}

	@Test
	public void test() {
		final StatsManager statsManager = new StatsManager(
				dataAdapter,
				schema);
		final ByteArrayId[] ids = statsManager.getSupportedStatisticsIds();
		assertTrue(ids.length > 6);
		assertTrue(ArrayUtils.contains(
				ids,
				FeatureNumericRangeStatistics.composeId("pop")));
		assertTrue(ArrayUtils.contains(
				ids,
				FeatureBoundingBoxStatistics.composeId("somewhere")));
		assertTrue(ArrayUtils.contains(
				ids,
				FeatureBoundingBoxStatistics.composeId("geometry")));
		assertTrue(ArrayUtils.contains(
				ids,
				FeatureTimeRangeStatistics.composeId("when")));
		assertTrue(ArrayUtils.contains(
				ids,
				FeatureTimeRangeStatistics.composeId("whennot")));

		// can each type be created uniquely
		DataStatistics<SimpleFeature> stat = statsManager.createDataStatistics(
				dataAdapter,
				FeatureBoundingBoxStatistics.composeId("somewhere"));
		assertNotNull(stat);
		assertFalse(stat == statsManager.createDataStatistics(
				dataAdapter,
				FeatureBoundingBoxStatistics.composeId("somewhere")));

		final FeatureBoundingBoxStatistics newStat = new FeatureBoundingBoxStatistics();
		newStat.fromBinary(stat.toBinary());
		assertEquals(
				newStat.getMaxY(),
				((FeatureBoundingBoxStatistics) stat).getMaxY(),
				0.001);
		assertEquals(
				newStat.getFieldName(),
				((FeatureBoundingBoxStatistics) stat).getFieldName());

		stat = statsManager.createDataStatistics(
				dataAdapter,
				FeatureTimeRangeStatistics.composeId("when"));
		assertNotNull(stat);
		assertFalse(stat == statsManager.createDataStatistics(
				dataAdapter,
				FeatureTimeRangeStatistics.composeId("when")));

		stat = statsManager.createDataStatistics(
				dataAdapter,
				FeatureNumericRangeStatistics.composeId("pop"));
		assertNotNull(stat);
		assertFalse(stat == statsManager.createDataStatistics(
				dataAdapter,
				FeatureNumericRangeStatistics.composeId("pop")));

	}

	@Test
	public void forcedConfiguration()
			throws SchemaException,
			JsonGenerationException,
			JsonMappingException,
			IndexOutOfBoundsException,
			IOException {
		final SimpleFeatureType schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,whennot:Date,somewhere:Polygon,pid:String");

		schema.getDescriptor(
				1).getUserData().put(
				"stats",
				new StatsConfigurationCollection(
						Arrays.asList(
								new FeatureFixedBinConfig(
										0.0,
										1.0,
										24),
								new FeatureNumericHistogramConfig())));
		schema.getDescriptor(
				5).getUserData().put(
				"stats",
				new StatsConfigurationCollection(
						Arrays.asList(
								new FeatureCountMinSketchConfig(
										0.01,
										0.97),
								new FeatureHyperLogLogConfig(
										24))));
		final FeatureDataAdapter dataAdapter = new FeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));

		final StatsManager statsManager = new StatsManager(
				dataAdapter,
				schema);

		final ByteArrayId[] ids = statsManager.getSupportedStatisticsIds();
		assertEquals(
				9,
				ids.length);
		DataStatistics<SimpleFeature> stat = statsManager.createDataStatistics(
				dataAdapter,
				FeatureFixedBinNumericStatistics.composeId("pop"));
		assertNotNull(stat);
		stat = statsManager.createDataStatistics(
				dataAdapter,
				FeatureNumericHistogramStatistics.composeId("pop"));
		assertNotNull(stat);
		stat = statsManager.createDataStatistics(
				dataAdapter,
				FeatureHyperLogLogStatistics.composeId("pid"));
		assertNotNull(stat);
		stat = statsManager.createDataStatistics(
				dataAdapter,
				FeatureCountMinSketchStatistics.composeId("pid"));
		assertNotNull(stat);

		final SimpleFeatureUserDataConfigurationSet config = new SimpleFeatureUserDataConfigurationSet();
		config.addConfigurations(
				schema.getTypeName(),
				new SimpleFeatureStatsConfigurationCollection());
		config.configureFromType(schema);
		config.fromJsonString(
				config.asJsonString(),
				schema);
	}

	private String dump(
			final String value ) {
		System.out.println(value);
		return value;
	}
}
