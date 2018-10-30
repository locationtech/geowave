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
package org.locationtech.geowave.adapter.vector.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;

import org.apache.commons.lang.ArrayUtils;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.adapter.vector.stats.FeatureCountMinSketchStatistics.FeatureCountMinSketchConfig;
import org.locationtech.geowave.adapter.vector.stats.FeatureFixedBinNumericStatistics.FeatureFixedBinConfig;
import org.locationtech.geowave.adapter.vector.stats.FeatureHyperLogLogStatistics.FeatureHyperLogLogConfig;
import org.locationtech.geowave.adapter.vector.stats.FeatureNumericHistogramStatistics.FeatureNumericHistogramConfig;
import org.locationtech.geowave.adapter.vector.stats.StatsConfigurationCollection.SimpleFeatureStatsConfigurationCollection;
import org.locationtech.geowave.adapter.vector.util.SimpleFeatureUserDataConfigurationSet;
import org.locationtech.geowave.core.geotime.store.query.api.VectorStatisticsQueryBuilder;
import org.locationtech.geowave.core.geotime.store.statistics.FeatureBoundingBoxStatistics;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapterWrapper;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsId;
import org.locationtech.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class StatsManagerTest
{

	private SimpleFeatureType schema;
	InternalDataAdapter<SimpleFeature> dataAdapter;

	@Before
	public void setup()
			throws SchemaException,
			CQLException,
			ParseException {
		schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,whennot:Date,somewhere:Polygon,pid:String");
		dataAdapter = new InternalDataAdapterWrapper<>(
				new FeatureDataAdapter(
						schema,
						new GlobalVisibilityHandler<SimpleFeature, Object>(
								"default")),
				(short) -1);
	}

	@Test
	public void test() {
		final StatsManager statsManager = new StatsManager(
				dataAdapter,
				schema);
		final StatisticsId[] ids = statsManager.getSupportedStatistics();
		assertTrue(ids.length > 6);
		assertTrue(ArrayUtils.contains(
				ids,
				FeatureNumericRangeStatistics.STATS_TYPE.newBuilder().fieldName(
						"pop").build().getId()));
		assertTrue(ArrayUtils.contains(
				ids,
				VectorStatisticsQueryBuilder.newBuilder().factory().bbox().fieldName(
						"somewhere").build().getId()));
		assertTrue(ArrayUtils.contains(
				ids,
				VectorStatisticsQueryBuilder.newBuilder().factory().bbox().fieldName(
						"geometry").build().getId()));
		assertTrue(ArrayUtils.contains(
				ids,
				VectorStatisticsQueryBuilder.newBuilder().factory().timeRange().fieldName(
						"when").build().getId()));
		assertTrue(ArrayUtils.contains(
				ids,
				VectorStatisticsQueryBuilder.newBuilder().factory().timeRange().fieldName(
						"whennot").build().getId()));

		// can each type be created uniquely
		InternalDataStatistics<SimpleFeature, ?, ?> stat = statsManager
				.createDataStatistics(VectorStatisticsQueryBuilder.newBuilder().factory().bbox().fieldName(
						"somewhere").build().getId());
		stat.setAdapterId((short) -1);
		assertNotNull(stat);
		assertFalse(stat == statsManager.createDataStatistics(VectorStatisticsQueryBuilder
				.newBuilder()
				.factory()
				.bbox()
				.fieldName(
						"somewhere")
				.build()
				.getId()));

		final FeatureBoundingBoxStatistics newStat = new FeatureBoundingBoxStatistics(
				(short) -1,
				"somewhere");
		newStat.fromBinary(stat.toBinary());
		assertEquals(
				newStat.getMaxY(),
				((FeatureBoundingBoxStatistics) stat).getMaxY(),
				0.001);
		assertEquals(
				newStat.getFieldName(),
				((FeatureBoundingBoxStatistics) stat).getFieldName());

		stat = statsManager.createDataStatistics(VectorStatisticsQueryBuilder
				.newBuilder()
				.factory()
				.timeRange()
				.fieldName(
						"when")
				.build()
				.getId());
		assertNotNull(stat);
		assertFalse(stat == statsManager.createDataStatistics(VectorStatisticsQueryBuilder
				.newBuilder()
				.factory()
				.timeRange()
				.fieldName(
						"when")
				.build()
				.getId()));

		stat = statsManager.createDataStatistics(FeatureNumericRangeStatistics.STATS_TYPE.newBuilder().fieldName(
				"pop").build().getId());
		assertNotNull(stat);
		assertFalse(stat == statsManager.createDataStatistics(FeatureNumericRangeStatistics.STATS_TYPE
				.newBuilder()
				.fieldName(
						"pop")
				.build()
				.getId()));

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
		final InternalDataAdapter<SimpleFeature> dataAdapter = new InternalDataAdapterWrapper<>(
				new FeatureDataAdapter(
						schema,
						new GlobalVisibilityHandler<SimpleFeature, Object>(
								"default")),
				(short) -1);

		final StatsManager statsManager = new StatsManager(
				dataAdapter,
				schema);

		final StatisticsId[] ids = statsManager.getSupportedStatistics();
		assertEquals(
				9,
				ids.length);
		InternalDataStatistics<SimpleFeature, ?, ?> stat = statsManager
				.createDataStatistics(FeatureFixedBinNumericStatistics.STATS_TYPE.newBuilder().fieldName(
						"pop").build().getId());
		assertNotNull(stat);
		stat = statsManager.createDataStatistics(FeatureNumericHistogramStatistics.STATS_TYPE.newBuilder().fieldName(
				"pop").build().getId());
		assertNotNull(stat);
		stat = statsManager.createDataStatistics(FeatureHyperLogLogStatistics.STATS_TYPE.newBuilder().fieldName(
				"pid").build().getId());
		assertNotNull(stat);
		stat = statsManager.createDataStatistics(FeatureCountMinSketchStatistics.STATS_TYPE.newBuilder().fieldName(
				"pid").build().getId());
		assertNotNull(stat);

		final SimpleFeatureUserDataConfigurationSet config = new SimpleFeatureUserDataConfigurationSet();
		config.addConfigurations(
				schema.getTypeName(),
				new SimpleFeatureStatsConfigurationCollection());
		config.configureFromType(schema);
		config.fromBinary(config.toBinary());
	}
}
