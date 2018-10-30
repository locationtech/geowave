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
package org.locationtech.geowave.adapter.vector.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.junit.Test;
import org.locationtech.geowave.core.geotime.index.dimension.LatitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.LongitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.TimeDefinition;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider.SpatialIndexBuilder;
import org.locationtech.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider.SpatialTemporalIndexBuilder;
import org.locationtech.geowave.core.geotime.store.query.api.VectorStatisticsQueryBuilder;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.SinglePartitionInsertionIds;
import org.locationtech.geowave.core.index.sfc.data.BasicNumericDataset;
import org.locationtech.geowave.core.index.sfc.data.NumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;
import org.locationtech.geowave.core.index.sfc.data.NumericValue;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsId;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.entities.GeoWaveKeyImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveRowImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.locationtech.geowave.core.store.index.NullIndex;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery.ConstraintData;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery.ConstraintSet;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery.Constraints;
import org.opengis.feature.simple.SimpleFeature;

import com.beust.jcommander.internal.Maps;

public class ChooseBestMatchIndexQueryStrategyTest
{
	final Index IMAGE_CHIP_INDEX1 = new NullIndex(
			"IMAGERY_CHIPS1");
	final Index IMAGE_CHIP_INDEX2 = new NullIndex(
			"IMAGERY_CHIPS2");
	private static long SEED = 12345;
	private static long ROWS = 1000000;

	@Test
	public void testChooseSpatialTemporalWithStats() {
		final Index temporalindex = new SpatialTemporalIndexBuilder().createIndex();
		final Index spatialIndex = new SpatialIndexBuilder().createIndex();

		final RowRangeHistogramStatistics<SimpleFeature> rangeTempStats = new RowRangeHistogramStatistics<>(
				null,
				temporalindex.getName(),
				null);

		final RowRangeHistogramStatistics<SimpleFeature> rangeStats = new RowRangeHistogramStatistics<>(
				null,
				spatialIndex.getName(),
				null);

		final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> statsMap = new HashMap<>();
		statsMap.put(
				VectorStatisticsQueryBuilder.newBuilder().factory().rowHistogram().indexName(
						spatialIndex.getName()).build().getId(),
				rangeStats);
		statsMap.put(
				VectorStatisticsQueryBuilder.newBuilder().factory().rowHistogram().indexName(
						temporalindex.getName()).build().getId(),
				rangeTempStats);

		final ChooseBestMatchIndexQueryStrategy strategy = new ChooseBestMatchIndexQueryStrategy();

		final ConstraintSet cs1 = new ConstraintSet();
		cs1.addConstraint(
				LatitudeDefinition.class,
				new ConstraintData(
						new ConstrainedIndexValue(
								0.3,
								0.5),
						true));

		cs1.addConstraint(
				LongitudeDefinition.class,
				new ConstraintData(
						new ConstrainedIndexValue(
								0.4,
								0.7),
						true));

		final ConstraintSet cs2a = new ConstraintSet();
		cs2a.addConstraint(
				TimeDefinition.class,
				new ConstraintData(
						new ConstrainedIndexValue(
								0.1,
								0.2),
						true));

		final Constraints constraints = new Constraints(
				Arrays.asList(cs2a)).merge(Collections.singletonList(cs1));

		final BasicQuery query = new BasicQuery(
				constraints);

		final NumericIndexStrategy temporalIndexStrategy = new SpatialTemporalIndexBuilder()
				.createIndex()
				.getIndexStrategy();
		final Random r = new Random(
				SEED);
		for (int i = 0; i < ROWS; i++) {
			final double x = r.nextDouble();
			final double y = r.nextDouble();
			final double t = r.nextDouble();
			final InsertionIds id = temporalIndexStrategy.getInsertionIds(new BasicNumericDataset(
					new NumericData[] {
						new NumericValue(
								x),
						new NumericValue(
								y),
						new NumericValue(
								t)
					}));
			for (final SinglePartitionInsertionIds range : id.getPartitionKeys()) {
				rangeTempStats.entryIngested(
						null,
						new GeoWaveRowImpl(
								new GeoWaveKeyImpl(
										new byte[] {
											1
										},
										(short) 1,
										range.getPartitionKey().getBytes(),
										range.getSortKeys().get(
												0).getBytes(),
										0),
								new GeoWaveValue[] {}));
			}
		}
		final Index index = new SpatialIndexBuilder().createIndex();
		final NumericIndexStrategy indexStrategy = index.getIndexStrategy();

		for (int i = 0; i < ROWS; i++) {
			final double x = r.nextDouble();
			final double y = r.nextDouble();
			final double t = r.nextDouble();
			final InsertionIds id = indexStrategy.getInsertionIds(new BasicNumericDataset(
					new NumericData[] {
						new NumericValue(
								x),
						new NumericValue(
								y),
						new NumericValue(
								t)
					}));
			for (final SinglePartitionInsertionIds range : id.getPartitionKeys()) {
				rangeStats.entryIngested(
						null,
						new GeoWaveRowImpl(
								new GeoWaveKeyImpl(
										new byte[] {
											1
										},
										(short) 1,
										range.getPartitionKey().getBytes(),
										range.getSortKeys().get(
												0).getBytes(),
										0),
								new GeoWaveValue[] {}));
			}
		}

		final Iterator<Index> it = getIndices(
				statsMap,
				query,
				strategy);
		assertTrue(it.hasNext());
		assertEquals(
				temporalindex.getName(),
				it.next().getName());
		assertFalse(it.hasNext());

	}

	public Iterator<Index> getIndices(
			final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> stats,
			final BasicQuery query,
			final ChooseBestMatchIndexQueryStrategy strategy ) {
		return strategy.getIndices(
				stats,
				query,
				new Index[] {
					IMAGE_CHIP_INDEX1,
					new SpatialTemporalIndexBuilder().createIndex(),
					new SpatialIndexBuilder().createIndex(),
					IMAGE_CHIP_INDEX2
				},
				Maps.newHashMap());
	}

	public static class ConstrainedIndexValue extends
			NumericRange implements
			CommonIndexValue
	{

		/**
		*
		*/
		private static final long serialVersionUID = 1L;

		public ConstrainedIndexValue(
				final double min,
				final double max ) {
			super(
					min,
					max);
			//
		}

		@Override
		public byte[] getVisibility() {
			return new byte[0];
		}

		@Override
		public void setVisibility(
				final byte[] visibility ) {

		}

		@Override
		public boolean overlaps(
				final NumericDimensionField[] field,
				final NumericData[] rangeData ) {
			return false;
		}

	}
}
