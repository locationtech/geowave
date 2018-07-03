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
package mil.nga.giat.geowave.adapter.vector.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider.SpatialIndexBuilder;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider.SpatialTemporalIndexBuilder;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.SinglePartitionInsertionIds;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.index.sfc.data.NumericValue;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.NullIndex;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.query.BasicQuery.ConstraintData;
import mil.nga.giat.geowave.core.store.query.BasicQuery.ConstraintSet;
import mil.nga.giat.geowave.core.store.query.BasicQuery.Constraints;

public class ChooseBestMatchIndexQueryStrategyTest
{
	final PrimaryIndex IMAGE_CHIP_INDEX1 = new NullIndex(
			"IMAGERY_CHIPS1");
	final PrimaryIndex IMAGE_CHIP_INDEX2 = new NullIndex(
			"IMAGERY_CHIPS2");
	private static long SEED = 12345;
	private static long ROWS = 1000000;

	@Test
	public void testChooseSpatialTemporalWithStats() {
		final PrimaryIndex temporalindex = new SpatialTemporalIndexBuilder().createIndex();
		final PrimaryIndex spatialIndex = new SpatialIndexBuilder().createIndex();

		final RowRangeHistogramStatistics<SimpleFeature> rangeTempStats = new RowRangeHistogramStatistics<>(
				null,
				temporalindex.getId(),
				null);

		final RowRangeHistogramStatistics<SimpleFeature> rangeStats = new RowRangeHistogramStatistics<>(
				null,
				spatialIndex.getId(),
				null);

		final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap = new HashMap<>();
		statsMap.put(
				RowRangeHistogramStatistics.composeId(
						spatialIndex.getId(),
						null),
				rangeStats);
		statsMap.put(
				RowRangeHistogramStatistics.composeId(
						temporalindex.getId(),
						null),
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
		PrimaryIndex index = new SpatialIndexBuilder().createIndex();
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

		final Iterator<Index<?, ?>> it = getIndices(
				statsMap,
				query,
				strategy);
		assertTrue(it.hasNext());
		assertEquals(
				temporalindex.getId(),
				it.next().getId());
		assertFalse(it.hasNext());

	}

	public Iterator<Index<?, ?>> getIndices(
			final Map<ByteArrayId, DataStatistics<SimpleFeature>> stats,
			final BasicQuery query,
			final ChooseBestMatchIndexQueryStrategy strategy ) {
		return strategy.getIndices(
				stats,
				query,
				new PrimaryIndex[] {
					IMAGE_CHIP_INDEX1,
					new SpatialTemporalIndexBuilder().createIndex(),
					new SpatialIndexBuilder().createIndex(),
					IMAGE_CHIP_INDEX2
				});
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
