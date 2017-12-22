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

import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider.SpatialIndexBuilder;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider.SpatialTemporalIndexBuilder;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.FixedBinNumericHistogram.FixedBinNumericHistogramFactory;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.NullIndex;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.query.BasicQuery.ConstraintData;
import mil.nga.giat.geowave.core.store.query.BasicQuery.ConstraintSet;
import mil.nga.giat.geowave.core.store.query.BasicQuery.Constraints;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

public class ChooseBestMatchIndexQueryStrategyTest
{
	final PrimaryIndex IMAGE_CHIP_INDEX1 = new NullIndex(
			"IMAGERY_CHIPS1");
	final PrimaryIndex IMAGE_CHIP_INDEX2 = new NullIndex(
			"IMAGERY_CHIPS2");

	@Test
	public void testChooseSpatialWithStats() {
		final PrimaryIndex temporalindex = new SpatialTemporalIndexBuilder().createIndex();
		final PrimaryIndex spatialIndex = new SpatialIndexBuilder().createIndex();

		final RowRangeHistogramStatistics<SimpleFeature> rangeTempStats = new RowRangeHistogramStatistics<>(
				temporalindex.getId(),
				temporalindex.getId(),
				new FixedBinNumericHistogramFactory(),
				1024);

		final RowRangeHistogramStatistics<SimpleFeature> rangeStats = new RowRangeHistogramStatistics<>(
				spatialIndex.getId(),
				spatialIndex.getId(),
				new FixedBinNumericHistogramFactory(),
				1024);

		final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap = new HashMap<>();
		statsMap.put(
				RowRangeHistogramStatistics.composeId(spatialIndex.getId()),
				rangeStats);
		statsMap.put(
				RowRangeHistogramStatistics.composeId(temporalindex.getId()),
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

		final PrimaryIndex index = new SpatialTemporalIndexBuilder().createIndex();
		final NumericIndexStrategy temporalIndexStrategy = index.getIndexStrategy();
		final List<MultiDimensionalNumericData> tempConstraints = query.getIndexConstraints(index);

		final List<ByteArrayRange> temporalRanges = DataStoreUtils.constraintsToByteArrayRanges(
				tempConstraints,
				temporalIndexStrategy,
				5000);

		for (final ByteArrayRange range : temporalRanges) {
			rangeTempStats.entryIngested(
					new DataStoreEntryInfo(
							new byte[] {
								1
							},
							Arrays.asList(range.getStart()),
							Arrays.asList(range.getStart()),
							Collections.<FieldInfo<?>> emptyList()),
					null);
			rangeTempStats.entryIngested(
					new DataStoreEntryInfo(
							new byte[] {
								1
							},
							Arrays.asList(range.getEnd()),
							Arrays.asList(range.getEnd()),
							Collections.<FieldInfo<?>> emptyList()),
					null);
		}
		final NumericIndexStrategy indexStrategy = index.getIndexStrategy();
		final List<MultiDimensionalNumericData> spatialConstraints = query.getIndexConstraints(index);

		final List<ByteArrayRange> spatialRanges = DataStoreUtils.constraintsToByteArrayRanges(
				spatialConstraints,
				indexStrategy,
				5000);

		for (final ByteArrayRange range : spatialRanges) {
			rangeStats.entryIngested(
					new DataStoreEntryInfo(
							new byte[] {
								1
							},
							Arrays.asList(range.getStart()),
							Arrays.asList(range.getStart()),
							Collections.<FieldInfo<?>> emptyList()),
					null);
		}

		final Iterator<Index<?, ?>> it = getIndices(
				statsMap,
				query,
				strategy);
		assertTrue(it.hasNext());
		assertEquals(
				spatialIndex.getId(),
				it.next().getId());
		assertFalse(it.hasNext());

	}

	/*
	 * @Test public void testChooseTemporalWithoutStats() { final PrimaryIndex
	 * temporalindex = new SpatialTemporalIndexBuilder().createIndex(); final
	 * ChooseBestMatchIndexQueryStrategy strategy = new
	 * ChooseBestMatchIndexQueryStrategy();
	 * 
	 * final ConstraintSet cs1 = new ConstraintSet(); cs1.addConstraint(
	 * LatitudeDefinition.class, new ConstraintData( new ConstrainedIndexValue(
	 * 0.3, 0.5), true));
	 * 
	 * cs1.addConstraint( LongitudeDefinition.class, new ConstraintData( new
	 * ConstrainedIndexValue( 0.4, 0.7), true));
	 * 
	 * final ConstraintSet cs2a = new ConstraintSet(); cs2a.addConstraint(
	 * TimeDefinition.class, new ConstraintData( new ConstrainedIndexValue( 0.1,
	 * 0.2), true));
	 * 
	 * final Constraints constraints = new Constraints(
	 * Arrays.asList(cs2a)).merge(Collections.singletonList(cs1));
	 * 
	 * final BasicQuery query = new BasicQuery( constraints);
	 * 
	 * final Iterator<Index<?, ?>> it = getIndices( new HashMap<ByteArrayId,
	 * DataStatistics<SimpleFeature>>(), query, strategy);
	 * assertTrue(it.hasNext()); assertEquals( temporalindex.getId(),
	 * it.next().getId()); assertFalse(it.hasNext());
	 * 
	 * }
	 * 
	 * @Test public void testChooseSpatialWithoutStats() { final PrimaryIndex
	 * spatialIndex = new SpatialIndexBuilder().createIndex(); final
	 * ChooseBestMatchIndexQueryStrategy strategy = new
	 * ChooseBestMatchIndexQueryStrategy();
	 * 
	 * final ConstraintSet cs1 = new ConstraintSet(); cs1.addConstraint(
	 * LatitudeDefinition.class, new ConstraintData( new ConstrainedIndexValue(
	 * 0.3, 0.5), true));
	 * 
	 * cs1.addConstraint( LongitudeDefinition.class, new ConstraintData( new
	 * ConstrainedIndexValue( 0.4, 0.7), true));
	 * 
	 * final Constraints constraints = new Constraints(
	 * Collections.singletonList(cs1));
	 * 
	 * final BasicQuery query = new BasicQuery( constraints);
	 * 
	 * final Iterator<Index<?, ?>> it = getIndices( new HashMap<ByteArrayId,
	 * DataStatistics<SimpleFeature>>(), query, strategy);
	 * assertTrue(it.hasNext()); assertEquals( spatialIndex.getId(),
	 * it.next().getId()); assertFalse(it.hasNext());
	 * 
	 * }
	 */

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
