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
import mil.nga.giat.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import mil.nga.giat.geowave.core.geotime.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider.SpatialIndexBuilder;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider.SpatialTemporalIndexBuilder;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.NullIndex;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.query.BasicQuery.ConstraintData;
import mil.nga.giat.geowave.core.store.query.BasicQuery.ConstraintSet;
import mil.nga.giat.geowave.core.store.query.BasicQuery.Constraints;

public class ChooseHeuristicMatchQueryStrategyTest
{
	private static final double HOUR = 3600000;
	private static final double DAY = HOUR * 24;
	private static final double WEEK = DAY * 7;
	private static final double HOUSE = 0.005;
	private static final double BLOCK = 0.07;
	private static final double CITY = 1.25;
	final PrimaryIndex IMAGE_CHIP_INDEX1 = new NullIndex(
			"IMAGERY_CHIPS1");
	final PrimaryIndex IMAGE_CHIP_INDEX2 = new NullIndex(
			"IMAGERY_CHIPS2");

	protected final List<PrimaryIndex> indices = Arrays.asList(
			IMAGE_CHIP_INDEX1,
			new SpatialTemporalIndexBuilder().setNumPartitions(
					5).setBias(
					SpatialTemporalDimensionalityTypeProvider.Bias.BALANCED).setPeriodicity(
					Unit.YEAR).createIndex(),
			new SpatialTemporalIndexBuilder().setNumPartitions(
					10).setBias(
					SpatialTemporalDimensionalityTypeProvider.Bias.BALANCED).setPeriodicity(
					Unit.DAY).createIndex(),
			new SpatialIndexBuilder().createIndex(),
			IMAGE_CHIP_INDEX2);

	@Test
	public void testChooseTemporalWithoutStatsHouseHour() {
		final ChooseHeuristicMatchIndexQueryStrategy strategy = new ChooseHeuristicMatchIndexQueryStrategy();

		final Iterator<Index<?, ?>> it = getIndices(
				new HashMap<ByteArrayId, DataStatistics<SimpleFeature>>(),
				new BasicQuery(
						createConstraints(
								HOUSE,
								HOUSE,
								HOUR)),
				strategy);
		assertTrue(it.hasNext());
		assertEquals(
				indices.get(
						1).getId(),
				it.next().getId());
		assertFalse(it.hasNext());

	}

	@Test
	public void testChooseSpatialWithoutStatsHouseDay() {
		final ChooseHeuristicMatchIndexQueryStrategy strategy = new ChooseHeuristicMatchIndexQueryStrategy();

		final Iterator<Index<?, ?>> it = getIndices(
				new HashMap<ByteArrayId, DataStatistics<SimpleFeature>>(),
				new BasicQuery(
						createConstraints(
								HOUSE,
								HOUSE,
								DAY)),
				strategy);
		assertTrue(it.hasNext());
		assertEquals(
				indices.get(
						1).getId(),
				it.next().getId());
		assertFalse(it.hasNext());

	}

	@Test
	public void testChooseSpatialWithoutStatsHouseWeek() {
		final ChooseHeuristicMatchIndexQueryStrategy strategy = new ChooseHeuristicMatchIndexQueryStrategy();

		final Iterator<Index<?, ?>> it = getIndices(
				new HashMap<ByteArrayId, DataStatistics<SimpleFeature>>(),
				new BasicQuery(
						createConstraints(
								HOUSE,
								HOUSE,
								WEEK)),
				strategy);
		assertTrue(it.hasNext());
		assertEquals(
				indices.get(
						1).getId(),
				it.next().getId());
		assertFalse(it.hasNext());

	}

	@Test
	public void testChooseTemporalWithoutStatsBlockHour() {
		final ChooseHeuristicMatchIndexQueryStrategy strategy = new ChooseHeuristicMatchIndexQueryStrategy();

		final Iterator<Index<?, ?>> it = getIndices(
				new HashMap<ByteArrayId, DataStatistics<SimpleFeature>>(),
				new BasicQuery(
						createConstraints(
								BLOCK,
								BLOCK,
								HOUR)),
				strategy);
		assertTrue(it.hasNext());
		assertEquals(
				indices.get(
						1).getId(),
				it.next().getId());
		assertFalse(it.hasNext());

	}

	@Test
	public void testChooseSpatialWithoutStatsBlockDay() {
		final ChooseHeuristicMatchIndexQueryStrategy strategy = new ChooseHeuristicMatchIndexQueryStrategy();

		final Iterator<Index<?, ?>> it = getIndices(
				new HashMap<ByteArrayId, DataStatistics<SimpleFeature>>(),
				new BasicQuery(
						createConstraints(
								BLOCK,
								BLOCK,
								DAY)),
				strategy);
		assertTrue(it.hasNext());
		assertEquals(
				indices.get(
						1).getId(),
				it.next().getId());
		assertFalse(it.hasNext());

	}

	@Test
	public void testChooseSpatialWithoutStatsBlockWeek() {
		final ChooseHeuristicMatchIndexQueryStrategy strategy = new ChooseHeuristicMatchIndexQueryStrategy();

		final Iterator<Index<?, ?>> it = getIndices(
				new HashMap<ByteArrayId, DataStatistics<SimpleFeature>>(),
				new BasicQuery(
						createConstraints(
								BLOCK,
								BLOCK,
								WEEK)),
				strategy);
		assertTrue(it.hasNext());
		assertEquals(
				indices.get(
						1).getId(),
				it.next().getId());
		assertFalse(it.hasNext());

	}

	@Test
	public void testChooseTemporalWithoutStatsCityHour() {
		final ChooseHeuristicMatchIndexQueryStrategy strategy = new ChooseHeuristicMatchIndexQueryStrategy();

		final Iterator<Index<?, ?>> it = getIndices(
				new HashMap<ByteArrayId, DataStatistics<SimpleFeature>>(),
				new BasicQuery(
						createConstraints(
								CITY,
								CITY,
								HOUR)),
				strategy);
		assertTrue(it.hasNext());
		assertEquals(
				indices.get(
						1).getId(),
				it.next().getId());
		assertFalse(it.hasNext());

	}

	@Test
	public void testChooseTemporalWithoutStatsCityDay() {
		final ChooseHeuristicMatchIndexQueryStrategy strategy = new ChooseHeuristicMatchIndexQueryStrategy();

		final Iterator<Index<?, ?>> it = getIndices(
				new HashMap<ByteArrayId, DataStatistics<SimpleFeature>>(),
				new BasicQuery(
						createConstraints(
								CITY,
								CITY,
								DAY)),
				strategy);
		assertTrue(it.hasNext());
		assertEquals(
				indices.get(
						1).getId(),
				it.next().getId());
		assertFalse(it.hasNext());

	}

	@Test
	public void testChooseSpatialWithoutStatsCityWeek() {
		final ChooseHeuristicMatchIndexQueryStrategy strategy = new ChooseHeuristicMatchIndexQueryStrategy();

		final Iterator<Index<?, ?>> it = getIndices(
				new HashMap<ByteArrayId, DataStatistics<SimpleFeature>>(),
				new BasicQuery(
						createConstraints(
								CITY,
								CITY,
								WEEK)),
				strategy);
		assertTrue(it.hasNext());
		assertEquals(
				indices.get(
						1).getId(),
				it.next().getId());
		assertFalse(it.hasNext());

	}

	public Iterator<Index<?, ?>> getIndices(
			final Map<ByteArrayId, DataStatistics<SimpleFeature>> stats,
			final BasicQuery query,
			final ChooseHeuristicMatchIndexQueryStrategy strategy ) {
		return strategy.getIndices(
				stats,
				query,
				indices.toArray(new PrimaryIndex[indices.size()]));
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

	private Constraints createConstraints(
			final double lat,
			final double lon,
			final double time ) {
		final ConstraintSet cs1 = new ConstraintSet();
		cs1.addConstraint(
				LatitudeDefinition.class,
				new ConstraintData(
						new ConstrainedIndexValue(
								0,
								lat),
						true));

		cs1.addConstraint(
				LongitudeDefinition.class,
				new ConstraintData(
						new ConstrainedIndexValue(
								0,
								lon),
						true));

		final ConstraintSet cs2a = new ConstraintSet();
		cs2a.addConstraint(
				TimeDefinition.class,
				new ConstraintData(
						new ConstrainedIndexValue(
								0,
								time),
						true));

		return new Constraints(
				Arrays.asList(cs2a)).merge(Collections.singletonList(cs1));
	}
}
