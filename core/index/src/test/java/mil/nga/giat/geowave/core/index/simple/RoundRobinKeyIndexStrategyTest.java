package mil.nga.giat.geowave.core.index.simple;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.CompoundIndexStrategy;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinates;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.SinglePartitionInsertionIds;
import mil.nga.giat.geowave.core.index.dimension.BasicDimensionDefinition;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;

public class RoundRobinKeyIndexStrategyTest
{

	private static final NumericDimensionDefinition[] SPATIAL_DIMENSIONS = new NumericDimensionDefinition[] {
		new BasicDimensionDefinition(
				-180,
				180),
		new BasicDimensionDefinition(
				-90,
				90)
	};

	private static final NumericIndexStrategy sfcIndexStrategy = TieredSFCIndexFactory.createSingleTierStrategy(
			SPATIAL_DIMENSIONS,
			new int[] {
				16,
				16
			},
			SFCType.HILBERT);

	private static final CompoundIndexStrategy compoundIndexStrategy = new CompoundIndexStrategy(
			new RoundRobinKeyIndexStrategy(),
			sfcIndexStrategy);

	private static final NumericRange dimension1Range = new NumericRange(
			50.0,
			50.025);
	private static final NumericRange dimension2Range = new NumericRange(
			-20.5,
			-20.455);
	private static final MultiDimensionalNumericData sfcIndexedRange = new BasicNumericDataset(
			new NumericData[] {
				dimension1Range,
				dimension2Range
			});

	@Test
	public void testBinaryEncoding() {
		final byte[] bytes = PersistenceUtils.toBinary(compoundIndexStrategy);
		final CompoundIndexStrategy deserializedStrategy = PersistenceUtils.fromBinary(
				bytes,
				CompoundIndexStrategy.class);
		final byte[] bytes2 = PersistenceUtils.toBinary(deserializedStrategy);
		Assert.assertArrayEquals(
				bytes,
				bytes2);
	}

	@Test
	public void testGetNumberOfDimensions() {
		final int numDimensions = compoundIndexStrategy.getNumberOfDimensions();
		Assert.assertEquals(
				2,
				numDimensions);
	}

	@Test
	public void testGetQueryRangesWithMaximumNumberOfRanges() {
		final List<ByteArrayRange> sfcIndexRanges = sfcIndexStrategy.getQueryRanges(
				sfcIndexedRange).getCompositeQueryRanges();
		final List<ByteArrayRange> ranges = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			for (final ByteArrayRange r2 : sfcIndexRanges) {
				final ByteArrayId start = new ByteArrayId(
						ByteArrayUtils.combineArrays(
								new byte[] {
									(byte) i
								},
								r2.getStart().getBytes()));
				final ByteArrayId end = new ByteArrayId(
						ByteArrayUtils.combineArrays(
								new byte[] {
									(byte) i
								},
								r2.getEnd().getBytes()));
				ranges.add(new ByteArrayRange(
						start,
						end));
			}
		}
		final Set<ByteArrayRange> testRanges = new HashSet<>(
				ranges);
		final Set<ByteArrayRange> compoundIndexRanges = new HashSet<>(
				compoundIndexStrategy.getQueryRanges(
						sfcIndexedRange).getCompositeQueryRanges());
		Assert.assertTrue(testRanges.containsAll(compoundIndexRanges));
		Assert.assertTrue(compoundIndexRanges.containsAll(testRanges));
	}

	@Test
	public void testUniformityAndLargeKeySet() {
		final RoundRobinKeyIndexStrategy strategy = new RoundRobinKeyIndexStrategy(
				512);
		final Map<ByteArrayId, Integer> countMap = new HashMap<ByteArrayId, Integer>();
		for (int i = 0; i < 2048; i++) {
			final Set<ByteArrayId> ids = strategy.getInsertionPartitionKeys(sfcIndexedRange);
			assertEquals(
					1,
					ids.size());
			final ByteArrayId key = ids.iterator().next();
			if (countMap.containsKey(key)) {
				countMap.put(
						key,
						countMap.get(key) + 1);
			}
			else {
				countMap.put(
						key,
						1);
			}

		}
		for (final Integer i : countMap.values()) {
			assertEquals(
					4,
					i.intValue());
		}
	}

	@Test
	public void testGetInsertionIds() {
		final List<ByteArrayId> ids = new ArrayList<>();

		final InsertionIds ids2 = sfcIndexStrategy.getInsertionIds(
				sfcIndexedRange,
				1);
		final List<ByteArrayId> compositeIds = ids2.getCompositeInsertionIds();
		for (int i = 0; i < 3; i++) {
			for (final ByteArrayId id2 : compositeIds) {
				ids.add(new ByteArrayId(
						ByteArrayUtils.combineArrays(
								new byte[] {
									(byte) i
								},
								id2.getBytes())));
			}
		}
		final Set<ByteArrayId> testIds = new HashSet<>(
				ids);
		final Set<ByteArrayId> compoundIndexIds = new HashSet<>(
				compoundIndexStrategy.getInsertionIds(
						sfcIndexedRange,
						8).getCompositeInsertionIds());
		Assert.assertTrue(testIds.containsAll(compoundIndexIds));
		final SinglePartitionInsertionIds id2 = ids2.getPartitionKeys().iterator().next();
		final MultiDimensionalCoordinates sfcIndexCoordinatesPerDim = sfcIndexStrategy.getCoordinatesPerDimension(
				id2.getPartitionKey(),
				id2.getSortKeys().get(
						0));
		// the first 2 bytes are the partition keys
		final MultiDimensionalCoordinates coordinatesPerDim = compoundIndexStrategy.getCoordinatesPerDimension(
				new ByteArrayId(
						Arrays.copyOfRange(
								ids.get(
										0).getBytes(),
								0,
								2)),
				new ByteArrayId(
						Arrays.copyOfRange(
								ids.get(
										0).getBytes(),
								2,
								ids.get(
										0).getBytes().length)));

		Assert.assertTrue(sfcIndexCoordinatesPerDim.equals(coordinatesPerDim));
	}

}
