package mil.nga.giat.geowave.core.geotime.index.sfc.hilbert.tiered;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import mil.nga.giat.geowave.core.geotime.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.index.sfc.data.NumericValue;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import mil.nga.giat.geowave.core.store.index.BasicIndexModel;
import mil.nga.giat.geowave.core.store.index.CustomIdIndex;

public class TieredSFCIndexStrategyTest
{
	public static final int[] DEFINED_BITS_OF_PRECISION = new int[] {
		0,
		1,
		2,
		3,
		4,
		5,
		6,
		7,
		8,
		9,
		10,
		11,
		13,
		18,
		31
	};
	NumericDimensionDefinition[] SPATIAL_TEMPORAL_DIMENSIONS = new NumericDimensionDefinition[] {
		new LongitudeDefinition(),
		new LatitudeDefinition(
				true),
		new TimeDefinition(
				Unit.YEAR),
	};
	private static final double QUERY_RANGE_EPSILON = 1E-12;

	@Test
	public void testSingleEntry() {
		final Calendar cal = Calendar.getInstance();
		final NumericData[] dataPerDimension1 = new NumericData[SPATIAL_TEMPORAL_DIMENSIONS.length];
		dataPerDimension1[0] = new NumericValue(
				45);
		dataPerDimension1[1] = new NumericValue(
				45);
		dataPerDimension1[2] = new NumericValue(
				cal.getTimeInMillis());

		final int year = cal.get(Calendar.YEAR);

		cal.set(
				Calendar.DAY_OF_YEAR,
				1);
		final NumericData[] dataPerDimension2 = new NumericData[SPATIAL_TEMPORAL_DIMENSIONS.length];
		dataPerDimension2[0] = new NumericValue(
				45);
		dataPerDimension2[1] = new NumericValue(
				45);
		dataPerDimension2[2] = new NumericValue(
				cal.getTimeInMillis());

		cal.set(
				Calendar.YEAR,
				year - 1);
		final NumericData[] dataPerDimension3 = new NumericData[SPATIAL_TEMPORAL_DIMENSIONS.length];
		dataPerDimension3[0] = new NumericValue(
				45);
		dataPerDimension3[1] = new NumericValue(
				45);
		dataPerDimension3[2] = new NumericValue(
				cal.getTimeInMillis());

		MultiDimensionalNumericData indexedData = new BasicNumericDataset(
				dataPerDimension1);
		final NumericIndexStrategy strategy = new SpatialTemporalDimensionalityTypeProvider()
				.createPrimaryIndex()
				.getIndexStrategy();

		final List<ByteArrayId> ids1 = strategy.getInsertionIds(indexedData);
		assertEquals(
				1,
				ids1.size());
		assertEquals(
				13,
				ids1.get(
						0).getBytes().length);

		// same bin
		indexedData = new BasicNumericDataset(
				dataPerDimension2);
		final List<ByteArrayId> ids2 = strategy.getInsertionIds(indexedData);
		assertEquals(
				1,
				ids2.size());
		assertTrue(compare(
				ids1.get(
						0).getBytes(),
				ids2.get(
						0).getBytes(),
				5));

		// different bin
		indexedData = new BasicNumericDataset(
				dataPerDimension3);
		final List<ByteArrayId> ids3 = strategy.getInsertionIds(indexedData);
		assertEquals(
				1,
				ids3.size());
		assertFalse(compare(
				ids1.get(
						0).getBytes(),
				ids3.get(
						0).getBytes(),
				5));
	}

	@Test
	public void testPredefinedSpatialEntries()
			throws Exception {
		final NumericIndexStrategy strategy = TieredSFCIndexFactory.createDefinedPrecisionTieredStrategy(
				new NumericDimensionDefinition[] {
					new LongitudeDefinition(),
					new LatitudeDefinition(
							true)
				},
				new int[][] {
					DEFINED_BITS_OF_PRECISION.clone(),
					DEFINED_BITS_OF_PRECISION.clone()
				},
				SFCType.HILBERT);
		for (int sfcIndex = 0; sfcIndex < DEFINED_BITS_OF_PRECISION.length; sfcIndex++) {
			final NumericData[] dataPerDimension = new NumericData[2];
			final double precision = 360 / Math.pow(
					2,
					DEFINED_BITS_OF_PRECISION[sfcIndex]);
			if (precision > 180) {
				dataPerDimension[0] = new NumericRange(
						-180,
						180);
				dataPerDimension[1] = new NumericRange(
						-90,
						90);
			}
			else {
				dataPerDimension[0] = new NumericRange(
						0,
						precision);

				dataPerDimension[1] = new NumericRange(
						-precision,
						0);
			}
			final MultiDimensionalNumericData indexedData = new BasicNumericDataset(
					dataPerDimension);
			final List<ByteArrayId> ids = strategy.getInsertionIds(indexedData);
			final NumericData[] queryRangePerDimension = new NumericData[2];
			queryRangePerDimension[0] = new NumericRange(
					dataPerDimension[0].getMin() + QUERY_RANGE_EPSILON,
					dataPerDimension[0].getMax() - QUERY_RANGE_EPSILON);
			queryRangePerDimension[1] = new NumericRange(
					dataPerDimension[1].getMin() + QUERY_RANGE_EPSILON,
					dataPerDimension[1].getMax() - QUERY_RANGE_EPSILON);
			final MultiDimensionalNumericData queryData = new BasicNumericDataset(
					queryRangePerDimension);
			final List<ByteArrayRange> queryRanges = strategy.getQueryRanges(queryData);
			final Set<Byte> queryRangeTiers = new HashSet<Byte>();
			boolean rangeAtTierFound = false;
			for (final ByteArrayRange range : queryRanges) {
				final byte tier = range.getStart().getBytes()[0];
				queryRangeTiers.add(range.getStart().getBytes()[0]);
				if (tier == DEFINED_BITS_OF_PRECISION[sfcIndex]) {
					if (rangeAtTierFound) {
						throw new Exception(
								"multiple ranges were found unexpectedly for tier " + tier);
					}
					assertEquals(
							"this range is an exact fit, so it should have exactly one value for tier "
									+ DEFINED_BITS_OF_PRECISION[sfcIndex],
							range.getStart(),
							range.getEnd());
					rangeAtTierFound = true;
				}
			}
			if (!rangeAtTierFound) {
				throw new Exception(
						"no ranges were found at the expected exact fit tier " + DEFINED_BITS_OF_PRECISION[sfcIndex]);
			}

			// ensure the first byte is equal to the appropriate number of bits
			// of precision
			if ((ids.get(
					0).getBytes()[0] == 0)
					|| ((sfcIndex == (DEFINED_BITS_OF_PRECISION.length - 1)) || (DEFINED_BITS_OF_PRECISION[sfcIndex + 1] != (DEFINED_BITS_OF_PRECISION[sfcIndex] + 1)))) {
				assertEquals(
						"Insertion ID expected to be exact match at tier " + DEFINED_BITS_OF_PRECISION[sfcIndex],
						DEFINED_BITS_OF_PRECISION[sfcIndex],
						ids.get(
								0).getBytes()[0]);
				assertEquals(
						"Insertion ID size expected to be 1 at tier " + DEFINED_BITS_OF_PRECISION[sfcIndex],
						1,
						ids.size());
			}
			else {
				assertEquals(
						"Insertion ID expected to be duplicated at tier " + DEFINED_BITS_OF_PRECISION[sfcIndex + 1],
						DEFINED_BITS_OF_PRECISION[sfcIndex + 1],
						ids.get(
								0).getBytes()[0]);
				// if the precision is within the bounds of longitude but not
				// within latitude we will end up with 2 (rectangular
				// decomposition)
				// otherwise we will get a square decomposition of 4 ids
				final int expectedIds = (precision > 90) && (precision <= 180) ? 2 : 4;
				assertEquals(
						"Insertion ID size expected to be " + expectedIds + " at tier "
								+ DEFINED_BITS_OF_PRECISION[sfcIndex + 1],
						expectedIds,
						ids.size());
			}
		}
	}

	@Test
	public void testOneEstimatedDuplicateInsertion()
			throws Exception {

		final NumericIndexStrategy strategy = TieredSFCIndexFactory.createFullIncrementalTieredStrategy(
				new NumericDimensionDefinition[] {
					new LongitudeDefinition(),
					new LatitudeDefinition(
							true)
				},
				new int[] {
					31,
					31
				},
				SFCType.HILBERT);

		for (final int element : DEFINED_BITS_OF_PRECISION) {
			final NumericData[] dataPerDimension = new NumericData[2];
			final double precision = 360 / Math.pow(
					2,
					element);
			if (precision > 180) {
				dataPerDimension[0] = new NumericRange(
						-180,
						180);
				dataPerDimension[1] = new NumericRange(
						-90,
						90);
			}
			else {
				dataPerDimension[0] = new NumericRange(
						0,
						precision);

				dataPerDimension[1] = new NumericRange(
						-precision,
						0);
			}
			final MultiDimensionalNumericData indexedData = new BasicNumericDataset(
					dataPerDimension);
			final List<ByteArrayId> ids = strategy.getInsertionIds(
					indexedData,
					1);
			assertEquals(
					"Insertion ID size expected to be 1 at tier " + element,
					1,
					ids.size());
			// ensure the first byte is equal to the appropriate number of bits
			// of precision
			assertEquals(
					"Insertion ID expected to be exact match at tier " + element,
					element,
					ids.get(
							0).getBytes()[0]);

		}
	}

	@Test
	public void testRegions()
			throws ParseException {
		final Calendar cal = Calendar.getInstance();
		final Calendar calEnd = Calendar.getInstance();
		final SimpleDateFormat format = new SimpleDateFormat(
				"MM-dd-yyyy HH:mm:ss");
		cal.setTime(format.parse("03-03-1999 11:01:01"));
		calEnd.setTime(format.parse("03-03-1999 11:05:01"));

		final NumericData[] dataPerDimension1 = new NumericData[SPATIAL_TEMPORAL_DIMENSIONS.length];
		dataPerDimension1[0] = new NumericRange(
				45.170,
				45.173);
		dataPerDimension1[1] = new NumericRange(
				50.190,
				50.192);
		dataPerDimension1[2] = new NumericRange(
				cal.getTimeInMillis(),
				calEnd.getTimeInMillis());

		final int year = cal.get(Calendar.YEAR);

		cal.set(
				Calendar.DAY_OF_YEAR,
				1);
		final NumericData[] dataPerDimension2 = new NumericData[SPATIAL_TEMPORAL_DIMENSIONS.length];
		dataPerDimension2[0] = new NumericRange(
				45,
				50);
		dataPerDimension2[1] = new NumericRange(
				45,
				50);
		dataPerDimension2[2] = new NumericRange(
				cal.getTimeInMillis(),
				calEnd.getTimeInMillis());

		cal.set(
				Calendar.YEAR,
				year - 1);
		calEnd.set(
				Calendar.YEAR,
				year - 1);
		final NumericData[] dataPerDimension3 = new NumericData[SPATIAL_TEMPORAL_DIMENSIONS.length];
		dataPerDimension3[0] = new NumericRange(
				45.1701,
				45.1703);
		dataPerDimension3[1] = new NumericRange(
				50.1901,
				50.1902);
		dataPerDimension3[2] = new NumericRange(
				cal.getTimeInMillis(),
				calEnd.getTimeInMillis());

		MultiDimensionalNumericData indexedData = new BasicNumericDataset(
				dataPerDimension1);
		final NumericIndexStrategy strategy = TieredSFCIndexFactory.createEqualIntervalPrecisionTieredStrategy(
				SPATIAL_TEMPORAL_DIMENSIONS,
				new int[] {
					20,
					20,
					20
				},
				SFCType.HILBERT,
				4);

		final List<ByteArrayId> ids1 = strategy.getInsertionIds(indexedData);
		assertEquals(
				1,
				ids1.size());
		assertEquals(
				10,
				ids1.get(
						0).getBytes().length);

		// different bin bin
		indexedData = new BasicNumericDataset(
				dataPerDimension2);
		final List<ByteArrayId> ids2 = strategy.getInsertionIds(indexedData);
		assertEquals(
				1,
				ids2.size());
		// different tier
		assertFalse(compare(
				ids1.get(
						0).getBytes(),
				ids2.get(
						0).getBytes(),
				1));
		// same time
		assertTrue(compare(
				ids1.get(
						0).getBytes(),
				ids2.get(
						0).getBytes(),
				1,
				5));

		// different bin
		indexedData = new BasicNumericDataset(
				dataPerDimension3);
		final List<ByteArrayId> ids3 = strategy.getInsertionIds(indexedData);
		assertEquals(
				1,
				ids3.size());
		assertFalse(compare(
				ids1.get(
						0).getBytes(),
				ids3.get(
						0).getBytes(),
				1,
				5));
	}

	private boolean compare(
			final byte[] one,
			final byte[] two,
			final int start,
			final int stop ) {
		return Arrays.equals(
				Arrays.copyOfRange(
						one,
						start,
						stop),
				Arrays.copyOfRange(
						two,
						start,
						stop));
	}

	private boolean compare(
			final byte[] one,
			final byte[] two,
			final int length ) {
		return Arrays.equals(
				Arrays.copyOf(
						one,
						length),
				Arrays.copyOf(
						two,
						length));
	}

}
