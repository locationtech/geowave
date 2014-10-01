package mil.nga.giat.geowave.index.sfc.hilbert.tiered;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.NumericIndexStrategy;
import mil.nga.giat.geowave.index.NumericIndexStrategyFactory.DataType;
import mil.nga.giat.geowave.index.NumericIndexStrategyFactory.SpatialTemporalFactory;
import mil.nga.giat.geowave.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.index.dimension.bin.TemporalBinningStrategy.Unit;
import mil.nga.giat.geowave.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.index.sfc.data.NumericData;
import mil.nga.giat.geowave.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.index.sfc.data.NumericValue;
import mil.nga.giat.geowave.index.sfc.tiered.TieredSFCIndexFactory;

import org.junit.Test;

public class TieredSFCIndexStrategyTest
{

	NumericDimensionDefinition[] SPATIAL_TEMPORAL_DIMENSIONS = new NumericDimensionDefinition[] {
		new LongitudeDefinition(),
		new LatitudeDefinition(),
		new TimeDefinition(
				Unit.YEAR),
	};

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
		final NumericIndexStrategy strategy = new SpatialTemporalFactory().createIndexStrategy(DataType.VECTOR);

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
