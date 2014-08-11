package mil.nga.giat.geowave.index.sfc.hilbert.tiered;

import static org.junit.Assert.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.NumericIndexStrategy;
import mil.nga.giat.geowave.index.NumericIndexStrategy.SpatialTemporalFactory;
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

public class TieredSFCIndexStrategyTest {

	NumericDimensionDefinition[] SPATIAL_TEMPORAL_DIMENSIONS = new NumericDimensionDefinition[] {
			new LongitudeDefinition(), new LatitudeDefinition(),
			new TimeDefinition(Unit.YEAR), };

	@Test
	public void testSingleEntry() {
		Calendar cal = GregorianCalendar.getInstance();
		NumericData[] dataPerDimension1 = new NumericData[SPATIAL_TEMPORAL_DIMENSIONS.length];
		dataPerDimension1[0] = new NumericValue(45);
		dataPerDimension1[1] = new NumericValue(45);
		dataPerDimension1[2] = new NumericValue(cal.getTimeInMillis());

		int year = cal.get(Calendar.YEAR);

		cal.set(Calendar.DAY_OF_YEAR, 1);
		NumericData[] dataPerDimension2 = new NumericData[SPATIAL_TEMPORAL_DIMENSIONS.length];
		dataPerDimension2[0] = new NumericValue(45);
		dataPerDimension2[1] = new NumericValue(45);
		dataPerDimension2[2] = new NumericValue(cal.getTimeInMillis());

		cal.set(Calendar.YEAR, year - 1);
		NumericData[] dataPerDimension3 = new NumericData[SPATIAL_TEMPORAL_DIMENSIONS.length];
		dataPerDimension3[0] = new NumericValue(45);
		dataPerDimension3[1] = new NumericValue(45);
		dataPerDimension3[2] = new NumericValue(cal.getTimeInMillis());

		MultiDimensionalNumericData indexedData = new BasicNumericDataset(
				dataPerDimension1);
		NumericIndexStrategy strategy = SpatialTemporalFactory
				.createIndexStrategy();

		List<ByteArrayId> ids1 = strategy.getInsertionIds(indexedData);
		assertEquals(1, ids1.size());
		assertEquals(13, ids1.get(0).getBytes().length);

		// same bin
		indexedData = new BasicNumericDataset(dataPerDimension2);
		List<ByteArrayId> ids2 = strategy.getInsertionIds(indexedData);
		assertEquals(1, ids2.size());
		assertTrue(compare(ids1.get(0).getBytes(), ids2.get(0).getBytes(),5));


		// different bin
		indexedData = new BasicNumericDataset(dataPerDimension3);
		List<ByteArrayId> ids3 = strategy.getInsertionIds(indexedData);
		assertEquals(1, ids3.size());
		assertFalse(compare(ids1.get(0).getBytes(), ids3.get(0)
				.getBytes(),5));
	}
	
	@Test
	public void testRegions() throws ParseException {
		Calendar cal = GregorianCalendar.getInstance();
		Calendar calEnd = GregorianCalendar.getInstance();
		SimpleDateFormat format = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss");
		cal.setTime( format.parse("03-03-1999 11:01:01"));
		calEnd.setTime( format.parse("03-03-1999 11:05:01"));
		
		NumericData[] dataPerDimension1 = new NumericData[SPATIAL_TEMPORAL_DIMENSIONS.length];
		dataPerDimension1[0] = new NumericRange(45.170,45.173);
		dataPerDimension1[1] = new NumericRange(50.190,50.192);
		dataPerDimension1[2] = new NumericRange(calEnd.getTimeInMillis(),cal.getTimeInMillis());

		int year = cal.get(Calendar.YEAR);

		cal.set(Calendar.DAY_OF_YEAR, 1);
		NumericData[] dataPerDimension2 = new NumericData[SPATIAL_TEMPORAL_DIMENSIONS.length];
		dataPerDimension2[0] = new NumericRange(45,50);
		dataPerDimension2[1] = new NumericRange(45,50);
		dataPerDimension2[2] = new NumericRange(calEnd.getTimeInMillis(),cal.getTimeInMillis());

		cal.set(Calendar.YEAR, year - 1);
		NumericData[] dataPerDimension3 = new NumericData[SPATIAL_TEMPORAL_DIMENSIONS.length];
		dataPerDimension3[0] = new NumericRange(45.1701,45.1703);
		dataPerDimension3[1] = new NumericRange(50.1901,50.1902);
		dataPerDimension3[2] = new NumericRange(calEnd.getTimeInMillis(),cal.getTimeInMillis());


		MultiDimensionalNumericData indexedData = new BasicNumericDataset(
				dataPerDimension1);
		NumericIndexStrategy strategy = TieredSFCIndexFactory.createEqualIntervalPrecisionTieredStrategy(SPATIAL_TEMPORAL_DIMENSIONS,new int[] {
					20,
					20,
					20
				},
				SFCType.HILBERT,
				4);

		List<ByteArrayId> ids1 = strategy.getInsertionIds(indexedData);
		assertEquals(1, ids1.size());
		assertEquals(13, ids1.get(0).getBytes().length);

		// same bin
		indexedData = new BasicNumericDataset(dataPerDimension2);
		List<ByteArrayId> ids2 = strategy.getInsertionIds(indexedData);
		assertEquals(1, ids2.size());
		assertTrue(compare(ids1.get(0).getBytes(), ids2.get(0).getBytes(),5));


		// different bin
		indexedData = new BasicNumericDataset(dataPerDimension3);
		List<ByteArrayId> ids3 = strategy.getInsertionIds(indexedData);
		assertEquals(1, ids3.size());
		assertFalse(compare(ids1.get(0).getBytes(), ids3.get(0)
				.getBytes(),5));
	}
	
	private boolean compare(byte[] one, byte[] two, int length) {
	   return Arrays.equals(Arrays.copyOf(one, length), Arrays.copyOf(two, length));	
	}

}
