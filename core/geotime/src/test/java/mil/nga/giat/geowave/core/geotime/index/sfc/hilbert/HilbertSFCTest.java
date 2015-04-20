package mil.nga.giat.geowave.core.geotime.index.sfc.hilbert;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.index.sfc.RangeDecomposition;
import mil.nga.giat.geowave.core.index.sfc.SFCDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory;
import mil.nga.giat.geowave.core.index.sfc.SpaceFillingCurve;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.primitives.SignedBytes;

public class HilbertSFCTest
{

	@Test
	public void testGetId_2DSpatialMaxValue()
			throws Exception {

		int LATITUDE_BITS = 31;
		int LONGITUDE_BITS = 31;

		double[] testValues = new double[] {
			90,
			180
		};
		long expectedID = 3074457345618258602L;

		SFCDimensionDefinition[] SPATIAL_DIMENSIONS = new SFCDimensionDefinition[] {
			new SFCDimensionDefinition(
					new LatitudeDefinition(),
					LATITUDE_BITS),
			new SFCDimensionDefinition(
					new LongitudeDefinition(),
					LONGITUDE_BITS)
		};

		SpaceFillingCurve hilbertSFC = SFCFactory.createSpaceFillingCurve(
				SPATIAL_DIMENSIONS,
				SFCType.HILBERT);
		Assert.assertEquals(
				expectedID,
				ByteBuffer.wrap(
						hilbertSFC.getId(testValues)).getLong());

	}

	@Test
	public void testGetId_2DSpatialMinValue()
			throws Exception {

		int LATITUDE_BITS = 31;
		int LONGITUDE_BITS = 31;

		double[] testValues = new double[] {
			-90,
			-180
		};
		long expectedID = 0L;

		SFCDimensionDefinition[] SPATIAL_DIMENSIONS = new SFCDimensionDefinition[] {
			new SFCDimensionDefinition(
					new LatitudeDefinition(),
					LATITUDE_BITS),
			new SFCDimensionDefinition(
					new LongitudeDefinition(),
					LONGITUDE_BITS)
		};

		SpaceFillingCurve hilbertSFC = SFCFactory.createSpaceFillingCurve(
				SPATIAL_DIMENSIONS,
				SFCType.HILBERT);

		Assert.assertEquals(
				expectedID,
				ByteBuffer.wrap(
						hilbertSFC.getId(testValues)).getLong());

	}

	@Test
	public void testGetId_2DSpatialCentroidValue()
			throws Exception {

		int LATITUDE_BITS = 31;
		int LONGITUDE_BITS = 31;

		double[] testValues = new double[] {
			0,
			0
		};
		long expectedID = 768614336404564650L;

		SFCDimensionDefinition[] SPATIAL_DIMENSIONS = new SFCDimensionDefinition[] {
			new SFCDimensionDefinition(
					new LatitudeDefinition(),
					LATITUDE_BITS),
			new SFCDimensionDefinition(
					new LongitudeDefinition(),
					LONGITUDE_BITS)
		};

		SpaceFillingCurve hilbertSFC = SFCFactory.createSpaceFillingCurve(
				SPATIAL_DIMENSIONS,
				SFCType.HILBERT);
		Assert.assertEquals(
				expectedID,
				ByteBuffer.wrap(
						hilbertSFC.getId(testValues)).getLong());

	}

	@Test
	public void testGetId_2DSpatialLexicographicOrdering()
			throws Exception {

		int LATITUDE_BITS = 31;
		int LONGITUDE_BITS = 31;

		double[] minValue = new double[] {
			-90,
			-180
		};
		double[] maxValue = new double[] {
			90,
			180
		};

		SFCDimensionDefinition[] SPATIAL_DIMENSIONS = new SFCDimensionDefinition[] {
			new SFCDimensionDefinition(
					new LatitudeDefinition(),
					LATITUDE_BITS),
			new SFCDimensionDefinition(
					new LongitudeDefinition(),
					LONGITUDE_BITS)
		};

		SpaceFillingCurve hilbertSFC = SFCFactory.createSpaceFillingCurve(
				SPATIAL_DIMENSIONS,
				SFCType.HILBERT);

		Assert.assertTrue(SignedBytes.lexicographicalComparator().compare(
				hilbertSFC.getId(minValue),
				hilbertSFC.getId(maxValue)) < 0);

	}

	// @Test(expected = IllegalArgumentException.class)
	public void testGetId_2DSpatialIllegalArgument()
			throws Exception {

		int LATITUDE_BITS = 31;
		int LONGITUDE_BITS = 31;

		double[] testValues = new double[] {
			-100,
			-180
		};
		long expectedID = 0L;

		SFCDimensionDefinition[] SPATIAL_DIMENSIONS = new SFCDimensionDefinition[] {
			new SFCDimensionDefinition(
					new LatitudeDefinition(),
					LATITUDE_BITS),
			new SFCDimensionDefinition(
					new LongitudeDefinition(),
					LONGITUDE_BITS)
		};

		SpaceFillingCurve hilbertSFC = SFCFactory.createSpaceFillingCurve(
				SPATIAL_DIMENSIONS,
				SFCType.HILBERT);

		Assert.assertEquals(
				expectedID,
				ByteBuffer.wrap(
						hilbertSFC.getId(testValues)).getLong());

	}

	@Test
	public void testDecomposeQuery_2DSpatialOneIndexFilter() {

		int LATITUDE_BITS = 31;
		int LONGITUDE_BITS = 31;

		SFCDimensionDefinition[] SPATIAL_DIMENSIONS = new SFCDimensionDefinition[] {
			new SFCDimensionDefinition(
					new LongitudeDefinition(),
					LONGITUDE_BITS),
			new SFCDimensionDefinition(
					new LatitudeDefinition(),
					LATITUDE_BITS)
		};

		SpaceFillingCurve hilbertSFC = SFCFactory.createSpaceFillingCurve(
				SPATIAL_DIMENSIONS,
				SFCType.HILBERT);

		// Create a IndexRange object using the x axis
		final NumericRange rangeX = new NumericRange(
				55,
				57);

		// Create a IndexRange object using the y axis
		final NumericRange rangeY = new NumericRange(
				25,
				27);
		BasicNumericDataset spatialQuery = new BasicNumericDataset(
				new NumericData[] {
					rangeX,
					rangeY
				});

		RangeDecomposition rangeDecomposition = hilbertSFC.decomposeQuery(
				spatialQuery,
				1);

		Assert.assertEquals(
				1,
				rangeDecomposition.getRanges().length);

	}

	@Test
	public void testDecomposeQuery_2DSpatialTwentyIndexFilters() {

		int LATITUDE_BITS = 31;
		int LONGITUDE_BITS = 31;

		SFCDimensionDefinition[] SPATIAL_DIMENSIONS = new SFCDimensionDefinition[] {
			new SFCDimensionDefinition(
					new LongitudeDefinition(),
					LONGITUDE_BITS),
			new SFCDimensionDefinition(
					new LatitudeDefinition(),
					LATITUDE_BITS)
		};

		SpaceFillingCurve hilbertSFC = SFCFactory.createSpaceFillingCurve(
				SPATIAL_DIMENSIONS,
				SFCType.HILBERT);
		// Create a IndexRange object using the x axis
		final NumericRange rangeX = new NumericRange(
				10,
				57);

		// Create a IndexRange object using the y axis
		final NumericRange rangeY = new NumericRange(
				25,
				50);
		BasicNumericDataset spatialQuery = new BasicNumericDataset(
				new NumericData[] {
					rangeX,
					rangeY
				});

		RangeDecomposition rangeDecomposition = hilbertSFC.decomposeQuery(
				spatialQuery,
				20);

		Assert.assertEquals(
				20,
				rangeDecomposition.getRanges().length);

	}

	/* public void testDecomposeQuery_2DSpatialRanges() {} */
}
