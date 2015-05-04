package mil.nga.giat.geowave.core.index;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

/**
 * This is a completely empty numeric index strategy representing no dimensions,
 * and always returning empty IDs and ranges. It can be used in cases when the
 * data is "indexed" by another means, and not using multi-dimensional numeric
 * data.
 * 
 */
public class NullNumericIndexStrategy implements
		NumericIndexStrategy
{
	private String id;

	protected NullNumericIndexStrategy() {
		super();
	}

	public NullNumericIndexStrategy(
			final String id ) {
		this.id = id;
	}

	@Override
	public byte[] toBinary() {
		return StringUtils.stringToBinary(id);
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		id = StringUtils.stringFromBinary(bytes);
	}

	@Override
	public List<ByteArrayRange> getQueryRanges(
			final MultiDimensionalNumericData indexedRange ) {
		return getQueryRanges(
				indexedRange,
				-1);
	}

	@Override
	public List<ByteArrayRange> getQueryRanges(
			final MultiDimensionalNumericData indexedRange,
			final int maxRangeDecomposition ) {
		// a null return here should be interpreted as negative to positive
		// infinite
		return null;
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			final MultiDimensionalNumericData indexedData ) {
		return getInsertionIds(
				indexedData,
				1);
	}

	@Override
	public NumericDimensionDefinition[] getOrderedDimensionDefinitions() {
		// there are no dimensions so return an empty array
		return new NumericDimensionDefinition[] {};
	}

	@Override
	public String getId() {
		return id;
	}

	@Override
	public MultiDimensionalNumericData getRangeForId(
			final ByteArrayId insertionId ) {
		// a null return here should be interpreted as negative to positive
		// infinite
		return null;
	}

	@Override
	public double[] getHighestPrecisionIdRangePerDimension() {
		// there are no dimensions so return an empty array
		return new double[] {};
	}

	@Override
	public long[] getCoordinatesPerDimension(
			final ByteArrayId insertionId ) {
		// there are no dimensions so return an empty array
		return new long[] {};
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			final MultiDimensionalNumericData indexedData,
			final int maxDuplicateInsertionIds ) {
		// return a single empty ID
		final List<ByteArrayId> retVal = new ArrayList<ByteArrayId>();
		retVal.add(new ByteArrayId(
				new byte[] {}));
		return retVal;
	}

}
