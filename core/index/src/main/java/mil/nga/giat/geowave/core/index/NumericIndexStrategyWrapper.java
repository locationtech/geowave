package mil.nga.giat.geowave.core.index;

import java.nio.ByteBuffer;
import java.util.List;

import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

public class NumericIndexStrategyWrapper implements
		NumericIndexStrategy
{
	private String id;
	private NumericIndexStrategy indexStrategy;

	protected NumericIndexStrategyWrapper() {}

	public NumericIndexStrategyWrapper(
			final String id,
			final NumericIndexStrategy indexStrategy ) {
		this.id = id;
		this.indexStrategy = indexStrategy;
	}

	@Override
	public String getId() {
		return id;
	}

	@Override
	public byte[] toBinary() {
		final byte[] idBinary = StringUtils.stringToBinary(id);
		final byte[] delegateBinary = PersistenceUtils.toBinary(indexStrategy);
		final ByteBuffer buf = ByteBuffer.allocate(4 + idBinary.length + delegateBinary.length);
		buf.putInt(idBinary.length);
		buf.put(idBinary);
		buf.put(delegateBinary);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int idBinaryLength = buf.getInt();
		final byte[] idBinary = new byte[idBinaryLength];
		final byte[] delegateBinary = new byte[bytes.length - idBinaryLength - 4];
		buf.get(idBinary);
		buf.get(delegateBinary);
		id = StringUtils.stringFromBinary(idBinary);
		indexStrategy = PersistenceUtils.fromBinary(
				delegateBinary,
				NumericIndexStrategy.class);
	}

	@Override
	public List<ByteArrayRange> getQueryRanges(
			final MultiDimensionalNumericData indexedRange ) {
		return indexStrategy.getQueryRanges(indexedRange);
	}

	@Override
	public List<ByteArrayRange> getQueryRanges(
			final MultiDimensionalNumericData indexedRange,
			final int maxRangeDecomposition ) {
		return indexStrategy.getQueryRanges(
				indexedRange,
				maxRangeDecomposition);
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			final MultiDimensionalNumericData indexedData ) {
		return indexStrategy.getInsertionIds(indexedData);
	}

	@Override
	public MultiDimensionalNumericData getRangeForId(
			final ByteArrayId insertionId ) {
		return indexStrategy.getRangeForId(insertionId);
	}

	@Override
	public long[] getCoordinatesPerDimension(
			final ByteArrayId insertionId ) {
		return indexStrategy.getCoordinatesPerDimension(insertionId);
	}

	@Override
	public NumericDimensionDefinition[] getOrderedDimensionDefinitions() {
		return indexStrategy.getOrderedDimensionDefinitions();
	}

	@Override
	public double[] getHighestPrecisionIdRangePerDimension() {
		return indexStrategy.getHighestPrecisionIdRangePerDimension();
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			final MultiDimensionalNumericData indexedData,
			final int maxDuplicateInsertionIds ) {
		return indexStrategy.getInsertionIds(
				indexedData,
				maxDuplicateInsertionIds);
	}
}
