package mil.nga.giat.geowave.core.index;

import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

public class PartitionIndexStrategyWrapper implements
		NumericIndexStrategy
{
	private PartitionIndexStrategy<MultiDimensionalNumericData, MultiDimensionalNumericData> partitionIndexStrategy;

	public PartitionIndexStrategyWrapper() {}

	public PartitionIndexStrategyWrapper(
			final PartitionIndexStrategy<MultiDimensionalNumericData, MultiDimensionalNumericData> partitionIndexStrategy ) {
		this.partitionIndexStrategy = partitionIndexStrategy;
	}

	@Override
	public QueryRanges getQueryRanges(
			final MultiDimensionalNumericData indexedRange,
			final IndexMetaData... hints ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public QueryRanges getQueryRanges(
			final MultiDimensionalNumericData indexedRange,
			final int maxEstimatedRangeDecomposition,
			final IndexMetaData... hints ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InsertionIds getInsertionIds(
			final MultiDimensionalNumericData indexedData ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InsertionIds getInsertionIds(
			final MultiDimensionalNumericData indexedData,
			final int maxEstimatedDuplicateIds ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MultiDimensionalNumericData getRangeForId(
			final ByteArrayId partitionKey,
			final ByteArrayId sortKey ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getId() {
		return partitionIndexStrategy.getId();
	}

	@Override
	public List<IndexMetaData> createMetaData() {
		return partitionIndexStrategy.createMetaData();
	}

	@Override
	public byte[] toBinary() {
		return PersistenceUtils.toBinary(partitionIndexStrategy);
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		partitionIndexStrategy = (PartitionIndexStrategy<MultiDimensionalNumericData, MultiDimensionalNumericData>) PersistenceUtils
				.fromBinary(bytes);
	}

	@Override
	public MultiDimensionalCoordinates getCoordinatesPerDimension(
			final ByteArrayId partitionKey,
			final ByteArrayId sortKey ) {
		return new MultiDimensionalCoordinates();
	}

	@Override
	public MultiDimensionalCoordinateRanges[] getCoordinateRangesPerDimension(
			final MultiDimensionalNumericData dataRange,
			final IndexMetaData... hints ) {
		return null;
	}

	@Override
	public NumericDimensionDefinition[] getOrderedDimensionDefinitions() {
		return null;
	}

	@Override
	public double[] getHighestPrecisionIdRangePerDimension() {
		return null;
	}

	@Override
	public int getPartitionKeyLength() {
		return partitionIndexStrategy.getPartitionKeyLength();
	}

	@Override
	public Set<ByteArrayId> getInsertionPartitionKeys(
			final MultiDimensionalNumericData insertionData ) {
		return partitionIndexStrategy.getInsertionPartitionKeys(insertionData);
	}

	@Override
	public Set<ByteArrayId> getQueryPartitionKeys(
			final MultiDimensionalNumericData queryData,
			final IndexMetaData... hints ) {
		return partitionIndexStrategy.getQueryPartitionKeys(
				queryData,
				hints);
	}
}
