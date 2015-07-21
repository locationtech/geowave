package mil.nga.giat.geowave.core.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;

/**
 * Class that implements a compound index strategy. It's a wrapper around two
 * NumericIndexStrategy objects that can externally be treated as a
 * multi-dimensional NumericIndexStrategy.
 */
public class CompoundIndexStrategy implements
		NumericIndexStrategy
{

	private NumericIndexStrategy subStrategy1;
	private NumericIndexStrategy subStrategy2;
	private NumericDimensionDefinition[] baseDefinitions;
	private int defaultNumberOfRanges;

	public CompoundIndexStrategy(
			final NumericIndexStrategy subStrategy1,
			final NumericIndexStrategy subStrategy2 ) {
		this.subStrategy1 = subStrategy1;
		this.subStrategy2 = subStrategy2;
		baseDefinitions = createNumericDimensionDefinitions();
		defaultNumberOfRanges = (int) Math.ceil(Math.pow(
				2,
				getNumberOfDimensions()));
	}

	protected CompoundIndexStrategy() {}

	public NumericIndexStrategy[] getSubStrategies() {
		return new NumericIndexStrategy[] {
			subStrategy1,
			subStrategy2
		};
	}

	@Override
	public byte[] toBinary() {
		final byte[] delegateBinary1 = PersistenceUtils.toBinary(subStrategy1);
		final byte[] delegateBinary2 = PersistenceUtils.toBinary(subStrategy2);
		final ByteBuffer buf = ByteBuffer.allocate(4 + delegateBinary1.length + delegateBinary2.length);
		buf.putInt(delegateBinary1.length);
		buf.put(delegateBinary1);
		buf.put(delegateBinary2);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int delegateBinary1Length = buf.getInt();
		final byte[] delegateBinary1 = new byte[delegateBinary1Length];
		buf.get(delegateBinary1);
		final byte[] delegateBinary2 = new byte[bytes.length - delegateBinary1Length - 4];
		buf.get(delegateBinary2);
		subStrategy1 = PersistenceUtils.fromBinary(
				delegateBinary1,
				NumericIndexStrategy.class);
		subStrategy2 = PersistenceUtils.fromBinary(
				delegateBinary2,
				NumericIndexStrategy.class);
		baseDefinitions = createNumericDimensionDefinitions();
		defaultNumberOfRanges = (int) Math.ceil(Math.pow(
				2,
				getNumberOfDimensions()));
	}

	/**
	 * Get the number of dimensions of each sub-strategy
	 * 
	 * @return an array with the number of dimensions for each sub-strategy
	 */
	public int[] getNumberOfDimensionsPerIndexStrategy() {
		return new int[] {
			subStrategy1.getOrderedDimensionDefinitions().length,
			subStrategy2.getOrderedDimensionDefinitions().length
		};
	}

	/**
	 * Get the total number of dimensions from all sub-strategies
	 * 
	 * @return the number of dimensions
	 */
	public int getNumberOfDimensions() {
		return baseDefinitions.length;
	}

	/**
	 * Create a compound ByteArrayId
	 * 
	 * @param id1
	 *            ByteArrayId for the first sub-strategy
	 * @param id2
	 *            ByteArrayId for the second sub-strategy
	 * @return the ByteArrayId for the compound strategy
	 */
	public ByteArrayId composeByteArrayId(
			final ByteArrayId id1,
			final ByteArrayId id2 ) {
		final byte[] bytes = new byte[id1.getBytes().length + id2.getBytes().length + 4];
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		buf.put(id1.getBytes());
		buf.put(id2.getBytes());
		buf.putInt(id1.getBytes().length);
		return new ByteArrayId(
				bytes);
	}

	/**
	 * Get the ByteArrayId for each sub-strategy from the ByteArrayId for the
	 * compound index strategy
	 * 
	 * @param id
	 *            the compound ByteArrayId
	 * @return the ByteArrayId for each sub-strategy
	 */
	public ByteArrayId[] decomposeByteArrayId(
			final ByteArrayId id ) {
		final ByteBuffer buf = ByteBuffer.wrap(id.getBytes());
		final int id1Length = buf.getInt(id.getBytes().length - 4);
		final byte[] bytes1 = new byte[id1Length];
		final byte[] bytes2 = new byte[id.getBytes().length - id1Length - 4];
		buf.get(bytes1);
		buf.get(bytes2);
		return new ByteArrayId[] {
			new ByteArrayId(
					bytes1),
			new ByteArrayId(
					bytes2)
		};
	}

	private List<ByteArrayId> composeByteArrayIds(
			final List<ByteArrayId> ids1,
			final List<ByteArrayId> ids2 ) {
		final List<ByteArrayId> ids = new ArrayList<>(
				ids1.size() * ids2.size());
		for (final ByteArrayId id1 : ids1) {
			for (final ByteArrayId id2 : ids2) {
				ids.add(composeByteArrayId(
						id1,
						id2));
			}
		}
		return ids;
	}

	private ByteArrayRange composeByteArrayRange(
			final ByteArrayRange rangeOfStrategy1,
			final ByteArrayRange rangeOfStrategy2 ) {
		final ByteArrayId start = composeByteArrayId(
				rangeOfStrategy1.getStart(),
				rangeOfStrategy2.getStart());
		final ByteArrayId end = composeByteArrayId(
				rangeOfStrategy1.getEnd(),
				rangeOfStrategy2.getEnd());
		return new ByteArrayRange(
				start,
				end);
	}

	private List<ByteArrayRange> getByteArrayRanges(
			final List<ByteArrayRange> ranges1,
			final List<ByteArrayRange> ranges2 ) {
		final List<ByteArrayRange> ranges = new ArrayList<>(
				ranges1.size() * ranges2.size());
		for (final ByteArrayRange range1 : ranges1) {
			for (final ByteArrayRange range2 : ranges2) {
				final ByteArrayRange range = composeByteArrayRange(
						range1,
						range2);
				ranges.add(range);
			}
		}
		return ranges;
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
			final int maxEstimatedRangeDecomposition ) {
		final MultiDimensionalNumericData[] ranges = getRangesForIndexedRange(indexedRange);
		final List<ByteArrayRange> rangeForStrategy1;
		final List<ByteArrayRange> rangeForStrategy2;
		if (maxEstimatedRangeDecomposition < 1) {
			rangeForStrategy1 = subStrategy1.getQueryRanges(ranges[0]);
			rangeForStrategy2 = subStrategy2.getQueryRanges(ranges[1]);
		}
		else {
			final int maxEstRangeDecompositionPerStrategy = (int) Math.ceil(Math.sqrt(maxEstimatedRangeDecomposition));
			rangeForStrategy1 = subStrategy1.getQueryRanges(
					ranges[0],
					maxEstRangeDecompositionPerStrategy);
			final int maxEstRangeDecompositionStrategy2 = maxEstimatedRangeDecomposition / rangeForStrategy1.size();
			rangeForStrategy2 = subStrategy2.getQueryRanges(
					ranges[1],
					maxEstRangeDecompositionStrategy2);
		}
		final List<ByteArrayRange> range = getByteArrayRanges(
				rangeForStrategy1,
				rangeForStrategy2);
		return range;
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			final MultiDimensionalNumericData indexedData ) {
		return getInsertionIds(
				indexedData,
				defaultNumberOfRanges);
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			final MultiDimensionalNumericData indexedData,
			final int maxEstimatedDuplicateIds ) {
		final int maxEstDuplicatesPerStrategy = (int) Math.sqrt(maxEstimatedDuplicateIds);
		final MultiDimensionalNumericData[] ranges = getRangesForIndexedRange(indexedData);
		final List<ByteArrayId> rangeForStrategy1 = subStrategy1.getInsertionIds(
				ranges[0],
				maxEstDuplicatesPerStrategy);
		final int maxEstDuplicatesStrategy2 = maxEstimatedDuplicateIds / rangeForStrategy1.size();
		final List<ByteArrayId> rangeForStrategy2 = subStrategy2.getInsertionIds(
				ranges[1],
				maxEstDuplicatesStrategy2);
		final List<ByteArrayId> range = composeByteArrayIds(
				rangeForStrategy1,
				rangeForStrategy2);
		return range;
	}

	private MultiDimensionalNumericData[] getRangesForId(
			final ByteArrayId insertionId ) {
		final ByteArrayId[] insertionIds = decomposeByteArrayId(insertionId);
		return new MultiDimensionalNumericData[] {
			subStrategy1.getRangeForId(insertionIds[0]),
			subStrategy2.getRangeForId(insertionIds[1])
		};
	}

	private MultiDimensionalNumericData[] getRangesForIndexedRange(
			final MultiDimensionalNumericData indexedRange ) {
		final int[] numDimensionsPerStrategy = getNumberOfDimensionsPerIndexStrategy();
		final NumericData[] datasets = indexedRange.getDataPerDimension();
		final NumericData[] datasetForStrategy1 = Arrays.copyOfRange(
				datasets,
				0,
				numDimensionsPerStrategy[0]);
		final NumericData[] datasetForStrategy2 = Arrays.copyOfRange(
				datasets,
				numDimensionsPerStrategy[0],
				datasets.length);
		return new MultiDimensionalNumericData[] {
			new BasicNumericDataset(
					datasetForStrategy1),
			new BasicNumericDataset(
					datasetForStrategy2)
		};
	}

	@Override
	public MultiDimensionalNumericData getRangeForId(
			final ByteArrayId insertionId ) {
		final MultiDimensionalNumericData[] rangesForId = getRangesForId(insertionId);
		final NumericData[] data1 = rangesForId[0].getDataPerDimension();
		final NumericData[] data2 = rangesForId[1].getDataPerDimension();
		final NumericData[] dataPerDimension = new NumericData[data1.length + data2.length];
		System.arraycopy(
				data1,
				0,
				dataPerDimension,
				0,
				data1.length);
		System.arraycopy(
				data2,
				0,
				dataPerDimension,
				data1.length,
				data2.length);
		return new BasicNumericDataset(
				dataPerDimension);
	}

	@Override
	public long[] getCoordinatesPerDimension(
			final ByteArrayId insertionId ) {
		final ByteArrayId[] insertionIds = decomposeByteArrayId(insertionId);
		final long[] coordinates1 = subStrategy1.getCoordinatesPerDimension(insertionIds[0]);
		final long[] coordinates2 = subStrategy2.getCoordinatesPerDimension(insertionIds[1]);
		final long[] coordinates = new long[coordinates1.length + coordinates2.length];
		System.arraycopy(
				coordinates1,
				0,
				coordinates,
				0,
				coordinates1.length);
		System.arraycopy(
				coordinates2,
				0,
				coordinates,
				coordinates1.length,
				coordinates2.length);
		return coordinates;
	}

	private NumericDimensionDefinition[] createNumericDimensionDefinitions() {
		final NumericDimensionDefinition[] strategy1Definitions = subStrategy1.getOrderedDimensionDefinitions();
		final NumericDimensionDefinition[] strategy2Definitions = subStrategy2.getOrderedDimensionDefinitions();
		final NumericDimensionDefinition[] definitions = new NumericDimensionDefinition[strategy1Definitions.length + strategy2Definitions.length];
		System.arraycopy(
				strategy1Definitions,
				0,
				definitions,
				0,
				strategy1Definitions.length);
		System.arraycopy(
				strategy2Definitions,
				0,
				definitions,
				strategy1Definitions.length,
				strategy2Definitions.length);
		return definitions;
	}

	@Override
	public NumericDimensionDefinition[] getOrderedDimensionDefinitions() {
		return baseDefinitions;
	}

	@Override
	public String getId() {
		return StringUtils.intToString(hashCode());
	}

	@Override
	public double[] getHighestPrecisionIdRangePerDimension() {
		final double[] strategy1HighestPrecision = subStrategy1.getHighestPrecisionIdRangePerDimension();
		final double[] strategy2HighestPrecision = subStrategy2.getHighestPrecisionIdRangePerDimension();
		final double[] highestPrecision = new double[strategy1HighestPrecision.length + strategy2HighestPrecision.length];
		System.arraycopy(
				strategy1HighestPrecision,
				0,
				highestPrecision,
				0,
				strategy1HighestPrecision.length);
		System.arraycopy(
				strategy2HighestPrecision,
				0,
				highestPrecision,
				strategy1HighestPrecision.length,
				strategy2HighestPrecision.length);
		return highestPrecision;
	}

}
