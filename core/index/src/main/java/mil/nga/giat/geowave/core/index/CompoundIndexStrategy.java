package mil.nga.giat.geowave.core.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;

/**
 * Class that implements a compound index strategy. It's a wrapper around two
 * NumericIndexStrategy objects that can externally be treated as a
 * multi-dimensional NumericIndexStrategy.
 * 
 * Each of the 'wrapped' strategies cannot share the same dimension definition.
 * 
 */
public class CompoundIndexStrategy implements
		NumericIndexStrategy
{

	private NumericIndexStrategy subStrategy1;
	private NumericIndexStrategy subStrategy2;
	private NumericDimensionDefinition[] baseDefinitions;
	private double[] highestPrecision;
	private int[] strategy1Mappings;
	private int[] strategy2Mappings;
	private int defaultNumberOfRanges;

	public CompoundIndexStrategy(
			final NumericIndexStrategy subStrategy1,
			final NumericIndexStrategy subStrategy2 ) {
		this.subStrategy1 = subStrategy1;
		this.subStrategy2 = subStrategy2;
		init();
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
		this.init();
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
		final NumericData[] datasets = indexedRange.getDataPerDimension();

		final int[] numDimensionsPerStrategy = getNumberOfDimensionsPerIndexStrategy();

		final NumericData[] datasetForStrategy1 = new NumericData[numDimensionsPerStrategy[0]];
		final NumericData[] datasetForStrategy2 = new NumericData[numDimensionsPerStrategy[1]];
		for (int i = 0; i < datasets.length; i++) {
			if (strategy1Mappings[i] >= 0) {
				datasetForStrategy1[strategy1Mappings[i]] = datasets[i];
			}
			if (strategy2Mappings[i] >= 0) {
				datasetForStrategy2[strategy2Mappings[i]] = datasets[i];
			}
		}
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
		final NumericData[] dataPerDimension = new NumericData[baseDefinitions.length];
		for (int i = 0; i < dataPerDimension.length; i++) {
			if (strategy1Mappings[i] >= 0) {
				dataPerDimension[i] = data1[strategy1Mappings[i]];
			}
			if (strategy2Mappings[i] >= 0) {
				dataPerDimension[i] = data2[strategy2Mappings[i]];
			}
		}
		return new BasicNumericDataset(
				dataPerDimension);
	}

	@Override
	public long[] getCoordinatesPerDimension(
			final ByteArrayId insertionId ) {
		final ByteArrayId[] insertionIds = decomposeByteArrayId(insertionId);
		final long[] coordinates1 = subStrategy1.getCoordinatesPerDimension(insertionIds[0]);
		final long[] coordinates2 = subStrategy2.getCoordinatesPerDimension(insertionIds[1]);
		final long[] coordinates = new long[baseDefinitions.length];
		for (int i = 0; i < baseDefinitions.length; i++) {
			if (strategy1Mappings[i] >= 0) {
				coordinates[i] = coordinates1[strategy1Mappings[i]];
			}
			if (strategy2Mappings[i] >= 0) {
				coordinates[i] = coordinates2[strategy2Mappings[i]];
			}
		}
		return coordinates;
	}

	private void init() {
		final NumericDimensionDefinition[] strategy1Definitions = subStrategy1.getOrderedDimensionDefinitions();
		final NumericDimensionDefinition[] strategy2Definitions = subStrategy2.getOrderedDimensionDefinitions();
		final double[] strategy1HighestPrecision = subStrategy1.getHighestPrecisionIdRangePerDimension();
		final double[] strategy2HighestPrecision = subStrategy2.getHighestPrecisionIdRangePerDimension();

		final List<NumericDimensionDefinition> definitions = new ArrayList<NumericDimensionDefinition>(
				strategy1Definitions.length + strategy2Definitions.length);
		final double precision[] = new double[strategy1Definitions.length + strategy2Definitions.length];
		strategy1Mappings = new int[precision.length];
		strategy2Mappings = new int[precision.length];

		int dimsPosition = 0;
		for (NumericDimensionDefinition definition : strategy1Definitions) {
			definitions.add(definition);
			strategy1Mappings[dimsPosition] = dimsPosition;
			strategy2Mappings[dimsPosition] = -1;
			precision[dimsPosition] = strategy1HighestPrecision[dimsPosition];
			dimsPosition++;
		}
		int twosDefsPosition = 0;
		for (NumericDimensionDefinition definition : strategy2Definitions) {
			int pos = definitions.indexOf(definition);
			if (pos >= 0) {
				strategy2Mappings[pos] = twosDefsPosition;
				precision[pos] = Math.max(
						precision[pos],
						strategy2HighestPrecision[twosDefsPosition]);
			}
			else {
				strategy2Mappings[dimsPosition] = twosDefsPosition;
				strategy1Mappings[dimsPosition] = -1;
				definitions.add(definition);
				precision[dimsPosition] = strategy2HighestPrecision[twosDefsPosition];
				dimsPosition++;
			}
			twosDefsPosition++;
		}

		this.baseDefinitions = definitions.toArray(new NumericDimensionDefinition[definitions.size()]);
		this.highestPrecision = Arrays.copyOfRange(
				precision,
				0,
				baseDefinitions.length);
	}

	@Override
	public NumericDimensionDefinition[] getOrderedDimensionDefinitions() {
		return baseDefinitions;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + Arrays.hashCode(baseDefinitions);
		result = (prime * result) + defaultNumberOfRanges;
		result = (prime * result) + ((subStrategy1 == null) ? 0 : subStrategy1.hashCode());
		result = (prime * result) + ((subStrategy2 == null) ? 0 : subStrategy2.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final CompoundIndexStrategy other = (CompoundIndexStrategy) obj;
		if (!Arrays.equals(
				baseDefinitions,
				other.baseDefinitions)) {
			return false;
		}
		if (defaultNumberOfRanges != other.defaultNumberOfRanges) {
			return false;
		}
		if (subStrategy1 == null) {
			if (other.subStrategy1 != null) {
				return false;
			}
		}
		else if (!subStrategy1.equals(other.subStrategy1)) {
			return false;
		}
		if (subStrategy2 == null) {
			if (other.subStrategy2 != null) {
				return false;
			}
		}
		else if (!subStrategy2.equals(other.subStrategy2)) {
			return false;
		}
		return true;
	}

	@Override
	public String getId() {
		return StringUtils.intToString(hashCode());
	}

	@Override
	public double[] getHighestPrecisionIdRangePerDimension() {
		return highestPrecision;
	}

	@Override
	public Set<ByteArrayId> getNaturalSplits() {
		// because substrategy one is prefixing substrategy2, just use the
		// splits associated with substrategy1
		return subStrategy1.getNaturalSplits();
	}

	@Override
	public int getByteOffsetFromDimensionalIndex() {
		// TODO: this only makes sense if substrategy 1 contributes no
		// dimensional index component
		return subStrategy1.getByteOffsetFromDimensionalIndex() + subStrategy2.getByteOffsetFromDimensionalIndex();
	}

}
