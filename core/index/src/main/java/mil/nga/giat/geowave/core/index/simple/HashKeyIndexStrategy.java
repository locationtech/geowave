package mil.nga.giat.geowave.core.index.simple;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericValue;

/**
 * Used to create determined, uniform row id prefix as one possible approach to
 * prevent hot spotting.
 * 
 * Before using this class, one should consider balancing options for the
 * specific data store. Can one pre-split using a component of another index
 * strategy (e.g. bin identifier)? How about ingest first and then do major
 * compaction?
 * 
 * Consider that Accumulo 1.7 supports two balancers
 * org.apache.accumulo.server.master.balancer.RegexGroupBalancer and
 * org.apache.accumulo.server.master.balancer.GroupBalancer.
 * 
 * This class should be used with a CompoundIndexStrategy. In addition, tablets
 * should be pre-split on the number of prefix IDs. Without splits, the splits
 * are at the mercy of the Big Table servers default. For example, Accumulo
 * fills up one tablet before splitting, regardless of the partitioning.
 * 
 * The key set size does not need to be large. For example, using two times the
 * number of tablet servers (for growth) and presplitting, two keys per server.
 * The default is 3.
 * 
 * There is a cost to using this approach: queries must span all prefixes. The
 * number of prefixes should initially be at least the number of tablet servers.
 * 
 * 
 * 
 */
public class HashKeyIndexStrategy implements
		NumericIndexStrategy
{

	private final List<ByteArrayRange> keySet = new ArrayList<ByteArrayRange>();
	private NumericDimensionDefinition[] definitions;
	private double[] higestPrecision;

	public HashKeyIndexStrategy() {
		this(
				new NumericDimensionDefinition[0],
				3);
	}

	public HashKeyIndexStrategy(
			final NumericDimensionDefinition[] definitions,
			final int size ) {
		this.definitions = definitions;
		higestPrecision = new double[definitions.length];
		init(size);
	}

	private void init(
			final int size ) {
		keySet.clear();
		if (size > 256) {
			final ByteBuffer buf = ByteBuffer.allocate(4);
			for (int i = 0; i < size; i++) {
				buf.putInt(i);
				final ByteArrayId id = new ByteArrayId(
						Arrays.copyOf(
								buf.array(),
								4));
				keySet.add(new ByteArrayRange(
						id,
						id));
				buf.rewind();
			}
		}
		else {
			for (int i = 0; i < size; i++) {
				final ByteArrayId id = new ByteArrayId(
						new byte[] {
							(byte) i
						});
				keySet.add(new ByteArrayRange(
						id,
						id));
			}
		}
	}

	/**
	 * Always returns all possible ranges
	 * 
	 * {@inheritDoc}
	 */
	@Override
	public List<ByteArrayRange> getQueryRanges(
			final MultiDimensionalNumericData indexedRange ) {
		return keySet;
	}

	/**
	 * Always returns all possible ranges
	 */
	@Override
	public List<ByteArrayRange> getQueryRanges(
			final MultiDimensionalNumericData indexedRange,
			final int maxEstimatedRangeDecomposition ) {
		return keySet;
	}

	/**
	 * Returns an insertion id selected round-robin from a predefined pool
	 * 
	 */
	@Override
	public List<ByteArrayId> getInsertionIds(
			final MultiDimensionalNumericData indexedData ) {
		final long hashCode = Math.abs(hashCode(
				indexedData.getMaxValuesPerDimension(),
				hashCode(
						indexedData.getMinValuesPerDimension(),
						1)));
		final int position = (int) (hashCode % keySet.size());

		return Collections.singletonList(keySet.get(
				position).getStart());
	}

	/**
	 * Returns all of the insertion ids for the range. Since this index strategy
	 * doensn't use binning, it will return the ByteArrayId of every value in
	 * the range (i.e. if you are storing a range using this index strategy,
	 * your data will be replicated for every integer value in the range).
	 * 
	 * {@inheritDoc}
	 */
	@Override
	public List<ByteArrayId> getInsertionIds(
			final MultiDimensionalNumericData indexedData,
			final int maxEstimatedDuplicateIds ) {
		return getInsertionIds(indexedData);
	}

	@Override
	public NumericDimensionDefinition[] getOrderedDimensionDefinitions() {
		return definitions;
	}

	@Override
	public MultiDimensionalNumericData getRangeForId(
			final ByteArrayId insertionId ) {
		return new BasicNumericDataset(
				new NumericData[] {
					new NumericValue(
							0),
					new NumericValue(
							0)
				});
	}

	@Override
	public long[] getCoordinatesPerDimension(
			final ByteArrayId insertionId ) {
		return new long[this.definitions.length];
	}

	@Override
	public double[] getHighestPrecisionIdRangePerDimension() {
		return higestPrecision;
	}

	@Override
	public String getId() {
		return StringUtils.intToString(hashCode());
	}

	@Override
	public byte[] toBinary() {
		final Object[] definitionBytes = new Object[this.definitions.length];
		int i = 0;
		int count = 0;
		for (NumericDimensionDefinition def : this.definitions) {
			definitionBytes[i] = PersistenceUtils.toBinary(def);
			count += ((byte[]) definitionBytes[i++]).length;
		}
		final ByteBuffer buf = ByteBuffer.allocate(8 + this.definitions.length * 4 + count);

		buf.putInt(keySet.size());

		buf.putInt(definitionBytes.length);
		for (int j = 0; j < definitionBytes.length; j++) {
			buf.putInt(((byte[]) definitionBytes[j]).length);
			buf.put((byte[]) definitionBytes[j]);
		}
		return buf.array();

	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		init(buf.getInt());
		definitions = new NumericDimensionDefinition[buf.getInt()];
		for (int i = 0; i < definitions.length; i++) {
			final byte[] data = new byte[buf.getInt()];
			buf.get(data);
			definitions[i] = PersistenceUtils.fromBinary(
					data,
					NumericDimensionDefinition.class);
		}
		this.higestPrecision = new double[definitions.length];
	}

	@Override
	public Set<ByteArrayId> getNaturalSplits() {
		final Set<ByteArrayId> naturalSplits = new HashSet<ByteArrayId>();
		for (final ByteArrayRange range : keySet) {
			naturalSplits.add(range.getStart());
		}
		return naturalSplits;
	}

	private static long hashCode(
			double a1[],
			long start ) {

		long result = start;
		for (double element : a1) {
			long bits = Double.doubleToLongBits(element);
			result = 31 * result + (long) (bits ^ (bits >>> 32));
		}
		return result;
	}

	@Override
	public int getByteOffsetFromDimensionalIndex() {
		if ((keySet != null) && !keySet.isEmpty()) {
			return keySet.get(
					0).getStart().getBytes().length;
		}
		return 0;
	}
}
