package mil.nga.giat.geowave.core.index.simple;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

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
public class RoundRobinKeyIndexStrategy implements
		NumericIndexStrategy
{

	public final List<ByteArrayRange> keySet = new ArrayList<ByteArrayRange>();
	public int position = 0;

	/**
	 * Default initial key set size is 3.
	 */
	public RoundRobinKeyIndexStrategy() {
		init(3);
	}

	public RoundRobinKeyIndexStrategy(
			int size ) {
		init(size);
	}

	private void init(
			int size ) {
		keySet.clear();
		if (size > 256) {
			ByteBuffer buf = ByteBuffer.allocate(4);
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
	 * 
	 * {@inheritDoc}
	 */
	@Override
	public List<ByteArrayRange> getQueryRanges(
			final MultiDimensionalNumericData indexedRange,
			final int maxEstimatedRangeDecomposition ) {
		return keySet;
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
			final MultiDimensionalNumericData indexedData ) {
		position = (position + 1) % keySet.size();
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
		position = (position + 1) % keySet.size();
		return Collections.singletonList(keySet.get(
				position).getStart());
	}

	@Override
	public NumericDimensionDefinition[] getOrderedDimensionDefinitions() {
		return new NumericDimensionDefinition[0];
	}

	@Override
	public MultiDimensionalNumericData getRangeForId(
			final ByteArrayId insertionId ) {
		return new BasicNumericDataset();
	}

	@Override
	public long[] getCoordinatesPerDimension(
			final ByteArrayId insertionId ) {
		return new long[0];
	}

	@Override
	public double[] getHighestPrecisionIdRangePerDimension() {
		return new double[0];
	}

	@Override
	public String getId() {
		return StringUtils.intToString(hashCode());
	}

	public byte[] toBinary() {
		final ByteBuffer buf = ByteBuffer.allocate(4);
		buf.putInt(this.keySet.size());
		return buf.array();

	}

	public void fromBinary(
			byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		init(buf.getInt());
	}
}
