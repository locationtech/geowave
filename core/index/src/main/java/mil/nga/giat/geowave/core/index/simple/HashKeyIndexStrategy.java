package mil.nga.giat.geowave.core.index.simple;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.PartitionIndexStrategy;
import mil.nga.giat.geowave.core.index.StringUtils;
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
public class HashKeyIndexStrategy implements
		PartitionIndexStrategy<MultiDimensionalNumericData, MultiDimensionalNumericData>
{

	private final List<ByteArrayId> keys = new ArrayList<ByteArrayId>();

	public HashKeyIndexStrategy() {
		this(
				3);
	}

	public HashKeyIndexStrategy(
			final int size ) {
		init(size);
	}

	private void init(
			final int size ) {
		keys.clear();
		if (size > 256) {
			final ByteBuffer buf = ByteBuffer.allocate(4);
			for (int i = 0; i < size; i++) {
				buf.putInt(i);
				final ByteArrayId id = new ByteArrayId(
						Arrays.copyOf(
								buf.array(),
								4));
				keys.add(id);
				buf.rewind();
			}
		}
		else {
			for (int i = 0; i < size; i++) {
				final ByteArrayId id = new ByteArrayId(
						new byte[] {
							(byte) i
						});
				keys.add(id);
			}
		}
	}

	@Override
	public String getId() {
		return StringUtils.intToString(hashCode());
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buf = ByteBuffer.allocate(4);
		buf.putInt(keys.size());
		return buf.array();

	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		init(buf.getInt());
	}

	public Set<ByteArrayId> getPartitionKeys() {
		return Sets.newHashSet(keys);
	}

	private static long hashCode(
			final double a1[],
			final long start ) {
		long result = start;
		for (final double element : a1) {
			final long bits = Double.doubleToLongBits(element);
			result = (31 * result) + (bits ^ (bits >>> 32));
		}
		return result;
	}

	@Override
	public int getPartitionKeyLength() {
		if ((keys != null) && !keys.isEmpty()) {
			return keys.get(
					0).getBytes().length;
		}
		return 0;
	}

	@Override
	public List<IndexMetaData> createMetaData() {
		return Collections.emptyList();
	}

	/**
	 * Returns an insertion id selected round-robin from a predefined pool
	 *
	 */
	@Override
	public Set<ByteArrayId> getInsertionPartitionKeys(
			final MultiDimensionalNumericData insertionData ) {
		final long hashCode = Math.abs(hashCode(
				insertionData.getMaxValuesPerDimension(),
				hashCode(
						insertionData.getMinValuesPerDimension(),
						1)));
		final int position = (int) (hashCode % keys.size());

		return Collections.singleton(keys.get(position));
	}

	/**
	 * always return all keys
	 */
	@Override
	public Set<ByteArrayId> getQueryPartitionKeys(
			final MultiDimensionalNumericData queryData,
			final IndexMetaData... hints ) {
		return getPartitionKeys();
	}
}
