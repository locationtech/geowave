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
public class RoundRobinKeyIndexStrategy implements
		PartitionIndexStrategy<MultiDimensionalNumericData, MultiDimensionalNumericData>
{

	private final List<ByteArrayId> keys = new ArrayList<ByteArrayId>();
	public int position = 0;

	/**
	 * Default initial key set size is 3.
	 */
	public RoundRobinKeyIndexStrategy() {
		init(3);
	}

	public RoundRobinKeyIndexStrategy(
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

	@Override
	public Set<ByteArrayId> getInsertionPartitionKeys(
			final MultiDimensionalNumericData insertionData ) {
		position = (position + 1) % keys.size();
		return Collections.singleton(keys.get(position));
	}

	@Override
	public Set<ByteArrayId> getQueryPartitionKeys(
			final MultiDimensionalNumericData queryData,
			final IndexMetaData... hints ) {
		return getPartitionKeys();
	}
}
