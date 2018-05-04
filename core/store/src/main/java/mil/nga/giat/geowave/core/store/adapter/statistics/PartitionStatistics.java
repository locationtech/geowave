package mil.nga.giat.geowave.core.store.adapter.statistics;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.ByteUtils;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.NumericHistogram;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.NumericHistogramFactory;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.MinimalBinDistanceHistogram.MinimalBinDistanceHistogramFactory;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class PartitionStatistics<T> extends
		AbstractDataStatistics<T>
{
	public static final ByteArrayId STATS_TYPE = new ByteArrayId(
			"PARTITIONS");
	private Set<ByteArrayId> partitions = new HashSet<ByteArrayId>();

	public PartitionStatistics() {
		super();
	}

	public PartitionStatistics(
			final ByteArrayId dataAdapterId,
			final ByteArrayId indexId ) {
		super(
				dataAdapterId,
				composeId(indexId));
	}

	public static ByteArrayId composeId(
			final ByteArrayId indexId ) {
		return composeId(
				STATS_TYPE.getString(),
				indexId.getString());
	}

	@Override
	public DataStatistics<T> duplicate() {
		return new PartitionStatistics<T>(
				dataAdapterId,
				decomposeIndexIdFromId(statisticsId)); // indexId
	}

	public static ByteArrayId decomposeIndexIdFromId(
			final ByteArrayId statisticsId ) {
		// Need to account for length of type and of the separator
		final int lengthOfNonId = STATS_TYPE.getBytes().length + STATS_ID_SEPARATOR.length();
		final int idLength = statisticsId.getBytes().length - lengthOfNonId;
		final byte[] idBytes = new byte[idLength];
		System.arraycopy(
				statisticsId.getBytes(),
				lengthOfNonId,
				idBytes,
				0,
				idLength);
		return new ByteArrayId(
				idBytes);
	}

	public Set<ByteArrayId> getPartitionKeys() {
		return partitions;
	}

	@Override
	public void merge(
			final Mergeable mergeable ) {
		if (mergeable instanceof PartitionStatistics) {
			partitions.addAll(((PartitionStatistics<?>) mergeable).partitions);
		}
	}

	@Override
	public byte[] toBinary() {
		if (!partitions.isEmpty()) {
			// we know each partition is constant size, so start with the size
			// of the partition keys
			ByteArrayId first = partitions.iterator().next();
			if (first != null && first.getBytes() != null) {
				final ByteBuffer buffer = super.binaryBuffer((first.getBytes().length * partitions.size()) + 1);
				buffer.put((byte) first.getBytes().length);
				for (final ByteArrayId e : partitions) {
					buffer.put(e.getBytes());
				}
				return buffer.array();
			}
		}
		return super.binaryBuffer(
				0).array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = super.binaryBuffer(bytes);
		partitions = new HashSet<>();
		if (buffer.remaining() > 0) {
			int partitionKeySize = unsignedToBytes(buffer.get());
			if (partitionKeySize > 0) {
				int numPartitions = buffer.remaining() / partitionKeySize;
				for (int i = 0; i < numPartitions; i++) {
					byte[] partition = new byte[partitionKeySize];
					buffer.get(partition);
					partitions.add(new ByteArrayId(
							partition));
				}
			}
		}
	}

	public static int unsignedToBytes(
			byte b ) {
		return b & 0xFF;
	}

	@Override
	public void entryIngested(
			final T entry,
			final GeoWaveRow... kvs ) {
		for (final GeoWaveRow kv : kvs) {
			add(getPartitionKey(kv.getPartitionKey()));

		}
	}

	protected static ByteArrayId getPartitionKey(
			final byte[] partitionBytes ) {
		return ((partitionBytes == null) || (partitionBytes.length == 0)) ? null : new ByteArrayId(
				partitionBytes);
	}

	protected void add(
			final ByteArrayId partition ) {
		partitions.add(partition);
	}

	@Override
	public String toString() {
		final StringBuffer buffer = new StringBuffer(
				statisticsId.getString()).append("=");
		if (!partitions.isEmpty()) {
			for (final ByteArrayId p : partitions) {
				if (p == null || p.getBytes() == null) {
					buffer.append("null,");
				}
				else {
					buffer.append(
							p.getHexString()).append(
							",");
				}
			}
			buffer.deleteCharAt(buffer.length() - 1);
		}
		else {
			buffer.append("none");
		}
		return buffer.toString();
	}

	/**
	 * Convert Row Range Numeric statistics to a JSON object
	 */

	@Override
	public JSONObject toJSONObject()
			throws JSONException {
		final JSONObject jo = new JSONObject();
		jo.put(
				"type",
				STATS_TYPE.getString());

		jo.put(
				"statisticsID",
				statisticsId.getString());
		final JSONArray partitionsArray = new JSONArray();
		for (final ByteArrayId p : partitions) {
			final JSONObject partition = new JSONObject();

			if ((p == null) || (p.getBytes() == null)) {
				partition.put(
						"partition",
						"null");
			}
			else {
				partition.put(
						"partition",
						p.getHexString());
			}
			partitionsArray.add(partition);
		}
		jo.put(
				"partitions",
				partitionsArray);
		return jo;
	}
}
