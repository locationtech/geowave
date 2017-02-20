package mil.nga.giat.geowave.mapreduce.splits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import mil.nga.giat.geowave.core.index.ByteArrayId;

/**
 * The Class GeoWaveInputSplit. Encapsulates a GeoWave Index and a set of Row
 * ranges for use in Map Reduce jobs.
 */
public class GeoWaveInputSplit extends
		InputSplit implements
		Writable
{
	private Map<ByteArrayId, SplitInfo> splitInfo;
	private String[] locations;

	protected GeoWaveInputSplit() {
		splitInfo = new HashMap<ByteArrayId, SplitInfo>();
		locations = new String[] {};
	}

	protected GeoWaveInputSplit(
			final Map<ByteArrayId, SplitInfo> splitInfo,
			final String[] locations ) {
		this.splitInfo = splitInfo;
		this.locations = locations;
	}

	public Set<ByteArrayId> getIndexIds() {
		return splitInfo.keySet();
	}

	public SplitInfo getInfo(
			final ByteArrayId indexId ) {
		return splitInfo.get(indexId);
	}

	/**
	 * This implementation of length is only an estimate, it does not provide
	 * exact values. Do not have your code rely on this return value.
	 */
	@Override
	public long getLength()
			throws IOException {
		long diff = 0;
		for (final Entry<ByteArrayId, SplitInfo> indexEntry : splitInfo.entrySet()) {
			for (final RangeLocationPair range : indexEntry.getValue().getRangeLocationPairs()) {
				diff += (long) range.getCardinality();
			}
		}
		return diff;
	}

	@Override
	public String[] getLocations()
			throws IOException {
		return locations;
	}

	@Override
	public void readFields(
			final DataInput in )
			throws IOException {
		final int numIndices = in.readInt();
		splitInfo = new HashMap<ByteArrayId, SplitInfo>(
				numIndices);
		for (int i = 0; i < numIndices; i++) {
			final int indexIdLength = in.readInt();
			final byte[] indexIdBytes = new byte[indexIdLength];
			in.readFully(indexIdBytes);
			final ByteArrayId indexId = new ByteArrayId(
					indexIdBytes);
			final SplitInfo si = new SplitInfo();
			si.readFields(in);
			splitInfo.put(
					indexId,
					si);
		}
		final int numLocs = in.readInt();
		locations = new String[numLocs];
		for (int i = 0; i < numLocs; ++i) {
			locations[i] = in.readUTF();
		}
	}

	@Override
	public void write(
			final DataOutput out )
			throws IOException {
		out.writeInt(splitInfo.size());
		for (final Entry<ByteArrayId, SplitInfo> range : splitInfo.entrySet()) {
			final byte[] indexIdBytes = range.getKey().getBytes();
			out.writeInt(indexIdBytes.length);
			out.write(indexIdBytes);
			final SplitInfo rangeList = range.getValue();
			rangeList.write(out);
		}
		out.writeInt(locations.length);
		for (final String location : locations) {
			out.writeUTF(location);
		}
	}
}