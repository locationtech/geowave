package mil.nga.giat.geowave.mapreduce.output;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.mapreduce.GeoWaveKey;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.io.WritableComparator;

/**
 * This class encapsulates the unique identifier for GeoWave to ingest data
 * using a map-reduce GeoWave output format. The record writer must have bother
 * the adapter and the index for the data element to ingest.
 */
public class GeoWaveOutputKey extends
		GeoWaveKey
{
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private Collection<ByteArrayId> indexIds;

	protected GeoWaveOutputKey() {
		super();
	}

	public GeoWaveOutputKey(
			final ByteArrayId adapterId,
			final ByteArrayId indexId ) {
		super(
				adapterId);
		this.indexIds = Arrays.asList(indexId);
	}

	public GeoWaveOutputKey(
			final ByteArrayId adapterId,
			final Collection<ByteArrayId> indexIds ) {
		super(
				adapterId);
		this.indexIds = indexIds;
	}

	public Collection<ByteArrayId> getIndexIds() {
		return indexIds;
	}

	@Override
	public int compareTo(
			final GeoWaveKey o ) {
		final int baseCompare = super.compareTo(o);
		if (baseCompare != 0) {
			return baseCompare;
		}
		if (o instanceof GeoWaveOutputKey) {
			final GeoWaveOutputKey other = (GeoWaveOutputKey) o;
			final byte[] thisIndex = getConcatenatedIndexId();
			final byte[] otherIndex = other.getConcatenatedIndexId();
			return WritableComparator.compareBytes(
					thisIndex,
					0,
					thisIndex.length,
					otherIndex,
					0,
					otherIndex.length);
		}
		return 1;
	}

	private byte[] getConcatenatedIndexId() {
		final Iterator<ByteArrayId> iterator = indexIds.iterator();
		byte[] bytes = iterator.next().getBytes();
		if (indexIds.size() > 1) {
			while (iterator.hasNext()) {
				bytes = ArrayUtils.addAll(
						bytes,
						iterator.next().getBytes());
			}
		}
		return bytes;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = (prime * result) + ((indexIds == null) ? 0 : indexIds.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (!super.equals(obj)) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final GeoWaveOutputKey other = (GeoWaveOutputKey) obj;
		if (indexIds == null) {
			if (other.indexIds != null) {
				return false;
			}
		}
		else if (!indexIds.equals(other.indexIds)) {
			return false;
		}
		return true;
	}

	@Override
	public void readFields(
			final DataInput input )
			throws IOException {
		super.readFields(input);
		final byte indexIdCount = input.readByte();
		indexIds = new ArrayList<ByteArrayId>();
		for (int i = 0; i < indexIdCount; i++) {
			final int indexIdLength = input.readInt();
			final byte[] indexIdBytes = new byte[indexIdLength];
			input.readFully(indexIdBytes);
			indexIds.add(new ByteArrayId(
					indexIdBytes));
		}
	}

	@Override
	public void write(
			final DataOutput output )
			throws IOException {
		super.write(output);
		output.writeByte(indexIds.size());
		for (final ByteArrayId indexId : indexIds) {
			output.writeInt(indexId.getBytes().length);
			output.write(indexId.getBytes());
		}
	}
}
