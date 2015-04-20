package mil.nga.giat.geowave.datastore.accumulo.mapreduce.output;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.GeoWaveKey;

import org.apache.hadoop.io.WritableComparator;

/**
 * This class encapsulates the unique identifier for GeoWave to ingest data
 * using a map-reduce GeoWave output format. The record writer must have bother
 * the adapter and the index for the data element to ingest.
 */
public class GeoWaveOutputKey extends
		GeoWaveKey
{
	private ByteArrayId indexId;

	protected GeoWaveOutputKey() {
		super();
	}

	public GeoWaveOutputKey(
			final ByteArrayId adapterId,
			final ByteArrayId indexId ) {
		super(
				adapterId);
		this.indexId = indexId;
	}

	public ByteArrayId getIndexId() {
		return indexId;
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
			return WritableComparator.compareBytes(
					indexId.getBytes(),
					0,
					indexId.getBytes().length,
					other.indexId.getBytes(),
					0,
					other.indexId.getBytes().length);
		}
		return 1;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + ((adapterId == null) ? 0 : adapterId.hashCode());
		result = (prime * result) + ((indexId == null) ? 0 : indexId.hashCode());
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
		final GeoWaveOutputKey other = (GeoWaveOutputKey) obj;
		if (adapterId == null) {
			if (other.adapterId != null) {
				return false;
			}
		}
		else if (!adapterId.equals(other.adapterId)) {
			return false;
		}
		if (indexId == null) {
			if (other.indexId != null) {
				return false;
			}
		}
		else if (!indexId.equals(other.indexId)) {
			return false;
		}
		return true;
	}

	@Override
	public void readFields(
			final DataInput input )
			throws IOException {
		super.readFields(input);
		final int indexIdLength = input.readInt();
		final byte[] indexIdBytes = new byte[indexIdLength];
		input.readFully(indexIdBytes);
		indexId = new ByteArrayId(
				indexIdBytes);
	}

	@Override
	public void write(
			final DataOutput output )
			throws IOException {
		super.write(output);
		output.writeInt(indexId.getBytes().length);
		output.write(indexId.getBytes());
	}
}
