/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.mapreduce.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.GeoWaveHBaseKey;

import org.apache.hadoop.io.WritableComparator;

/**
 * @author viggy Functionality similar to <code> GeoWaveInputKey </code>
 */
public class GeoWaveHBaseInputKey extends
		GeoWaveHBaseKey
{

	private ByteArrayId dataId;
	private Byte[] key;

	public GeoWaveHBaseInputKey() {
		super();
	}

	public GeoWaveHBaseInputKey(
			final ByteArrayId adapterId,
			final ByteArrayId dataId ) {
		super(
				adapterId);
		this.dataId = dataId;
	}

	public Byte[] getKey() {
		return key;
	}

	public void setKey(
			final Byte[] key ) {
		this.key = key;
	}

	public void setDataId(
			ByteArrayId dataId ) {
		this.dataId = dataId;
	}

	public ByteArrayId getDataId() {
		return dataId;
	}

	@Override
	public int compareTo(
			final GeoWaveHBaseKey o ) {
		final int baseCompare = super.compareTo(o);
		if (baseCompare != 0) {
			return baseCompare;
		}
		if (o instanceof GeoWaveHBaseInputKey) {
			final GeoWaveHBaseInputKey other = (GeoWaveHBaseInputKey) o;
			return WritableComparator.compareBytes(
					dataId.getBytes(),
					0,
					dataId.getBytes().length,
					other.dataId.getBytes(),
					0,
					other.dataId.getBytes().length);
		}
		return 1;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + ((adapterId == null) ? 0 : adapterId.hashCode());
		result = (prime * result) + ((dataId == null) ? 0 : dataId.hashCode());
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
		final GeoWaveHBaseInputKey other = (GeoWaveHBaseInputKey) obj;
		if (adapterId == null) {
			if (other.adapterId != null) {
				return false;
			}
		}
		else if (!adapterId.equals(other.adapterId)) {
			return false;
		}
		if (dataId == null) {
			if (other.dataId != null) {
				return false;
			}
		}
		else if (!dataId.equals(other.dataId)) {
			return false;
		}
		return true;
	}

	@Override
	public void readFields(
			final DataInput input )
			throws IOException {
		super.readFields(input);
		final int dataIdLength = input.readInt();
		final byte[] dataIdBytes = new byte[dataIdLength];
		input.readFully(dataIdBytes);
		dataId = new ByteArrayId(
				dataIdBytes);
	}

	@Override
	public void write(
			final DataOutput output )
			throws IOException {
		super.write(output);
		output.writeInt(dataId.getBytes().length);
		output.write(dataId.getBytes());
	}

}
