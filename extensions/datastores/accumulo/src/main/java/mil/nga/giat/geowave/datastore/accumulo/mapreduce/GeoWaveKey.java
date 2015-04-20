package mil.nga.giat.geowave.datastore.accumulo.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import mil.nga.giat.geowave.core.index.ByteArrayId;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * This is the base class for both GeoWaveInputKey and GeoWaveOutputKey and is
 * responsible for persisting the adapter ID
 */
public abstract class GeoWaveKey implements
		WritableComparable<GeoWaveKey>
{
	protected ByteArrayId adapterId;

	protected GeoWaveKey() {}

	public GeoWaveKey(
			final ByteArrayId adapterId ) {
		this.adapterId = adapterId;
	}

	public ByteArrayId getAdapterId() {
		return adapterId;
	}

	public void setAdapterId(
			ByteArrayId adapterId ) {
		this.adapterId = adapterId;
	}

	@Override
	public int compareTo(
			final GeoWaveKey o ) {
		return WritableComparator.compareBytes(
				adapterId.getBytes(),
				0,
				adapterId.getBytes().length,
				o.adapterId.getBytes(),
				0,
				o.adapterId.getBytes().length);
	}

	@Override
	public void readFields(
			final DataInput input )
			throws IOException {
		final int adapterIdLength = input.readInt();
		final byte[] adapterIdBinary = new byte[adapterIdLength];
		input.readFully(adapterIdBinary);
		adapterId = new ByteArrayId(
				adapterIdBinary);
	}

	@Override
	public void write(
			final DataOutput output )
			throws IOException {
		final byte[] adapterIdBinary = adapterId.getBytes();
		output.writeInt(adapterIdBinary.length);
		output.write(adapterIdBinary);
	}

}
