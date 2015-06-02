package mil.nga.giat.geowave.datastore.hbase.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import mil.nga.giat.geowave.core.index.ByteArrayId;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author viggy
 * 
 *         TODO #406 This is just duplicate of GeoWaveKey and added to not have
 *         dependency. It can be merged together and moved to a common source
 *         directory. It is currently needed because this source directory
 *         cannot depend on geowave-store-accumulo
 */
public abstract class GeoWaveHBaseKey implements
		WritableComparable<GeoWaveHBaseKey>
{
	protected ByteArrayId adapterId;

	protected GeoWaveHBaseKey() {}

	public GeoWaveHBaseKey(
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
			final GeoWaveHBaseKey o ) {
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
