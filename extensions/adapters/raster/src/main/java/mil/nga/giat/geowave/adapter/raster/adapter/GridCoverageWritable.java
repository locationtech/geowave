package mil.nga.giat.geowave.adapter.raster.adapter;

import java.awt.image.DataBuffer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * This class is used by GridCoverageDataAdapter to persist GridCoverages. The
 * adapter has information regarding the sample model and color model so all
 * that is necessary to persist is the buffer and the envelope.
 */
public class GridCoverageWritable implements
		Writable
{
	private DataBuffer dataBuffer;
	private double minX;
	private double maxX;
	private double minY;
	private double maxY;

	protected GridCoverageWritable() {}

	public GridCoverageWritable(
			final DataBuffer dataBuffer,
			final double minX,
			final double maxX,
			final double minY,
			final double maxY ) {
		this.dataBuffer = dataBuffer;
		this.minX = minX;
		this.maxX = maxX;
		this.minY = minY;
		this.maxY = maxY;
	}

	public void set(
			final DataBuffer dataBuffer,
			final double minX,
			final double maxX,
			final double minY,
			final double maxY ) {
		this.dataBuffer = dataBuffer;
		this.minX = minX;
		this.maxX = maxX;
		this.minY = minY;
		this.maxY = maxY;
	}

	public DataBuffer getDataBuffer() {
		return dataBuffer;
	}

	public double getMinX() {
		return minX;
	}

	public double getMaxX() {
		return maxX;
	}

	public double getMinY() {
		return minY;
	}

	public double getMaxY() {
		return maxY;
	}

	@Override
	public void readFields(
			final DataInput input )
			throws IOException {
		final int bufferSize = input.readInt();
		final byte[] buffer = new byte[bufferSize];
		input.readFully(buffer);
		try {
			dataBuffer = RasterTile.getDataBuffer(buffer);
		}
		catch (final ClassNotFoundException e) {
			throw new IOException(
					"Unable to read raster data buffer",
					e);
		}
		minX = input.readDouble();
		maxX = input.readDouble();
		minY = input.readDouble();
		maxY = input.readDouble();
	}

	@Override
	public void write(
			final DataOutput output )
			throws IOException {
		final byte[] dataBufferBinary = RasterTile.getDataBufferBinary(dataBuffer);
		output.writeInt(dataBufferBinary.length);
		output.write(dataBufferBinary);
		output.writeDouble(minX);
		output.writeDouble(maxX);
		output.writeDouble(minY);
		output.writeDouble(maxY);
	}
}
