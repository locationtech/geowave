package mil.nga.giat.geowave.adapter.raster.adapter;

import java.awt.image.DataBuffer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import javax.media.jai.remote.SerializableState;
import javax.media.jai.remote.SerializerFactory;

import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RasterTile<T extends Persistable> implements
		Mergeable
{
	private final static Logger LOGGER = LoggerFactory.getLogger(RasterTile.class);
	private DataBuffer dataBuffer;
	private T metadata;

	protected RasterTile() {
		super();
	}

	public RasterTile(
			final DataBuffer dataBuffer,
			final T metadata ) {
		this.dataBuffer = dataBuffer;
		this.metadata = metadata;
	}

	public DataBuffer getDataBuffer() {
		return dataBuffer;
	}

	public T getMetadata() {
		return metadata;
	}

	protected static byte[] getDataBufferBinary(
			final DataBuffer dataBuffer ) {
		final SerializableState serializableDataBuffer = SerializerFactory.getState(dataBuffer);
		try {
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			final ObjectOutputStream oos = new ObjectOutputStream(
					baos);
			oos.writeObject(serializableDataBuffer);
			return baos.toByteArray();
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to serialize data buffer",
					e);
		}
		return new byte[] {};
	}

	protected static DataBuffer getDataBuffer(
			final byte[] binary )
			throws IOException,
			ClassNotFoundException {
		final ByteArrayInputStream bais = new ByteArrayInputStream(
				binary);
		final ObjectInputStream ois = new ObjectInputStream(
				bais);
		final Object o = ois.readObject();
		if ((o instanceof SerializableState) && (((SerializableState) o).getObject() instanceof DataBuffer)) {
			return (DataBuffer) ((SerializableState) o).getObject();
		}
		return null;
	}

	@Override
	public byte[] toBinary() {
		final byte[] dataBufferBinary = getDataBufferBinary(dataBuffer);
		byte[] metadataBytes;
		if (metadata != null) {
			metadataBytes = PersistenceUtils.toBinary(metadata);
		}
		else {
			metadataBytes = new byte[] {};
		}
		final ByteBuffer buf = ByteBuffer.allocate(metadataBytes.length + dataBufferBinary.length + 4);
		buf.putInt(metadataBytes.length);
		buf.put(metadataBytes);
		buf.put(dataBufferBinary);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		try {
			final ByteBuffer buf = ByteBuffer.wrap(bytes);
			final int metadataLength = buf.getInt();
			if (metadataLength > 0) {
				final byte[] metadataBytes = new byte[metadataLength];
				buf.get(metadataBytes);
				metadata = (T) PersistenceUtils.fromBinary(
						metadataBytes,
						Persistable.class);
			}
			final byte[] dataBufferBytes = new byte[bytes.length - metadataLength - 4];
			buf.get(dataBufferBytes);
			dataBuffer = getDataBuffer(dataBufferBytes);
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Unable to deserialize data buffer",
					e);
		}
	}

	public void setDataBuffer(
			final DataBuffer dataBuffer ) {
		this.dataBuffer = dataBuffer;
	}

	public void setMetadata(
			final T metadata ) {
		this.metadata = metadata;
	}

	@Override
	public void merge(
			final Mergeable merge ) {
		// This will get wrapped as a MergeableRasterTile by the combiner to
		// support merging
	}
}
