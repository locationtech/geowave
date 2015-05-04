package mil.nga.giat.geowave.core.store.index;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;

public class CustomIdIndex extends
		Index
{
	private ByteArrayId id;

	public CustomIdIndex() {
		super();
	}

	public CustomIdIndex(
			final NumericIndexStrategy indexStrategy,
			final CommonIndexModel indexModel,
			final ByteArrayId id ) {
		super(
				indexStrategy,
				indexModel);
		this.id = id;
	}

	@Override
	public ByteArrayId getId() {
		return id;
	}

	@Override
	public byte[] toBinary() {
		final byte[] selfBinary = super.toBinary();
		final byte[] idBinary = id.getBytes();
		final ByteBuffer buf = ByteBuffer.allocate(4 + idBinary.length + selfBinary.length);
		buf.putInt(selfBinary.length);
		buf.put(selfBinary);
		buf.put(idBinary);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int selfBinaryLength = buf.getInt();
		final byte[] selfBinary = new byte[selfBinaryLength];
		buf.get(selfBinary);

		super.fromBinary(selfBinary);
		final byte[] idBinary = new byte[bytes.length - selfBinaryLength - 4];
		buf.get(idBinary);
		id = new ByteArrayId(
				idBinary);
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (!(obj instanceof CustomIdIndex)) {
			return false;
		}
		return super.equals(obj);
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}

}
