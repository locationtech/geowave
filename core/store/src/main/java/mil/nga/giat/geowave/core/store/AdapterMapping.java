package mil.nga.giat.geowave.core.store;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.persist.Persistable;

public class AdapterMapping implements
		Persistable
{
	private ByteArrayId adapterId;
	private short internalAdapterId;

	public AdapterMapping() {

	}

	public AdapterMapping(
			ByteArrayId adapterId,
			short internalAdapterId ) {
		super();
		this.adapterId = adapterId;
		this.internalAdapterId = internalAdapterId;
	}

	public ByteArrayId getAdapterId() {
		return adapterId;
	}

	public short getInteranalAdapterId() {
		return internalAdapterId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((adapterId == null) ? 0 : adapterId.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		AdapterMapping other = (AdapterMapping) obj;
		if (adapterId == null) {
			if (other.adapterId != null) return false;
		}
		else if (!adapterId.equals(other.adapterId)) return false;
		if (internalAdapterId != other.internalAdapterId) return false;
		return true;
	}

	@Override
	public byte[] toBinary() {
		final byte[] adapterIdBytes = this.adapterId.getBytes();
		final ByteBuffer buf = ByteBuffer.allocate(adapterIdBytes.length + 2);
		buf.put(adapterIdBytes);
		buf.putShort(internalAdapterId);
		return buf.array();
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		buf.getShort(internalAdapterId);
		final byte[] adapterIdBytes = new byte[bytes.length - 2];
		buf.get(adapterIdBytes);
		this.adapterId = new ByteArrayId(
				adapterIdBytes);
	}
}
