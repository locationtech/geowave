package mil.nga.giat.geowave.core.index;

import java.util.Arrays;

/**
 * This class is a wrapper around a byte array to ensure equals and hashcode
 * operations use the values of the bytes rather than explicit object identity
 */
public class ByteArrayId implements
		java.io.Serializable
{
	private final byte[] id;

	public ByteArrayId(
			final byte[] id ) {
		this.id = id;
	}

	public ByteArrayId(
			final String id ) {
		this.id = StringUtils.stringToBinary(id);
	}

	public byte[] getBytes() {
		return id;
	}

	public String getString() {
		return StringUtils.stringFromBinary(id);
	}

	@Override
	public String toString() {
		return "ByteArrayId [getString()=" + getString() + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + Arrays.hashCode(id);
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
		final ByteArrayId other = (ByteArrayId) obj;
		return Arrays.equals(
				id,
				other.id);
	}
}
