package mil.nga.giat.geowave.datastore.accumulo.encoding;

public class AccumuloFieldInfo
{
	private final int fieldPosition;
	private final byte[] value;

	public AccumuloFieldInfo(
			final int fieldPosition,
			final byte[] value ) {
		this.fieldPosition = fieldPosition;
		this.value = value;
	}

	public int getFieldPosition() {
		return fieldPosition;
	}

	public byte[] getValue() {
		return value;
	}
}
