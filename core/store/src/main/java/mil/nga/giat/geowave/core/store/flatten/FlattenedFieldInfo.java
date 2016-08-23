package mil.nga.giat.geowave.core.store.flatten;

public class FlattenedFieldInfo
{
	private final int fieldPosition;
	private final byte[] value;

	public FlattenedFieldInfo(
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
