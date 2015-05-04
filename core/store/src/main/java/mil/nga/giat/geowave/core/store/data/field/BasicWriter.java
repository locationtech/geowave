package mil.nga.giat.geowave.core.store.data.field;

import mil.nga.giat.geowave.core.index.ByteArrayId;

/**
 * This class contains all of the primitive writer field types supported
 * 
 */
public class BasicWriter<RowType, FieldType> implements
		FieldWriter<RowType, FieldType>
{
	private FieldVisibilityHandler<RowType, Object> visibilityHandler;
	private FieldWriter<?, FieldType> writer;

	public BasicWriter(
			final FieldWriter<?, FieldType> writer ) {
		this(
				writer,
				null);
	}

	public BasicWriter(
			final FieldWriter<?, FieldType> writer,
			final FieldVisibilityHandler<RowType, Object> visibilityHandler ) {
		this.writer = writer;
		this.visibilityHandler = visibilityHandler;
	}

	@Override
	public byte[] getVisibility(
			final RowType rowValue,
			final ByteArrayId fieldId,
			final FieldType fieldValue ) {
		if (visibilityHandler != null) {
			return visibilityHandler.getVisibility(
					rowValue,
					fieldId,
					fieldValue);
		}
		return new byte[] {};
	}

	@Override
	public byte[] writeField(
			final FieldType fieldValue ) {
		return writer.writeField(fieldValue);
	}

}
