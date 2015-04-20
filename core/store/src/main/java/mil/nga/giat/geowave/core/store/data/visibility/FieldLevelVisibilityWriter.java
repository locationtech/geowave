package mil.nga.giat.geowave.core.store.data.visibility;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

/**
 * Wrapper for a writer to extract the overriding column level visibility for
 * each specific attribute.
 * 
 * 
 * 
 * @param <T>
 * @param <CommonIndexValue>
 */
public class FieldLevelVisibilityWriter<T, CommonIndexValue> implements
		FieldWriter<T, CommonIndexValue>
{
	private final FieldWriter<T, CommonIndexValue> writer;

	private FieldVisibilityHandler<T, Object> fieldVisiblityHandler;

	public FieldLevelVisibilityWriter(
			final String fieldName,
			final FieldWriter<T, CommonIndexValue> writer,
			FieldVisibilityHandler<T, Object> fieldVisiblityHandler,
			final String visibilityAttributeName,
			final VisibilityManagement<T> visibilityManagement ) {
		super();
		this.writer = writer;
		this.fieldVisiblityHandler = visibilityManagement.createVisibilityHandler(
				fieldName,
				fieldVisiblityHandler,
				visibilityAttributeName);
	}

	@Override
	public byte[] getVisibility(
			T rowValue,
			ByteArrayId fieldId,
			CommonIndexValue fieldValue ) {

		return fieldVisiblityHandler.getVisibility(
				rowValue,
				fieldId,
				fieldValue);

	}

	@Override
	public byte[] writeField(
			CommonIndexValue fieldValue ) {
		if (writer == null) return null;
		return writer.writeField(fieldValue);
	}

}
