package mil.nga.giat.geowave.core.store.data.visibility;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;

/**
 * Basic implementation of a visibility handler to allow all access
 * 
 * @param <RowType>
 * @param <FieldType>
 */
public class UnconstrainedVisibilityHandler<RowType, FieldType> implements
		FieldVisibilityHandler<RowType, FieldType>
{

	@Override
	public byte[] getVisibility(
			final RowType rowValue,
			final ByteArrayId fieldId,
			final FieldType fieldValue ) {
		return new byte[0];
	}

}
