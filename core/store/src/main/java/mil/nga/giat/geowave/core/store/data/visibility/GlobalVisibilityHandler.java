package mil.nga.giat.geowave.core.store.data.visibility;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;

/**
 * Basic implementation of a visibility handler where the decision of visibility
 * is not determined on a per field or even per row basis, but rather a single
 * visibility is globally assigned for every field written.
 * 
 * @param <RowType>
 * @param <FieldType>
 */
public class GlobalVisibilityHandler<RowType, FieldType> implements
		FieldVisibilityHandler<RowType, FieldType>
{
	private final String globalVisibility;

	public GlobalVisibilityHandler(
			final String globalVisibility ) {
		this.globalVisibility = globalVisibility;
	}

	@Override
	public byte[] getVisibility(
			final RowType rowValue,
			final ByteArrayId fieldId,
			final FieldType fieldValue ) {
		return StringUtils.stringToBinary(globalVisibility);
	}

}
