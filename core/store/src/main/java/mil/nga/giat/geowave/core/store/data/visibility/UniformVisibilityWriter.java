package mil.nga.giat.geowave.core.store.data.visibility;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;

/**
 * All fields use the same visibility writer
 */

public class UniformVisibilityWriter<RowType> implements
		VisibilityWriter<RowType>
{

	final FieldVisibilityHandler<RowType, Object> uniformHandler;

	public UniformVisibilityWriter(
			FieldVisibilityHandler<RowType, Object> uniformHandler ) {
		super();
		this.uniformHandler = uniformHandler;
	}

	@Override
	public FieldVisibilityHandler<RowType, Object> getFieldVisibilityHandler(
			ByteArrayId fieldId ) {

		return uniformHandler;
	}

}
