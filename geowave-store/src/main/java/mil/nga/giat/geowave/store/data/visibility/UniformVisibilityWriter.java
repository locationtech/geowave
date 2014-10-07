package mil.nga.giat.geowave.store.data.visibility;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.data.VisibilityWriter;
import mil.nga.giat.geowave.store.data.field.FieldVisibilityHandler;

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
