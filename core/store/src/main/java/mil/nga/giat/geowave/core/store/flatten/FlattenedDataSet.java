package mil.nga.giat.geowave.core.store.flatten;

import java.util.List;

public class FlattenedDataSet
{
	private final List<FlattenedFieldInfo> fieldsRead;
	private final FlattenedUnreadData fieldsDeferred;

	public FlattenedDataSet(
			final List<FlattenedFieldInfo> fieldsRead,
			final FlattenedUnreadData fieldsDeferred ) {
		this.fieldsRead = fieldsRead;
		this.fieldsDeferred = fieldsDeferred;
	}

	public List<FlattenedFieldInfo> getFieldsRead() {
		return fieldsRead;
	}

	public FlattenedUnreadData getFieldsDeferred() {
		return fieldsDeferred;
	}
}
