package mil.nga.giat.geowave.datastore.accumulo.encoding;

import java.util.List;

public class AccumuloDataSet
{
	private final List<AccumuloFieldInfo> fieldsRead;
	private final AccumuloUnreadData fieldsDeferred;

	public AccumuloDataSet(
			final List<AccumuloFieldInfo> fieldsRead,
			final AccumuloUnreadData fieldsDeferred ) {
		this.fieldsRead = fieldsRead;
		this.fieldsDeferred = fieldsDeferred;
	}

	public List<AccumuloFieldInfo> getFieldsRead() {
		return fieldsRead;
	}

	public AccumuloUnreadData getFieldsDeferred() {
		return fieldsDeferred;
	}
}
