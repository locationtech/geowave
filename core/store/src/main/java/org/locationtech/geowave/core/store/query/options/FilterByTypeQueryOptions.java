package org.locationtech.geowave.core.store.query.options;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;

public class FilterByTypeQueryOptions<T> implements
		DataTypeQueryOptions<T>
{
	private ByteArrayId[] dataTypeAdapterIds;
	private DataTypeAdapter<T>[] dataTypeAdapters;

	protected FilterByTypeQueryOptions() {}

	public FilterByTypeQueryOptions(
			ByteArrayId[] dataTypeAdapterIds ) {
		this.dataTypeAdapterIds = dataTypeAdapterIds;
	}

	public FilterByTypeQueryOptions(
			DataTypeAdapter<T>[] dataTypeAdapters ) {
		this.dataTypeAdapters = dataTypeAdapters;
	}

	public ByteArrayId[] getDataTypeAdapterIds() {
		return dataTypeAdapterIds;
	}

	public DataTypeAdapter<T>[] getDataTypeAdapters() {
		return dataTypeAdapters;
	}

	@Override
	public byte[] toBinary() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {
		// TODO Auto-generated method stub

	}

}
