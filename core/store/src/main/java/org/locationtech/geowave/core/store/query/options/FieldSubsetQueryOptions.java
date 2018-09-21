package org.locationtech.geowave.core.store.query.options;

import org.locationtech.geowave.core.store.api.DataTypeAdapter;

public class FieldSubsetQueryOptions<T> implements
		DataTypeQueryOptions<T>
{
	private final DataTypeAdapter<T> dataTypeAdapter;
	private final String[] fieldIds;

	public FieldSubsetQueryOptions(
			DataTypeAdapter<T> dataTypeAdapter,
			String[] fieldIds ) {
		this.dataTypeAdapter = dataTypeAdapter;
		this.fieldIds = fieldIds;
	}

	public DataTypeAdapter<T> getType() {
		return dataTypeAdapter;
	}

	public String[] getFieldIds() {
		return fieldIds;
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
