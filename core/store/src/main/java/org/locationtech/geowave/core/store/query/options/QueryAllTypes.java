package org.locationtech.geowave.core.store.query.options;

public class QueryAllTypes<T> implements
		DataTypeQueryOptions<T>
{
	@Override
	public byte[] toBinary() {
		return new byte[] {};
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {}

}
