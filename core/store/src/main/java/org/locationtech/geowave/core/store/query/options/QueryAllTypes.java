package org.locationtech.geowave.core.store.query.options;

public class QueryAllTypes<T> extends
		FilterByTypeQueryOptions<T>
{
	public QueryAllTypes(
			final String[] typeNames ) {
		super(
				null);
	}

	@Override
	public byte[] toBinary() {
		return new byte[] {};
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}

}
