package org.locationtech.geowave.core.store.query.options;

public class QueryAllTypes<T> extends
		FilterByTypeQueryOptions<T>
{
	public QueryAllTypes() {
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

	@Override
	public int hashCode() {
		return getClass().hashCode();
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (obj == null) {
			return false;
		}
		return getClass() == obj.getClass();
	}
}
