package org.locationtech.geowave.core.store.query.options;

public class QueryAllIndices extends
		QuerySingleIndex
{

	public QueryAllIndices() {
		super(
				null);
	}

	@Override
	public byte[] toBinary() {
		return new byte[0];
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
