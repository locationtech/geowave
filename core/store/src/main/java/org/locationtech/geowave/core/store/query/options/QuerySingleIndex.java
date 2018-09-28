package org.locationtech.geowave.core.store.query.options;

public class QuerySingleIndex implements
		IndexQueryOptions
{
	private final String indexName;

	public QuerySingleIndex(
			final String indexName ) {
		this.indexName = indexName;
	}

	public String getIndexName() {
		return indexName;
	}

	@Override
	public byte[] toBinary() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		// TODO Auto-generated method stub

	}
}
