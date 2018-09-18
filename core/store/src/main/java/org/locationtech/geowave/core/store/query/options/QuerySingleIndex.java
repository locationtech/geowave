package org.locationtech.geowave.core.store.query.options;

import org.locationtech.geowave.core.index.StringUtils;

public class QuerySingleIndex implements
		IndexQueryOptions
{
	private String indexName;

	public QuerySingleIndex() {
		this(
				null);
	}

	public QuerySingleIndex(
			final String indexName ) {
		this.indexName = indexName;
	}

	@Override
	public String getIndexName() {
		return indexName;
	}

	@Override
	public byte[] toBinary() {
		if ((indexName == null) || indexName.isEmpty()) {
			return new byte[0];
		}
		return StringUtils.stringToBinary(indexName);
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		if (bytes.length == 0) {
			indexName = null;
		}
		else {
			indexName = StringUtils.stringFromBinary(bytes);
		}

	}

	@Override
	public boolean isAllIndicies() {
		return indexName != null;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((indexName == null) ? 0 : indexName.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		QuerySingleIndex other = (QuerySingleIndex) obj;
		if (indexName == null) {
			if (other.indexName != null) return false;
		}
		else if (!indexName.equals(other.indexName)) return false;
		return true;
	}
}
