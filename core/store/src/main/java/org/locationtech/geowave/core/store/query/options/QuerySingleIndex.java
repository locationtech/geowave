package org.locationtech.geowave.core.store.query.options;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.store.api.Index;

public class QuerySingleIndex implements
		IndexQueryOptions
{
	private ByteArrayId indexId;
	private Index index;

	public QuerySingleIndex(
			ByteArrayId indexId,
			Index index ) {
		this.indexId = indexId;
		this.index = index;
	}

	public ByteArrayId getIndexId() {
		return indexId;
	}

	public Index getIndex() {
		return index;
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
