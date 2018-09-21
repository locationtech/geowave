package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;
import org.locationtech.geowave.core.store.query.options.DataTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.IndexQueryOptions;

public class Query<T> implements
		Persistable
{
	private CommonQueryOptions commonQueryOptions;
	private DataTypeQueryOptions<T> dataTypeQueryOptions;
	private IndexQueryOptions indexQueryOptions;
	private QueryConstraints queryConstraints;

	protected Query() {}

	public Query(
			CommonQueryOptions commonQueryOptions,
			DataTypeQueryOptions<T> dataTypeQueryOptions,
			IndexQueryOptions indexQueryOptions,
			QueryConstraints queryConstraints ) {
		this.commonQueryOptions = commonQueryOptions;
		this.dataTypeQueryOptions = dataTypeQueryOptions;
		this.indexQueryOptions = indexQueryOptions;
		this.queryConstraints = queryConstraints;
	}

	public CommonQueryOptions getCommonQueryOptions() {
		return commonQueryOptions;
	}

	public DataTypeQueryOptions<T> getDataTypeQueryOptions() {
		return dataTypeQueryOptions;
	}

	public IndexQueryOptions getIndexQueryOptions() {
		return indexQueryOptions;
	}

	public QueryConstraints getQueryConstraints() {
		return queryConstraints;
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
