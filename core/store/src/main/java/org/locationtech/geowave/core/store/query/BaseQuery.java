package org.locationtech.geowave.core.store.query;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;
import org.locationtech.geowave.core.store.query.options.DataTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.IndexQueryOptions;

public abstract class BaseQuery<T, O extends DataTypeQueryOptions<T>> implements
		Persistable
{
	private CommonQueryOptions commonQueryOptions;
	private O dataTypeQueryOptions;
	private IndexQueryOptions indexQueryOptions;
	private QueryConstraints queryConstraints;

	protected BaseQuery() {}

	public BaseQuery(
			CommonQueryOptions commonQueryOptions,
			O dataTypeQueryOptions,
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

	public O getDataTypeQueryOptions() {
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
