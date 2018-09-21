package org.locationtech.geowave.core.store.query.options;

import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;

public class AggregateTypeQueryOptions<P extends Persistable, R extends Mergeable, T> implements
		DataTypeQueryOptions<R>
{
	private DataTypeAdapter<T> adapter;
	private Aggregation<P, R, T> aggregation;

	protected AggregateTypeQueryOptions() {}

	public AggregateTypeQueryOptions(
			DataTypeAdapter<T> adapter,
			Aggregation<P, R, T> aggregation ) {
		this.adapter = adapter;
		this.aggregation = aggregation;
	}

	public DataTypeAdapter<T> getType() {
		return adapter;
	}

	public Aggregation<P, R, T> getAggregation() {
		return aggregation;
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
