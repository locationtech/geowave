package org.locationtech.geowave.core.store.query.options;

import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.Aggregation;

public class AggregateTypeQueryOptions<P extends Persistable, R extends Mergeable, T> implements
		DataTypeQueryOptions<R>
{
	private String typeName;
	private Aggregation<P, R, T> aggregation;

	protected AggregateTypeQueryOptions() {}

	public AggregateTypeQueryOptions(
			String typeName,
			Aggregation<P, R, T> aggregation ) {
		this.typeName = typeName;
		this.aggregation = aggregation;
	}

	public String getTypeName() {
		return typeName;
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
