package org.locationtech.geowave.core.geotime.store.query.aggregate;

import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.util.IndexOptimizationUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.Index;
import org.threeten.extra.Interval;

public class OptimalVectorTimeRangeAggregation<P extends Persistable, T> extends
		BaseOptimalVectorAggregation<P, Interval, T>
{
	public OptimalVectorTimeRangeAggregation() {}

	public OptimalVectorTimeRangeAggregation(
			final FieldNameParam fieldNameParam ) {
		super(
				fieldNameParam);
	}

	@Override
	protected boolean isCommonIndex(
			final Index index,
			final GeotoolsFeatureDataAdapter adapter ) {
		// because field name param doesn't allow for multiple, ranges cannot be
		// set, field name param can be null in which case it can use a range,
		// or if field name is non-nul it must use a timestamp
		return ((fieldNameParam == null) || ((adapter.getTimeDescriptors().getTime() != null) && fieldNameParam
				.getFieldName()
				.equals(
						adapter.getTimeDescriptors().getTime().getLocalName()))) && IndexOptimizationUtils.hasTime(
				index,
				adapter);
	}

	@Override
	protected Aggregation<P, Interval, T> createCommonIndexAggregation() {
		return (Aggregation<P, Interval, T>) new CommonIndexTimeRangeAggregation<P>();
	}

	@Override
	protected Aggregation<P, Interval, T> createAggregation() {
		return (Aggregation<P, Interval, T>) new VectorTimeRangeAggregation(
				fieldNameParam);
	}
}
