package org.locationtech.geowave.core.geotime.store.query.aggregate;

import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.util.IndexOptimizationUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.Index;

import com.vividsolutions.jts.geom.Envelope;

public class OptimalVectorBoundingBoxAggregation<P extends Persistable, T> extends
		BaseOptimalVectorAggregation<P, Envelope, T>
{
	public OptimalVectorBoundingBoxAggregation() {}

	public OptimalVectorBoundingBoxAggregation(
			final FieldNameParam fieldNameParam ) {
		super(
				fieldNameParam);
	}

	@Override
	protected boolean isCommonIndex(
			final Index index,
			final GeotoolsFeatureDataAdapter adapter ) {
		return fieldNameParam.getFieldName().equals(
				adapter.getFeatureType().getGeometryDescriptor().getLocalName())
				&& IndexOptimizationUtils.hasAtLeastSpatial(index);
	}

	@Override
	protected Aggregation<P, Envelope, T> createCommonIndexAggregation() {
		return (Aggregation<P, Envelope, T>) new CommonIndexBoundingBoxAggregation<P>();
	}

	@Override
	protected Aggregation<P, Envelope, T> createAggregation() {
		return (Aggregation<P, Envelope, T>) new VectorBoundingBoxAggregation(
				fieldNameParam);
	}
}
