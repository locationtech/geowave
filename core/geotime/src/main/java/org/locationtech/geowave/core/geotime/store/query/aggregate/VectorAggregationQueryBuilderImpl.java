package org.locationtech.geowave.core.geotime.store.query.aggregate;

import org.locationtech.geowave.core.geotime.store.query.api.VectorAggregationQueryBuilder;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.query.aggregate.AggregationQueryBuilderImpl;
import org.locationtech.geowave.core.store.query.options.AggregateTypeQueryOptions;
import org.opengis.feature.simple.SimpleFeature;

public class VectorAggregationQueryBuilderImpl<P extends Persistable, R> extends
		AggregationQueryBuilderImpl<P, R, SimpleFeature, VectorAggregationQueryBuilder<P, R>> implements
		VectorAggregationQueryBuilder<P, R>
{

	@Override
	public VectorAggregationQueryBuilder<P, R> bboxOfResults(
			final String... typeNames ) {
		options = new AggregateTypeQueryOptions(
				new OptimalVectorBoundingBoxAggregation(),
				typeNames);
		return this;
	}

	@Override
	public VectorAggregationQueryBuilder<P, R> bboxOfResultsForGeometryField(
			final String typeName,
			final String geomFieldName ) {
		options = new AggregateTypeQueryOptions(
				new OptimalVectorBoundingBoxAggregation<>(
						new FieldNameParam(
								geomFieldName)),
				typeName);
		return this;
	}

	@Override
	public VectorAggregationQueryBuilder<P, R> timeRangeOfResults(
			final String... typeNames ) {
		options = new AggregateTypeQueryOptions(
				new VectorTimeRangeAggregation(),
				typeNames);
		return this;
	}

	@Override
	public VectorAggregationQueryBuilder<P, R> timeRangeOfResultsForTimeField(
			final String typeName,
			final String timeFieldName ) {
		options = new AggregateTypeQueryOptions(
				new VectorBoundingBoxAggregation(
						new FieldNameParam(
								timeFieldName)),
				typeName);
		return this;
	}
}
