package org.locationtech.geowave.core.geotime.store.query.api;

import org.locationtech.geowave.core.geotime.store.query.BaseVectorQueryBuilder;
import org.locationtech.geowave.core.geotime.store.query.VectorQueryConstraintsFactoryImpl;
import org.locationtech.geowave.core.geotime.store.query.aggregate.VectorAggregationQueryBuilderImpl;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.AggregationQuery;
import org.locationtech.geowave.core.store.api.AggregationQueryBuilder;
import org.opengis.feature.simple.SimpleFeature;

public interface VectorAggregationQueryBuilder<P extends Persistable, R> extends
		AggregationQueryBuilder<P, R, SimpleFeature, VectorAggregationQueryBuilder<P, R>>,
		BaseVectorQueryBuilder<R, AggregationQuery<P, R, SimpleFeature>, VectorAggregationQueryBuilder<P, R>>
{
	@Override
	default VectorQueryConstraintsFactory constraintsFactory() {
		return VectorQueryConstraintsFactoryImpl.SINGLETON_INSTANCE;
	}
	
	 static <P extends Persistable,R> VectorAggregationQueryBuilder<P, R> newBuilder(){
		return new VectorAggregationQueryBuilderImpl<>();
	}

	// more convenience methods for aggregations
	// envelope will be in indexed CRS
	// uses default geometry
	VectorAggregationQueryBuilder<P, R> bboxOfResults(
			String... typeNames );

	// this can be particularly useful if you want to calculate the bbox on a
	// different field than the indexed Geometry
	VectorAggregationQueryBuilder<P, R> bboxOfResultsForGeometryField(
			String typeName,
			String geomAttributeName );

	// this has to use inferred or hinted temporal attribute names
	// if uncertain what implicit time attributes are being used
	// then explicitly set timeAttributeName in other method
	VectorAggregationQueryBuilder<P, R> timeRangeOfResults(
			String... typeNames );

	VectorAggregationQueryBuilder<P, R> timeRangeOfResultsForTimeField(
			String typeName,
			String timeAttributeName );
}
