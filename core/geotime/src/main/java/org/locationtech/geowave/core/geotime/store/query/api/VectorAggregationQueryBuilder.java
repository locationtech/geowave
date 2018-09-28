package org.locationtech.geowave.core.geotime.store.query.api;

import org.locationtech.geowave.core.geotime.store.query.BaseVectorQueryBuilder;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.AggregationQuery;
import org.locationtech.geowave.core.store.api.AggregationQueryBuilder;
import org.opengis.feature.simple.SimpleFeature;

public interface VectorAggregationQueryBuilder<P extends Persistable, R extends Mergeable> extends
		AggregationQueryBuilder<P, R, SimpleFeature, VectorAggregationQueryBuilder<P, R>>,
		BaseVectorQueryBuilder<R, AggregationQuery<P, R, SimpleFeature>, VectorAggregationQueryBuilder<P, R>>
{
	// more convenience methods for aggregations
	// this will work if geo is part of common index or data adapter is provided
	VectorAggregationQueryBuilder<P,R> bboxOfResults();

	VectorAggregationQueryBuilder<P,R> timeRangeOfResults();
}
