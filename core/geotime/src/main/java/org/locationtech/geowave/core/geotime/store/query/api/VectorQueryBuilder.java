package org.locationtech.geowave.core.geotime.store.query.api;

import org.locationtech.geowave.core.geotime.store.query.BaseVectorQueryBuilder;
import org.locationtech.geowave.core.geotime.store.query.VectorQueryBuilderImpl;
import org.locationtech.geowave.core.geotime.store.query.VectorQueryConstraintsFactoryImpl;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.opengis.feature.simple.SimpleFeature;

/**
 * A QueryBuilder for vector (SimpleFeature) data. This should be preferred as
 * the mechanism for constructing a query in all cases when working with
 * SimpleFeature data.
 *
 */
public interface VectorQueryBuilder extends
		QueryBuilder<SimpleFeature, VectorQueryBuilder>,
		BaseVectorQueryBuilder<SimpleFeature, Query<SimpleFeature>, VectorQueryBuilder>
{
	static VectorQueryBuilder newBuilder() {
		return new VectorQueryBuilderImpl();
	}

	@Override
	default VectorQueryConstraintsFactory constraintsFactory() {
		return VectorQueryConstraintsFactoryImpl.SINGLETON_INSTANCE;
	}
}
