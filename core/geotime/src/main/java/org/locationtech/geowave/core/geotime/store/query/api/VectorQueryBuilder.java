package org.locationtech.geowave.core.geotime.store.query.api;

import org.locationtech.geowave.core.geotime.store.query.BaseVectorQueryBuilder;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.opengis.feature.simple.SimpleFeature;

public interface VectorQueryBuilder extends
		QueryBuilder<SimpleFeature, VectorQueryBuilder>,
		BaseVectorQueryBuilder<SimpleFeature, Query<SimpleFeature>, VectorQueryBuilder>
{

	static VectorQueryBuilder newBuilder() {
		return null;
	}

}
