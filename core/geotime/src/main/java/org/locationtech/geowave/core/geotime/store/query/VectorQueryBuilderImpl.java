package org.locationtech.geowave.core.geotime.store.query;

import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.store.query.QueryBuilderImpl;
import org.opengis.feature.simple.SimpleFeature;

public class VectorQueryBuilderImpl extends
		QueryBuilderImpl<SimpleFeature, VectorQueryBuilder> implements
		VectorQueryBuilder
{
}
