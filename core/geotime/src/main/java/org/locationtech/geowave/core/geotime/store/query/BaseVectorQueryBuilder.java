package org.locationtech.geowave.core.geotime.store.query;

import java.util.Date;

import javax.annotation.Nullable;

import org.locationtech.geowave.core.geotime.store.query.filter.SpatialQueryFilter.CompareOperation;
import org.locationtech.geowave.core.store.query.BaseQuery;
import org.opengis.filter.Filter;

import com.vividsolutions.jts.geom.Geometry;

public interface BaseVectorQueryBuilder<T, Q extends BaseQuery<T, ?>, R extends BaseVectorQueryBuilder<T, Q, R>>
{

}
