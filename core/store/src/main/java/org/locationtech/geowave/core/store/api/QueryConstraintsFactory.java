package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery.Constraints;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.filter.BasicQueryFilter.BasicQueryCompareOperation;

public interface QueryConstraintsFactory
{
	QueryConstraints dataIds(
			final ByteArrayId... dataIds );

	QueryConstraints prefix(
			final ByteArrayId partitionKey,
			final ByteArrayId sortKeyPrefix );

	QueryConstraints coordinateRanges(
			final NumericIndexStrategy indexStrategy,
			final MultiDimensionalCoordinateRangesArray[] coordinateRanges );

	QueryConstraints constraints(
			final Constraints constraints );

	QueryConstraints constraints(
			final Constraints constraints,
			final BasicQueryCompareOperation compareOp );

	QueryConstraints noConstraints();

}
