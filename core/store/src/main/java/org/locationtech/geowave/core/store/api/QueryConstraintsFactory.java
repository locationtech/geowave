package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery.Constraints;
import org.locationtech.geowave.core.store.query.constraints.CoordinateRangeQuery;
import org.locationtech.geowave.core.store.query.constraints.DataIdQuery;
import org.locationtech.geowave.core.store.query.constraints.EverythingQuery;
import org.locationtech.geowave.core.store.query.constraints.PrefixIdQuery;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.filter.BasicQueryFilter.BasicQueryCompareOperation;

public class QueryConstraintsFactory
{

	public static QueryConstraints dataIds(
			ByteArrayId[] dataIds ) {
		return new DataIdQuery(
				dataIds);
	}

	public static QueryConstraints prefix(
			ByteArrayId partitionKey,
			ByteArrayId sortKeyPrefix ) {
		return new PrefixIdQuery(partitionKey, sortKeyPrefix);
	}

	public static QueryConstraints coordinateRanges(
			NumericIndexStrategy indexStrategy,
			MultiDimensionalCoordinateRangesArray[] coordinateRanges ) {
		return new CoordinateRangeQuery(
				indexStrategy,
				coordinateRanges);
	}

	public static QueryConstraints constraints(
			Constraints constraints ) {
		return new BasicQuery(
				constraints);
	}

	public static QueryConstraints constraints(
			Constraints constraints,
			BasicQueryCompareOperation compareOp ) {
		return new BasicQuery(
				constraints,
				compareOp);
	}

	public static QueryConstraints noConstraints() {
		return new EverythingQuery();
	}

}
