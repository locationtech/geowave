package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery.Constraints;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.filter.BasicQueryFilter.BasicQueryCompareOperation;

/**
 * This is a simple mechanism to create existing supported query constraints.
 *
 */
public interface QueryConstraintsFactory
{
	/**
	 * constrain a query by data IDs
	 *
	 * @param dataIds
	 *            the data IDs to constrain to
	 * @return the constraints
	 */
	QueryConstraints dataIds(
			final ByteArray... dataIds );

	/**
	 * constrain a query by prefix
	 *
	 * @param partitionKey
	 *            the prefix
	 * @param sortKeyPrefix
	 *            the sort prefix
	 * @return the constraints
	 */
	QueryConstraints prefix(
			final ByteArray partitionKey,
			final ByteArray sortKeyPrefix );

	/**
	 * constrain by coordinate ranges
	 *
	 * @param indexStrategy
	 *            the index strategy
	 * @param coordinateRanges
	 *            the coordinate ranges
	 * @return the constraints
	 */
	QueryConstraints coordinateRanges(
			final NumericIndexStrategy indexStrategy,
			final MultiDimensionalCoordinateRangesArray[] coordinateRanges );

	/**
	 * constrain generally by constraints
	 *
	 * @param constraints
	 *            the constraints
	 * @return the query constraints
	 */
	QueryConstraints constraints(
			final Constraints constraints );

	/**
	 * constrain generally by constraints with a compare operation
	 *
	 * @param constraints
	 *            the constraints
	 * @param compareOp
	 *            the relationship to use for comparison
	 * @return the query constraints
	 */
	QueryConstraints constraints(
			final Constraints constraints,
			final BasicQueryCompareOperation compareOp );

	/**
	 * no query constraints, meaning wide open query (this is the default)
	 *
	 * @return the query constraints
	 */
	QueryConstraints noConstraints();
}
