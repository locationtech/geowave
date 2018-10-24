package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.query.BaseQueryBuilder;
import org.locationtech.geowave.core.store.query.aggregate.AggregationQueryBuilderImpl;

/**
 * This and its extensions should be used to create an AggregationQuery. 
 *
 * @param <P>
 *            input type for the aggregation
 * @param <R>
 *            result type for the aggregation
 * @param <T>
 *            data type of the entries for the aggregation
 * @param <A>
 *            the type of the builder, useful for extending this builder and
 *            maintaining the builder type
 */
public interface AggregationQueryBuilder<P extends Persistable, R, T, A extends AggregationQueryBuilder<P, R, T, A>>
		extends
		BaseQueryBuilder<R, AggregationQuery<P, R, T>, A>
{
	/**
	 * Provide the Aggregation function and the type name to apply the aggregation on
	 * @param typeName the type name of the dataset
	 * @param aggregation the aggregation function
	 * @return an aggregation
	 */
	A aggregate(
			String typeName,
			Aggregation<P, R, T> aggregation );

	/**
	 * this is a convenience method to set the count aggregation
	 * if no type names are given it is assumed to count every type
	 * @param typeNames the type names to count results
	 * @return a count of how many entries match the query criteria
	 */
	A count(
			String... typeNames );

	/**
	 * get a new default implementation of the builder
	 * @return an AggregationQueryBuilder
	 */
	static <P extends Persistable, R, T, A extends AggregationQueryBuilder<P, R, T, A>> AggregationQueryBuilder<P, R, T, A> newBuilder() {
		return new AggregationQueryBuilderImpl<>();
	}
}
