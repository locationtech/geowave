package org.locationtech.geowave.core.geotime.store.query.api;

import org.locationtech.geowave.core.geotime.store.query.BaseVectorQueryBuilder;
import org.locationtech.geowave.core.geotime.store.query.VectorQueryConstraintsFactoryImpl;
import org.locationtech.geowave.core.geotime.store.query.aggregate.VectorAggregationQueryBuilderImpl;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.AggregationQuery;
import org.locationtech.geowave.core.store.api.AggregationQueryBuilder;
import org.opengis.feature.simple.SimpleFeature;

/**
 * An aggregation query builder particular for vector data. This should be
 * preferentially used to build AggregationQuery's for SimpleFeature data.
 *
 * @param <P>
 *            the type for input parameters
 * @param <R>
 *            the result type
 */
public interface VectorAggregationQueryBuilder<P extends Persistable, R> extends
		AggregationQueryBuilder<P, R, SimpleFeature, VectorAggregationQueryBuilder<P, R>>,
		BaseVectorQueryBuilder<R, AggregationQuery<P, R, SimpleFeature>, VectorAggregationQueryBuilder<P, R>>
{
	@Override
	default VectorQueryConstraintsFactory constraintsFactory() {
		return VectorQueryConstraintsFactoryImpl.SINGLETON_INSTANCE;
	}

	/**
	 * get a default implementation of this builder
	 *
	 * @return the builder
	 */
	static <P extends Persistable, R> VectorAggregationQueryBuilder<P, R> newBuilder() {
		return new VectorAggregationQueryBuilderImpl<>();
	}

	/**
	 * convenience method for getting a bounding box of the results of a query.
	 * It uses the default geometry for a feature type which is also the indexed
	 * geometry.
	 *
	 * @param typeNames
	 *            the type names to constrain by
	 * @return this builder
	 */
	VectorAggregationQueryBuilder<P, R> bboxOfResults(
			String... typeNames );

	/**
	 *
	 * convenience method for getting a bounding box of the results of a query
	 * this can be particularly useful if you want to calculate the bbox on a
	 * different field than the default/indexed Geometry
	 *
	 * @param typeName
	 *            the type name
	 * @param geomAttributeName
	 *            the geometry attribute name
	 * @return this builder
	 */
	VectorAggregationQueryBuilder<P, R> bboxOfResultsForGeometryField(
			String typeName,
			String geomAttributeName );

	/**
	 * convenience method for getting a time range of the results of a query.
	 * this has to use inferred or hinted temporal attribute names if uncertain
	 * what implicit time attributes are being used then explicitly set
	 * timeAttributeName in other method
	 *
	 * @param typeNames
	 *            the type names to constrain by
	 * @return this builder
	 */
	VectorAggregationQueryBuilder<P, R> timeRangeOfResults(
			String... typeNames );

	/**
	 *
	 * convenience method for getting a time range of the results of a query
	 * this can be particularly useful if you want to calculate the time range
	 * on a specific time field
	 *
	 * @param typeName
	 *            the type names to constrain by
	 * @param timeAttributeName
	 *            the time attribute name
	 * @return this builder
	 */
	VectorAggregationQueryBuilder<P, R> timeRangeOfResultsForTimeField(
			String typeName,
			String timeAttributeName );
}
