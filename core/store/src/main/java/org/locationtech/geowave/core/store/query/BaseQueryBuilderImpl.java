package org.locationtech.geowave.core.store.query;

import org.apache.commons.lang.ArrayUtils;
import org.locationtech.geowave.core.store.query.constraints.EverythingQuery;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;

public abstract class BaseQueryBuilderImpl<T, Q extends BaseQuery<T, ?>, R extends BaseQueryBuilder<T, Q, R>> implements
		BaseQueryBuilder<T, Q, R>
{
	protected String indexName = null;
	protected String[] authorizations = new String[0];
	protected double[] maxResolutionPerDimension = null;
	protected Integer limit = null;
	protected Integer maxRangeDecomposition = null;
	protected QueryConstraints constraints = new EverythingQuery();

	@Override
	public R allIndicies() {
		this.indexName = null;
		return (R) this;
	}

	@Override
	public R indexName(
			final String indexName ) {
		this.indexName = indexName;
		return (R) this;
	}

	@Override
	public R addAuthorization(
			final String authorization ) {
		ArrayUtils
				.add(
						authorizations,
						authorization);
		return (R) this;
	}

	@Override
	public R setAuthorizations(
			final String[] authorizations ) {
		this.authorizations = authorizations;
		return (R) this;
	}

	@Override
	public R noAuthorizations() {
		this.authorizations = new String[0];
		return (R) this;
	}

	@Override
	public R subsampling(
			final double[] maxResolutionPerDimension ) {
		this.maxResolutionPerDimension = maxResolutionPerDimension;
		return (R) this;
	}

	@Override
	public R noSubsampling() {
		maxResolutionPerDimension = null;
		return (R) this;
	}

	@Override
	public R noLimit() {
		limit = null;
		return (R) this;
	}

	@Override
	public R limit(
			final int limit ) {
		this.limit = limit;
		return (R) this;
	}

	@Override
	public R maxRanges(
			final int maxRangeDecomposition ) {
		this.maxRangeDecomposition = maxRangeDecomposition;
		return (R) this;
	}

	@Override
	public R noMaxRanges() {
		this.maxRangeDecomposition = -1;
		return (R) this;
	}

	@Override
	public R defaultMaxRanges() {
		this.maxRangeDecomposition = null;
		return (R) this;
	}

	@Override
	public R constraints(
			final QueryConstraints constraints ) {
		this.constraints = constraints;
		return (R) this;
	}

	protected CommonQueryOptions newCommonQueryOptions() {
		return new CommonQueryOptions(
				maxResolutionPerDimension,
				maxRangeDecomposition,
				limit,
				authorizations);
	}
}
