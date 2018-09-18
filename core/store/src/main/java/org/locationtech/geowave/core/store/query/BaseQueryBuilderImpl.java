package org.locationtech.geowave.core.store.query;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.locationtech.geowave.core.store.query.constraints.EverythingQuery;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions.HintKey;
import org.locationtech.geowave.core.store.query.options.IndexQueryOptions;
import org.locationtech.geowave.core.store.query.options.QuerySingleIndex;

public abstract class BaseQueryBuilderImpl<T, Q extends BaseQuery<T, ?>, R extends BaseQueryBuilder<T, Q, R>> implements
		BaseQueryBuilder<T, Q, R>
{
	protected String indexName = null;
	protected String[] authorizations = new String[0];
	protected Integer limit = null;
	protected Map<HintKey<?>, Object> hints = new HashMap<>();
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
		authorizations = (String[]) ArrayUtils.add(
				authorizations,
				authorization);
		return (R) this;
	}

	@Override
	public R setAuthorizations(
			final String[] authorizations ) {
		if (authorizations == null) {
			this.authorizations = new String[0];
		}
		else {
			this.authorizations = authorizations;
		}
		return (R) this;
	}

	@Override
	public R noAuthorizations() {
		this.authorizations = new String[0];
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
	public <HintValueType> R addHint(
			final HintKey<HintValueType> key,
			final HintValueType value ) {
		this.hints.put(
				key,
				value);
		return (R) this;
	}

	@Override
	public R noHints() {
		hints.clear();
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
				limit,
				hints,
				authorizations);
	}

	protected IndexQueryOptions newIndexQueryOptions() {
		return new QuerySingleIndex(
				indexName);
	}
}
