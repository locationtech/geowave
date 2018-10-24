package org.locationtech.geowave.core.store.query;

import org.apache.commons.lang.ArrayUtils;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.query.options.FilterByTypeQueryOptions;

public class QueryBuilderImpl<T, R extends QueryBuilder<T, R>> extends
		BaseQueryBuilderImpl<T, Query<T>, R> implements
		QueryBuilder<T, R>
{
	protected String[] typeNames = new String[0];
	protected String[] fieldNames = null;

	@Override
	public R allTypes() {
		this.typeNames = new String[0];
		return (R) this;
	}

	@Override
	public R addTypeName(
			final String typeName ) {
		if ((fieldNames == null) || (fieldNames.length == 0)) {
			typeNames = (String[]) ArrayUtils.add(
					typeNames,
					typeName);
		}
		else {
			throw new IllegalStateException(
					"Subsetting fields only allows for a single type name");
		}
		return (R) this;
	}

	@Override
	public R setTypeNames(
			final String[] typeNames ) {
		if ((fieldNames == null) || (fieldNames.length == 0)) {
			if (typeNames == null) {
				return allTypes();
			}
			this.typeNames = typeNames;
		}
		else if (typeNames == null || typeNames.length != 1) {
			throw new IllegalStateException(
					"Subsetting fields only allows for a single type name");
		}
		else {
			// we assume the user knows what they're doing and is choosing to
			// override the current type name with this
			this.typeNames = typeNames;
		}
		return (R) this;
	}

	@Override
	public R subsetFields(
			final String typeName,
			final String... fieldNames ) {
		this.typeNames = new String[] {
			typeName
		};
		this.fieldNames = fieldNames;
		return (R) this;
	}

	@Override
	public R allFields() {
		this.fieldNames = null;
		return (R) this;
	}

	protected FilterByTypeQueryOptions<T> newFilterByTypeQueryOptions() {
		return typeNames.length == 1 ? new FilterByTypeQueryOptions<>(
				typeNames[0],
				fieldNames) : new FilterByTypeQueryOptions<>(
				typeNames);
	}

	@Override
	public Query<T> build() {
		return new Query<>(
				newCommonQueryOptions(),
				newFilterByTypeQueryOptions(),
				newIndexQueryOptions(),
				constraints);
	}
}
