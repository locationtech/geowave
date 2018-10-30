/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.geotime.store.query;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.filter.CQLQueryFilter;
import org.locationtech.geowave.core.geotime.util.PropertyConstraintSet;
import org.locationtech.geowave.core.geotime.util.PropertyFilterVisitor;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.SecondaryIndexImpl;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.constraints.TypeConstraintQuery;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExplicitCQLQuery implements
		QueryConstraints,
		TypeConstraintQuery
{
	private final static Logger LOGGER = LoggerFactory.getLogger(ExplicitCQLQuery.class);
	private QueryConstraints baseQuery;
	private CQLQueryFilter filter;
	private Filter cqlFilter;

	public ExplicitCQLQuery() {}

	public ExplicitCQLQuery(
			final QueryConstraints baseQuery,
			final Filter filter,
			final GeotoolsFeatureDataAdapter adapter ) {
		// TODO consider ensuring the baseQuery amd the filter are in the
		// coordinate reference system of the adapter
		// only if the query has spatial predicate(s)
		this.baseQuery = baseQuery;
		cqlFilter = filter;
		this.filter = new CQLQueryFilter(
				filter,
				adapter);
	}

	@Override
	public List<QueryFilter> createFilters(
			final Index index ) {
		List<QueryFilter> queryFilters;
		// note, this assumes the CQL filter covers the baseQuery which *should*
		// be a safe assumption, otherwise we need to add the
		// baseQuery.createFilters to the list of query filters
		queryFilters = new ArrayList<>();
		if (filter != null) {
			queryFilters = new ArrayList<>(
					queryFilters);
			queryFilters.add(filter);
		}
		return queryFilters;
	}

	@Override
	public List<MultiDimensionalNumericData> getIndexConstraints(
			final Index index ) {
		if (baseQuery != null) {
			return baseQuery.getIndexConstraints(index);
		}
		return Collections.emptyList();
	}

	@Override
	public byte[] toBinary() {
		byte[] baseQueryBytes;
		if (baseQuery != null) {
			baseQueryBytes = PersistenceUtils.toBinary(baseQuery);
		}
		else {
			// base query can be null, no reason to log a warning
			baseQueryBytes = new byte[] {};
		}
		final byte[] filterBytes;
		if (filter != null) {
			filterBytes = filter.toBinary();
		}
		else {
			LOGGER.warn("Filter is null");
			filterBytes = new byte[] {};
		}

		final ByteBuffer buf = ByteBuffer.allocate(filterBytes.length + baseQueryBytes.length + 4);
		buf.putInt(filterBytes.length);
		buf.put(filterBytes);
		buf.put(baseQueryBytes);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int filterBytesLength = buf.getInt();
		final int baseQueryBytesLength = bytes.length - filterBytesLength - 4;
		if (filterBytesLength > 0) {
			final byte[] filterBytes = new byte[filterBytesLength];

			filter = new CQLQueryFilter();
			filter.fromBinary(filterBytes);
		}
		else {
			LOGGER.warn("CQL filter is empty bytes");
			filter = null;
		}
		if (baseQueryBytesLength > 0) {
			final byte[] baseQueryBytes = new byte[baseQueryBytesLength];

			try {
				baseQuery = (QueryConstraints) PersistenceUtils.fromBinary(baseQueryBytes);
			}
			catch (final Exception e) {
				throw new IllegalArgumentException(
						e);
			}
		}
		else {
			// base query can be null, no reason to log a warning
			baseQuery = null;
		}
	}

	public List<ByteArrayRange> getSecondaryIndexConstraints(
			final SecondaryIndexImpl<?> index ) {
		final PropertyFilterVisitor visitor = new PropertyFilterVisitor();
		final PropertyConstraintSet constraints = (PropertyConstraintSet) cqlFilter.accept(
				visitor,
				null);
		return constraints.getRangesFor(index);
	}

	public List<QueryFilter> getSecondaryQueryFilter(
			final SecondaryIndexImpl<?> index ) {
		final PropertyFilterVisitor visitor = new PropertyFilterVisitor();
		final PropertyConstraintSet constraints = (PropertyConstraintSet) cqlFilter.accept(
				visitor,
				null);
		return constraints.getFiltersFor(index);
	}

	@Override
	public String getTypeName() {
		return filter.getTypeName();
	}
}
