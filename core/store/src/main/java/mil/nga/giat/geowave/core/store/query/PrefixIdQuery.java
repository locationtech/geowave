/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.store.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.filter.PrefixIdQueryFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class PrefixIdQuery implements
		Query
{
	private final ByteArrayId sortKeyPrefix;
	private final ByteArrayId partitionKey;

	public PrefixIdQuery(
			final ByteArrayId partitionKey,
			final ByteArrayId sortKeyPrefix ) {
		this.partitionKey = partitionKey;
		this.sortKeyPrefix = sortKeyPrefix;
	}

	public ByteArrayId getPartitionKey() {
		return partitionKey;
	}

	public ByteArrayId getSortKeyPrefix() {
		return sortKeyPrefix;
	}

	@Override
	public List<QueryFilter> createFilters(
			final PrimaryIndex index ) {
		final List<QueryFilter> filters = new ArrayList<>();
		filters.add(new PrefixIdQueryFilter(
				partitionKey,
				sortKeyPrefix));
		return filters;
	}

	@Override
	public boolean isSupported(
			final Index index ) {
		return true;
	}

	@Override
	public List<MultiDimensionalNumericData> getIndexConstraints(
			final PrimaryIndex index ) {
		return Collections.emptyList();
	}

}
