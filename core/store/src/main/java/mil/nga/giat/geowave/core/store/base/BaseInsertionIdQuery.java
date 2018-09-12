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
package mil.nga.giat.geowave.core.store.base;

import java.util.Collections;

import com.google.common.collect.Lists;

import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapter;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.data.visibility.FieldVisibilityCount;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.InsertionIdQuery;

/**
 * Represents a query operation for a specific set of row IDs.
 *
 */
class BaseInsertionIdQuery<T> extends
		BaseConstraintsQuery
{
	private final QueryRanges ranges;

	public BaseInsertionIdQuery(
			final InternalDataAdapter<?> adapter,
			final PrimaryIndex index,
			final InsertionIdQuery query,
			final ScanCallback<T, ?> scanCallback,
			final DedupeFilter dedupeFilter,
			final DifferingFieldVisibilityEntryCount differingVisibilityCounts,
			final FieldVisibilityCount visibilityCounts,
			final String[] authorizations ) {
		super(
				Collections.<Short> singletonList(adapter.getInternalAdapterId()),
				index,
				query,
				dedupeFilter,
				scanCallback,
				null,
				null,
				null,
				null,
				differingVisibilityCounts,
				visibilityCounts,
				authorizations);
		this.ranges = new InsertionIds(
				query.getPartitionKey(),
				Lists.newArrayList(query.getSortKey())).asQueryRanges();
	}

	@Override
	protected QueryRanges getRanges(
			int maxRangeDecomposition ) {
		return ranges;
	}
}
