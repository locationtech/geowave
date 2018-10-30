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
package org.locationtech.geowave.core.store.base;

import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import org.locationtech.geowave.core.store.data.visibility.FieldVisibilityCount;
import org.locationtech.geowave.core.store.query.constraints.InsertionIdQuery;
import org.locationtech.geowave.core.store.query.filter.DedupeFilter;

import com.google.common.collect.Lists;

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
			final Index index,
			final InsertionIdQuery query,
			final ScanCallback<T, ?> scanCallback,
			final DedupeFilter dedupeFilter,
			final DifferingFieldVisibilityEntryCount differingVisibilityCounts,
			final FieldVisibilityCount visibilityCounts,
			final String[] authorizations ) {
		super(
				new short[] {
					adapter.getAdapterId()
				},
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
			final int maxRangeDecomposition,
			final double[] targetResolutionPerDimensionForHierarchicalIndex ) {
		return ranges;
	}
}
