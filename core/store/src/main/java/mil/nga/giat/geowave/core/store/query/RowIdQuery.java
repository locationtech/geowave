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
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.filter.RowIdQueryFilter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class RowIdQuery implements
		Query
{
	private final List<ByteArrayId> rowIds;

	public RowIdQuery(
			final ByteArrayId rowId ) {
		rowIds = Collections.singletonList(rowId);
	}

	public RowIdQuery(
			final List<ByteArrayId> rowIds ) {
		this.rowIds = new ArrayList<>(
				rowIds);
	}

	public List<ByteArrayId> getRowIds() {
		return rowIds;
	}

	@Override
	public List<QueryFilter> createFilters(
			final PrimaryIndex index ) {
		final List<QueryFilter> filters = new ArrayList<>();
		filters.add(new RowIdQueryFilter(
				rowIds));
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
