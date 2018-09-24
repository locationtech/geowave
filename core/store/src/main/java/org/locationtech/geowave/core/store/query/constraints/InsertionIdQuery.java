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
package org.locationtech.geowave.core.store.query.constraints;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.filter.InsertionIdQueryFilter;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class InsertionIdQuery implements
		QueryConstraints
{
	private final ByteArrayId partitionKey;
	private final ByteArrayId sortKey;
	private final ByteArrayId dataId;

	public InsertionIdQuery(
			final ByteArrayId partitionKey,
			final ByteArrayId sortKey,
			final ByteArrayId dataId ) {
		this.partitionKey = partitionKey;
		this.sortKey = sortKey;
		this.dataId = dataId;
	}

	public ByteArrayId getPartitionKey() {
		return partitionKey;
	}

	public ByteArrayId getSortKey() {
		return sortKey;
	}

	public ByteArrayId getDataId() {
		return dataId;
	}

	@Override
	public List<QueryFilter> createFilters(
			final Index index ) {
		final List<QueryFilter> filters = new ArrayList<>();
		filters.add(new InsertionIdQueryFilter(
				partitionKey,
				sortKey,
				dataId));
		return filters;
	}

	@Override
	public List<MultiDimensionalNumericData> getIndexConstraints(
			final Index index ) {
		return Collections.emptyList();
	}

}
