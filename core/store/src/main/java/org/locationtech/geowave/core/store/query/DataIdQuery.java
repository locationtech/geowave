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
package org.locationtech.geowave.core.store.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.filter.DataIdQueryFilter;
import org.locationtech.geowave.core.store.filter.QueryFilter;
import org.locationtech.geowave.core.store.index.PrimaryIndex;

public class DataIdQuery implements
		Query
{
	private final List<ByteArrayId> dataIds;

	public DataIdQuery(
			final ByteArrayId dataId ) {
		dataIds = Collections.singletonList(dataId);
	}

	public DataIdQuery(
			final List<ByteArrayId> dataIds ) {
		this.dataIds = new ArrayList<>(
				dataIds);
	}

	public List<ByteArrayId> getDataIds() {
		return dataIds;
	}

	@Override
	public List<QueryFilter> createFilters(
			final PrimaryIndex index ) {
		final List<QueryFilter> filters = new ArrayList<>();
		filters.add(new DataIdQueryFilter(
				dataIds));
		return filters;
	}

	@Override
	public List<MultiDimensionalNumericData> getIndexConstraints(
			final PrimaryIndex index ) {
		return Collections.emptyList();
	}

}
