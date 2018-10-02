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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import org.locationtech.geowave.core.store.data.visibility.FieldVisibilityCount;
import org.locationtech.geowave.core.store.index.PrimaryIndex;

/**
 * Represents a query operation using an Accumulo row prefix.
 *
 */
class BaseRowPrefixQuery<T> extends
		AbstractBaseRowQuery<T>
{
	final QueryRanges queryRanges;

	public BaseRowPrefixQuery(
			final PrimaryIndex index,
			final ByteArrayId partitionKey,
			final ByteArrayId sortKeyPrefix,
			final ScanCallback<T, ?> scanCallback,
			final DifferingFieldVisibilityEntryCount differingVisibilityCounts,
			final FieldVisibilityCount visibilityCounts,
			final String[] authorizations ) {
		super(
				index,
				authorizations,
				scanCallback,
				differingVisibilityCounts,
				visibilityCounts);

		final ByteArrayRange sortKeyPrefixRange = new ByteArrayRange(
				sortKeyPrefix,
				sortKeyPrefix,
				false);
		final List<SinglePartitionQueryRanges> ranges = new ArrayList<SinglePartitionQueryRanges>();
		final Collection<ByteArrayRange> sortKeys = Collections.singleton(sortKeyPrefixRange);
		ranges.add(new SinglePartitionQueryRanges(
				partitionKey,
				sortKeys));
		queryRanges = new QueryRanges(
				ranges);
	}

	@Override
	protected QueryRanges getRanges(
			int maxRangeDecomposition ) {
		return queryRanges;
	}

}
