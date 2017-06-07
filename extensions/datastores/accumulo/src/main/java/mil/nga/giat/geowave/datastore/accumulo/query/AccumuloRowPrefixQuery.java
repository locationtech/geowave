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
package mil.nga.giat.geowave.datastore.accumulo.query;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

/**
 * Represents a query operation using an Accumulo row prefix.
 *
 */
public class AccumuloRowPrefixQuery<T> extends
		AbstractAccumuloRowQuery<T>
{

	final Integer limit;
	final ByteArrayId rowPrefix;

	public AccumuloRowPrefixQuery(
			final PrimaryIndex index,
			final ByteArrayId rowPrefix,
			final ScanCallback<T> scanCallback,
			final Integer limit,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String[] authorizations ) {
		super(
				index,
				authorizations,
				scanCallback,
				visibilityCounts);
		this.limit = limit;
		this.rowPrefix = rowPrefix;
	}

	@Override
	protected Integer getScannerLimit() {
		return limit;
	}

	@Override
	protected List<ByteArrayRange> getRanges() {
		final List<ByteArrayRange> ranges = new ArrayList<ByteArrayRange>();
		ranges.add(new ByteArrayRange(
				rowPrefix,
				rowPrefix,
				false));
		return ranges;
	}

}
