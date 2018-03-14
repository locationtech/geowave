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

import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;

public class CoordinateRangeQuery implements
		DistributableQuery
{
	private NumericIndexStrategy indexStrategy;
	private MultiDimensionalCoordinateRangesArray[] coordinateRanges;

	@Override
	public List<QueryFilter> createFilters(
			final PrimaryIndex index ) {
		return Collections.singletonList(new CoordinateRangeQueryFilter(
				indexStrategy,
				coordinateRanges));
	}

	@Override
	public boolean isSupported(
			final Index<?, ?> index ) {
		return index.getIndexStrategy().equals(
				indexStrategy);
	}

	@Override
	public List<MultiDimensionalNumericData> getIndexConstraints(
			final PrimaryIndex index ) {
		// TODO should we consider implementing this?
		return Collections.EMPTY_LIST;
	}

	@Override
	public byte[] toBinary() {
		return new CoordinateRangeQueryFilter(
				indexStrategy,
				coordinateRanges).toBinary();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final CoordinateRangeQueryFilter filter = new CoordinateRangeQueryFilter();
		filter.fromBinary(bytes);
		indexStrategy = filter.indexStrategy;
		coordinateRanges = filter.coordinateRanges;
	}

	@Override
	public List<ByteArrayRange> getSecondaryIndexConstraints(
			final SecondaryIndex<?> index ) {
		// TODO should we consider implementing this?
		return null;
	}

	@Override
	public List<DistributableQueryFilter> getSecondaryQueryFilter(
			final SecondaryIndex<?> index ) {
		// TODO should we consider implementing this?
		return null;
	}
}
