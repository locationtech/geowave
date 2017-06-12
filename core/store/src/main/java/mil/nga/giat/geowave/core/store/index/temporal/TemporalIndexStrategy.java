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
package mil.nga.giat.geowave.core.store.index.temporal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.lexicoder.Lexicoders;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.index.FieldIndexStrategy;

public class TemporalIndexStrategy implements
		FieldIndexStrategy<TemporalQueryConstraint, Date>
{
	private static final String ID = "TEMPORAL";

	public TemporalIndexStrategy() {
		super();
	}

	@Override
	public List<ByteArrayRange> getQueryRanges(
			final TemporalQueryConstraint indexedRange,
			final IndexMetaData... hints ) {
		return indexedRange.getRange();
	}

	@Override
	public List<ByteArrayRange> getQueryRanges(
			final TemporalQueryConstraint indexedRange,
			final int maxEstimatedRangeDecomposition,
			final IndexMetaData... hints ) {
		return getQueryRanges(indexedRange);
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			final List<FieldInfo<Date>> indexedData ) {
		final List<ByteArrayId> insertionIds = new ArrayList<>();
		for (final FieldInfo<Date> fieldInfo : indexedData) {
			insertionIds.add(new ByteArrayId(
					toIndexByte(fieldInfo.getDataValue().getValue())));
		}
		return insertionIds;
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			final List<FieldInfo<Date>> indexedData,
			final int maxEstimatedDuplicateIds ) {
		return getInsertionIds(indexedData);
	}

	@Override
	public List<FieldInfo<Date>> getRangeForId(
			final ByteArrayId insertionId ) {
		return Collections.emptyList();
	}

	@Override
	public String getId() {
		return ID;
	}

	@Override
	public byte[] toBinary() {
		return new byte[0];
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}

	public static final byte[] toIndexByte(
			final Date date ) {
		return Lexicoders.LONG.toByteArray(date.getTime());
	}

	@Override
	public Set<ByteArrayId> getNaturalSplits() {
		return null;
	}

	@Override
	public List<IndexMetaData> createMetaData() {
		return Collections.emptyList();
	}
}
