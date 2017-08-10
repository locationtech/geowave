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
package mil.nga.giat.geowave.core.store.index.numeric;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.lexicoder.Lexicoders;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.index.FieldIndexStrategy;

public class NumericFieldIndexStrategy implements
		FieldIndexStrategy<NumericQueryConstraint, Number>
{
	private static final String ID = "NUMERIC";

	public NumericFieldIndexStrategy() {
		super();
	}

	@Override
	public byte[] toBinary() {
		return new byte[0];
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}

	@Override
	public List<ByteArrayRange> getQueryRanges(
			final NumericQueryConstraint indexedRange,
			final IndexMetaData... hints ) {
		return indexedRange.getRange();
	}

	@Override
	public List<ByteArrayRange> getQueryRanges(
			final NumericQueryConstraint indexedRange,
			final int maxEstimatedRangeDecomposition,
			final IndexMetaData... hints ) {
		return getQueryRanges(
				indexedRange,
				hints);
	}

	@Override
	public String getId() {
		return ID;
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			final List<FieldInfo<Number>> indexedData ) {
		final List<ByteArrayId> insertionIds = new ArrayList<>();
		for (final FieldInfo<Number> fieldInfo : indexedData) {
			insertionIds.add(new ByteArrayId(
					toIndexByte(fieldInfo.getDataValue().getValue())));
		}
		return insertionIds;
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			final List<FieldInfo<Number>> indexedData,
			final int maxEstimatedDuplicateIds ) {
		return getInsertionIds(indexedData);
	}

	@Override
	public List<FieldInfo<Number>> getRangeForId(
			final ByteArrayId insertionId ) {
		return Collections.emptyList();
	}

	public static final byte[] toIndexByte(
			final Number number ) {
		return Lexicoders.DOUBLE.toByteArray(number.doubleValue());
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
