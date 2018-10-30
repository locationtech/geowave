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
package org.locationtech.geowave.core.store.index.numeric;

import java.util.Collections;
import java.util.List;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.IndexMetaData;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.lexicoder.Lexicoders;
import org.locationtech.geowave.core.store.index.FieldIndexStrategy;

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
	public QueryRanges getQueryRanges(
			final NumericQueryConstraint indexedRange,
			final IndexMetaData... hints ) {
		return indexedRange.getQueryRanges();
	}

	@Override
	public QueryRanges getQueryRanges(
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
	public InsertionIds getInsertionIds(
			final Number indexedData ) {
		return new InsertionIds(
				null,
				Collections.singletonList(new ByteArray(
						toIndexByte(indexedData))));
	}

	@Override
	public InsertionIds getInsertionIds(
			final Number indexedData,
			final int maxEstimatedDuplicateIds ) {
		return getInsertionIds(indexedData);
	}

	public static final byte[] toIndexByte(
			final Number number ) {
		return Lexicoders.DOUBLE.toByteArray(number.doubleValue());
	}

	@Override
	public List<IndexMetaData> createMetaData() {
		return Collections.emptyList();
	}

	@Override
	public Number getRangeForId(
			final ByteArray partitionKey,
			final ByteArray sortKey ) {
		return Lexicoders.DOUBLE.fromByteArray(sortKey.getBytes());
	}
}
