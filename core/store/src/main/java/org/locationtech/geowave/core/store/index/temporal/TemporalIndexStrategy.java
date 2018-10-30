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
package org.locationtech.geowave.core.store.index.temporal;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.IndexMetaData;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.lexicoder.Lexicoders;
import org.locationtech.geowave.core.store.index.FieldIndexStrategy;

public class TemporalIndexStrategy implements
		FieldIndexStrategy<TemporalQueryConstraint, Date>
{
	private static final String ID = "TEMPORAL";

	public TemporalIndexStrategy() {
		super();
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
	public List<IndexMetaData> createMetaData() {
		return Collections.emptyList();
	}

	@Override
	public QueryRanges getQueryRanges(
			final TemporalQueryConstraint indexedRange,
			final IndexMetaData... hints ) {
		return indexedRange.getQueryRanges();
	}

	@Override
	public QueryRanges getQueryRanges(
			final TemporalQueryConstraint indexedRange,
			final int maxEstimatedRangeDecomposition,
			final IndexMetaData... hints ) {
		return getQueryRanges(indexedRange);
	}

	@Override
	public InsertionIds getInsertionIds(
			final Date indexedData ) {
		return new InsertionIds(
				Collections.singletonList(new ByteArray(
						toIndexByte(indexedData))));
	}

	@Override
	public InsertionIds getInsertionIds(
			final Date indexedData,
			final int maxEstimatedDuplicateIds ) {
		return getInsertionIds(indexedData);
	}

	@Override
	public Date getRangeForId(
			final ByteArray partitionKey,
			final ByteArray sortKey ) {
		return new Date(
				Lexicoders.LONG.fromByteArray(sortKey.getBytes()));
	}
}
