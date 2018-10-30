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
package org.locationtech.geowave.core.store.index.text;

import java.util.Collections;
import java.util.List;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.IndexMetaData;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.store.index.FieldIndexStrategy;

public class TextIndexStrategy implements
		FieldIndexStrategy<TextQueryConstraint, String>
{
	private static final String ID = "TEXT";

	public TextIndexStrategy() {
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
	public String getId() {
		return ID;
	}

	@Override
	public List<IndexMetaData> createMetaData() {
		return Collections.emptyList();
	}

	@Override
	public QueryRanges getQueryRanges(
			final TextQueryConstraint indexedRange,
			final IndexMetaData... hints ) {
		return indexedRange.getQueryRanges();
	}

	@Override
	public QueryRanges getQueryRanges(
			final TextQueryConstraint indexedRange,
			final int maxEstimatedRangeDecomposition,
			final IndexMetaData... hints ) {
		return getQueryRanges(
				indexedRange,
				hints);
	}

	@Override
	public InsertionIds getInsertionIds(
			final String indexedData ) {
		return new InsertionIds(
				Collections.singletonList(new ByteArray(
						indexedData)));
	}

	@Override
	public InsertionIds getInsertionIds(
			final String indexedData,
			final int maxEstimatedDuplicateIds ) {
		return getInsertionIds(indexedData);
	}

	@Override
	public String getRangeForId(
			final ByteArray partitionKey,
			final ByteArray sortKey ) {
		return sortKey.getString();
	}
}
