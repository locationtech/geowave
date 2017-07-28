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
package mil.nga.giat.geowave.core.index;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

public class NumericIndexStrategyWrapper implements
		NumericIndexStrategy
{
	private String id;
	private NumericIndexStrategy indexStrategy;

	protected NumericIndexStrategyWrapper() {}

	public NumericIndexStrategyWrapper(
			final String id,
			final NumericIndexStrategy indexStrategy ) {
		this.id = id;
		this.indexStrategy = indexStrategy;
	}

	@Override
	public String getId() {
		return id;
	}

	@Override
	public byte[] toBinary() {
		final byte[] idBinary = StringUtils.stringToBinary(id);
		final byte[] delegateBinary = PersistenceUtils.toBinary(indexStrategy);
		final ByteBuffer buf = ByteBuffer.allocate(4 + idBinary.length + delegateBinary.length);
		buf.putInt(idBinary.length);
		buf.put(idBinary);
		buf.put(delegateBinary);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int idBinaryLength = buf.getInt();
		final byte[] idBinary = new byte[idBinaryLength];
		final byte[] delegateBinary = new byte[bytes.length - idBinaryLength - 4];
		buf.get(idBinary);
		buf.get(delegateBinary);
		id = StringUtils.stringFromBinary(idBinary);
		indexStrategy = (NumericIndexStrategy) PersistenceUtils.fromBinary(delegateBinary);
	}

	@Override
	public QueryRanges getQueryRanges(
			final MultiDimensionalNumericData indexedRange,
			final IndexMetaData... hints ) {
		return indexStrategy.getQueryRanges(
				indexedRange,
				hints);
	}

	@Override
	public QueryRanges getQueryRanges(
			final MultiDimensionalNumericData indexedRange,
			final int maxRangeDecomposition,
			final IndexMetaData... hints ) {
		return indexStrategy.getQueryRanges(
				indexedRange,
				maxRangeDecomposition,
				hints);
	}

	@Override
	public InsertionIds getInsertionIds(
			final MultiDimensionalNumericData indexedData ) {
		return indexStrategy.getInsertionIds(indexedData);
	}

	@Override
	public MultiDimensionalNumericData getRangeForId(
			final ByteArrayId partitionKey,
			final ByteArrayId sortKey ) {
		return indexStrategy.getRangeForId(
				partitionKey,
				sortKey);
	}

	@Override
	public MultiDimensionalCoordinates getCoordinatesPerDimension(
			final ByteArrayId partitionKey,
			final ByteArrayId sortKey ) {
		return indexStrategy.getCoordinatesPerDimension(
				partitionKey,
				sortKey);
	}

	@Override
	public NumericDimensionDefinition[] getOrderedDimensionDefinitions() {
		return indexStrategy.getOrderedDimensionDefinitions();
	}

	@Override
	public double[] getHighestPrecisionIdRangePerDimension() {
		return indexStrategy.getHighestPrecisionIdRangePerDimension();
	}

	@Override
	public InsertionIds getInsertionIds(
			final MultiDimensionalNumericData indexedData,
			final int maxDuplicateInsertionIds ) {
		return indexStrategy.getInsertionIds(
				indexedData,
				maxDuplicateInsertionIds);
	}

	@Override
	public int getPartitionKeyLength() {
		return indexStrategy.getPartitionKeyLength();
	}

	@Override
	public List<IndexMetaData> createMetaData() {
		return indexStrategy.createMetaData();
	}

	@Override
	public MultiDimensionalCoordinateRanges[] getCoordinateRangesPerDimension(
			final MultiDimensionalNumericData dataRange,
			final IndexMetaData... hints ) {
		return indexStrategy.getCoordinateRangesPerDimension(
				dataRange,
				hints);
	}

	@Override
	public Set<ByteArrayId> getInsertionPartitionKeys(
			final MultiDimensionalNumericData insertionData ) {
		return indexStrategy.getInsertionPartitionKeys(insertionData);
	}

	@Override
	public Set<ByteArrayId> getQueryPartitionKeys(
			final MultiDimensionalNumericData queryData,
			final IndexMetaData... hints ) {
		return indexStrategy.getQueryPartitionKeys(
				queryData,
				hints);
	}
}
