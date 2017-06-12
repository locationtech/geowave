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
package mil.nga.giat.geowave.core.index.sfc;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.dimension.bin.BinRange;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;

/**
 * This class wraps a dimension definition with a cardinality (bits of
 * precision) on a space filling curve
 * 
 */
public class SFCDimensionDefinition implements
		NumericDimensionDefinition
{
	private int bitsOfPrecision;
	private NumericDimensionDefinition dimensionDefinition;

	public SFCDimensionDefinition() {}

	/**
	 * @param dimensionDefinition
	 *            an object which defines a dimension used to create a space
	 *            filling curve
	 * @param bitsOfPrecision
	 *            the number of bits associated with the specified dimension
	 *            object
	 */
	public SFCDimensionDefinition(
			final NumericDimensionDefinition dimensionDefinition,
			final int bitsOfPrecision ) {
		this.bitsOfPrecision = bitsOfPrecision;
		this.dimensionDefinition = dimensionDefinition;
	}

	@Override
	public NumericData getFullRange() {
		return dimensionDefinition.getFullRange();
	}

	/**
	 * 
	 * @return bitsOfPrecision the bits of precision for the dimension object
	 */
	public int getBitsOfPrecision() {
		return bitsOfPrecision;
	}

	/**
	 * 
	 * @param range
	 *            numeric data to be normalized
	 * @return a BinRange[] based on numeric data
	 */
	@Override
	public BinRange[] getNormalizedRanges(
			final NumericData range ) {
		return dimensionDefinition.getNormalizedRanges(range);
	}

	public NumericDimensionDefinition getDimensionDefinition() {
		return dimensionDefinition;
	}

	@Override
	public double normalize(
			final double value ) {
		return dimensionDefinition.normalize(value);
	}

	@Override
	public double denormalize(
			final double value ) {
		return dimensionDefinition.denormalize(value);
	}

	@Override
	public NumericRange getDenormalizedRange(
			final BinRange range ) {
		return dimensionDefinition.getDenormalizedRange(range);
	}

	@Override
	public int getFixedBinIdSize() {
		return dimensionDefinition.getFixedBinIdSize();
	}

	@Override
	public double getRange() {
		return dimensionDefinition.getRange();
	}

	@Override
	public NumericRange getBounds() {
		return dimensionDefinition.getBounds();
	}

	@Override
	public byte[] toBinary() {
		final byte[] dimensionBinary = PersistenceUtils.toBinary(dimensionDefinition);
		final ByteBuffer buf = ByteBuffer.allocate(dimensionBinary.length + 4);
		buf.putInt(bitsOfPrecision);
		buf.put(dimensionBinary);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final byte[] dimensionBinary = new byte[bytes.length - 4];
		bitsOfPrecision = buf.getInt();
		buf.get(dimensionBinary);
		dimensionDefinition = (NumericDimensionDefinition) PersistenceUtils.fromBinary(dimensionBinary);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + bitsOfPrecision;
		result = (prime * result) + ((dimensionDefinition == null) ? 0 : dimensionDefinition.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final SFCDimensionDefinition other = (SFCDimensionDefinition) obj;
		if (bitsOfPrecision != other.bitsOfPrecision) {
			return false;
		}
		if (dimensionDefinition == null) {
			if (other.dimensionDefinition != null) {
				return false;
			}
		}
		else if (!dimensionDefinition.equals(other.dimensionDefinition)) {
			return false;
		}
		return true;
	}
}
