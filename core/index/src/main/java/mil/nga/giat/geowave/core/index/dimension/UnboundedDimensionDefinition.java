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
package mil.nga.giat.geowave.core.index.dimension;

import mil.nga.giat.geowave.core.index.dimension.bin.BinRange;
import mil.nga.giat.geowave.core.index.dimension.bin.BinningStrategy;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;

/**
 * Because space filling curves require an extent (minimum & maximum), the
 * unbounded implementation relies on an external binning strategy to translate
 * an unbounded variable into bounded bins
 */
public class UnboundedDimensionDefinition extends
		BasicDimensionDefinition
{

	protected BinningStrategy binningStrategy;

	public UnboundedDimensionDefinition() {
		super();
	}

	/**
	 * 
	 * @param binningStrategy
	 *            a bin strategy associated with the dimension
	 */
	public UnboundedDimensionDefinition(
			final BinningStrategy binningStrategy ) {
		super(
				binningStrategy.getBinMin(),
				binningStrategy.getBinMax());
		this.binningStrategy = binningStrategy;

	}

	/**
	 * @param index
	 *            a numeric value to be normalized
	 */
	@Override
	public BinRange[] getNormalizedRanges(
			final NumericData index ) {
		return binningStrategy.getNormalizedRanges(index);
	}

	/**
	 * 
	 * @return a bin strategy associated with the dimension
	 */
	public BinningStrategy getBinningStrategy() {
		return binningStrategy;
	}

	@Override
	public NumericRange getDenormalizedRange(
			BinRange range ) {
		return binningStrategy.getDenormalizedRanges(range);
	}

	@Override
	public int getFixedBinIdSize() {
		return binningStrategy.getFixedBinIdSize();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + ((binningStrategy == null) ? 0 : binningStrategy.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (!super.equals(obj)) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final UnboundedDimensionDefinition other = (UnboundedDimensionDefinition) obj;
		if (binningStrategy == null) {
			if (other.binningStrategy != null) {
				return false;
			}
		}
		else if (!binningStrategy.equals(other.binningStrategy)) {
			return false;
		}
		return true;
	}

	@Override
	public byte[] toBinary() {
		return PersistenceUtils.toBinary(binningStrategy);
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		binningStrategy = (BinningStrategy) PersistenceUtils.fromBinary(bytes);
		min = binningStrategy.getBinMin();
		max = binningStrategy.getBinMax();
	}
}
