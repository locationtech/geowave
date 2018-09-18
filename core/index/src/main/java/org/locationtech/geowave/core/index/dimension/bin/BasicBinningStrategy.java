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
package org.locationtech.geowave.core.index.dimension.bin;

import java.nio.ByteBuffer;

import org.locationtech.geowave.core.index.sfc.data.NumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;

public class BasicBinningStrategy implements
		BinningStrategy
{
	private double interval;
	private double halfInterval;

	public BasicBinningStrategy() {
		super();
	}

	public BasicBinningStrategy(
			double interval ) {
		super();
		this.interval = interval;
		this.halfInterval = interval / 2;
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buf = ByteBuffer.allocate(8);
		buf.putDouble(interval);
		return buf.array();
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		interval = buf.getDouble();
		halfInterval = interval / 2;
	}

	@Override
	public double getBinMin() {
		return -halfInterval;
	}

	@Override
	public double getBinMax() {
		return halfInterval;
	}

	@Override
	public BinValue getBinnedValue(
			double value ) {
		double bin = Math.floor((value - halfInterval) / interval);
		return new BinValue(
				intToBinary((int) bin),
				(value - interval * bin));
	}

	private static byte[] intToBinary(
			int bin ) {
		ByteBuffer buf = ByteBuffer.allocate(4);
		buf.putInt((int) bin);
		return buf.array();
	}

	@Override
	public BinRange[] getNormalizedRanges(
			NumericData index ) {
		if (!index.isRange()) {
			BinValue value = getBinnedValue(index.getMin());
			return new BinRange[] {
				new BinRange(
						value.getBinId(),
						value.getNormalizedValue(),
						value.getNormalizedValue(),
						false)
			};
		}
		int minBin = (int) Math.ceil((index.getMin() - halfInterval) / interval);
		int maxBin = (int) Math.ceil((index.getMax() - halfInterval) / interval);
		if (minBin == maxBin) {
			double min = (index.getMin() - interval * minBin);
			double max = (index.getMax() - interval * maxBin);
			ByteBuffer buf = ByteBuffer.allocate(4);
			buf.putInt((int) minBin);
			return new BinRange[] {
				new BinRange(
						buf.array(),
						min,
						max,
						false)
			};
		}
		BinRange[] retVal = new BinRange[maxBin - minBin + 1];
		retVal[0] = new BinRange(
				intToBinary(minBin),
				(index.getMin() - interval * minBin),
				halfInterval,
				false);
		for (int b = minBin + 1; b < maxBin; b++) {
			retVal[b - minBin] = new BinRange(
					intToBinary(b),
					-halfInterval,
					halfInterval,
					true);
		}
		retVal[maxBin - minBin] = new BinRange(
				intToBinary(maxBin),
				-halfInterval,
				(index.getMax() - interval * maxBin),
				false);
		return retVal;
	}

	@Override
	public NumericRange getDenormalizedRanges(
			BinRange binnedRange ) {
		int bin = ByteBuffer.wrap(
				binnedRange.getBinId()).getInt();
		double center = bin * interval;
		if (binnedRange.isFullExtent()) {
			return new NumericRange(
					center - halfInterval,
					center + halfInterval);
		}

		return new NumericRange(
				center + binnedRange.getNormalizedMin(),
				center + binnedRange.getNormalizedMax());
	}

	@Override
	public int getFixedBinIdSize() {
		return 4;
	}
}
