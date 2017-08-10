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
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.persist.Persistable;

public class MultiDimensionalCoordinateRanges implements
		Persistable
{
	// this is a generic placeholder for "tiers"
	private byte[] multiDimensionalId;
	private CoordinateRange[][] coordinateRangesPerDimension;

	public MultiDimensionalCoordinateRanges() {
		coordinateRangesPerDimension = new CoordinateRange[][] {};
	}

	public MultiDimensionalCoordinateRanges(
			final byte[] multiDimensionalPrefix,
			final CoordinateRange[][] coordinateRangesPerDimension ) {
		multiDimensionalId = multiDimensionalPrefix;
		this.coordinateRangesPerDimension = coordinateRangesPerDimension;
	}

	public byte[] getMultiDimensionalId() {
		return multiDimensionalId;
	}

	public int getNumDimensions() {
		return coordinateRangesPerDimension.length;
	}

	public CoordinateRange[] getRangeForDimension(
			final int dimension ) {
		return coordinateRangesPerDimension[dimension];
	}

	@Override
	public byte[] toBinary() {
		final List<byte[]> serializedRanges = new ArrayList<>();
		final int idLength = (multiDimensionalId == null ? 0 : multiDimensionalId.length);

		int byteLength = (4 * getNumDimensions()) + 8 + idLength;
		final int[] numPerDimension = new int[getNumDimensions()];
		int d = 0;
		for (final CoordinateRange[] dim : coordinateRangesPerDimension) {
			numPerDimension[d++] = dim.length;
			for (final CoordinateRange range : dim) {
				final byte[] serializedRange = range.toBinary();
				byteLength += (serializedRange.length + 4);
				serializedRanges.add(serializedRange);
			}
		}
		final ByteBuffer buf = ByteBuffer.allocate(byteLength);
		buf.putInt(idLength);
		if (idLength > 0) {
			buf.put(multiDimensionalId);
		}
		buf.putInt(coordinateRangesPerDimension.length);
		for (final int num : numPerDimension) {
			buf.putInt(num);
		}
		for (final byte[] serializedRange : serializedRanges) {
			buf.putInt(serializedRange.length);
			buf.put(serializedRange);
		}
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int idLength = buf.getInt();
		if (idLength > 0) {
			multiDimensionalId = new byte[idLength];
			buf.get(multiDimensionalId);
		}
		else {
			multiDimensionalId = null;
		}
		coordinateRangesPerDimension = new CoordinateRange[buf.getInt()][];
		for (int d = 0; d < coordinateRangesPerDimension.length; d++) {
			coordinateRangesPerDimension[d] = new CoordinateRange[buf.getInt()];
		}
		for (int d = 0; d < coordinateRangesPerDimension.length; d++) {
			for (int i = 0; i < coordinateRangesPerDimension[d].length; i++) {
				final byte[] serializedRange = new byte[buf.getInt()];
				buf.get(serializedRange);

				coordinateRangesPerDimension[d][i] = new CoordinateRange();
				coordinateRangesPerDimension[d][i].fromBinary(serializedRange);
			}
		}
	}
}
