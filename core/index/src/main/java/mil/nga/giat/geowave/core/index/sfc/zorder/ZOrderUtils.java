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
package mil.nga.giat.geowave.core.index.sfc.zorder;

import java.util.Arrays;
import java.util.BitSet;

import mil.nga.giat.geowave.core.index.sfc.SFCDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;

/**
 * Convenience methods used to decode/encode Z-Order space filling curve values
 * (using a simple bit-interleaving approach).
 * 
 */
public class ZOrderUtils
{
	public static NumericRange[] decodeRanges(
			final byte[] bytes,
			final int bitsPerDimension,
			final SFCDimensionDefinition[] dimensionDefinitions ) {
		final byte[] littleEndianBytes = swapEndianFormat(bytes);
		final BitSet bitSet = BitSet.valueOf(littleEndianBytes);
		final NumericRange[] normalizedValues = new NumericRange[dimensionDefinitions.length];
		for (int d = 0; d < dimensionDefinitions.length; d++) {
			final BitSet dimensionSet = new BitSet();
			int j = 0;
			for (int i = d; i < (bitsPerDimension * dimensionDefinitions.length); i += dimensionDefinitions.length) {
				dimensionSet.set(
						j++,
						bitSet.get(i));
			}

			normalizedValues[d] = decode(
					dimensionSet,
					0,
					1,
					dimensionDefinitions[d]);
		}

		return normalizedValues;
	}

	public static long[] decodeIndices(
			final byte[] bytes,
			final int bitsPerDimension,
			final int numDimensions ) {
		final byte[] littleEndianBytes = swapEndianFormat(bytes);
		final BitSet bitSet = BitSet.valueOf(littleEndianBytes);
		final long[] coordinates = new long[numDimensions];
		final long rangePerDimension = (long) Math.pow(
				2,
				bitsPerDimension);
		for (int d = 0; d < numDimensions; d++) {
			final BitSet dimensionSet = new BitSet();
			int j = 0;
			for (int i = d; i < (bitsPerDimension * numDimensions); i += numDimensions) {
				dimensionSet.set(
						j++,
						bitSet.get(i));
			}

			coordinates[d] = decodeIndex(
					dimensionSet,
					rangePerDimension);
		}

		return coordinates;
	}

	private static long decodeIndex(
			final BitSet bs,
			final long rangePerDimension ) {
		long floor = 0;
		long ceiling = rangePerDimension;
		long mid = 0;
		for (int i = 0; i < bs.length(); i++) {
			mid = (floor + ceiling) / 2;
			if (bs.get(i)) {
				floor = mid;
			}
			else {
				ceiling = mid;
			}
		}
		return mid;
	}

	private static NumericRange decode(
			final BitSet bs,
			double floor,
			double ceiling,
			final SFCDimensionDefinition dimensionDefinition ) {
		double mid = 0;
		for (int i = 0; i < bs.length(); i++) {
			mid = (floor + ceiling) / 2;
			if (bs.get(i)) {
				floor = mid;
			}
			else {
				ceiling = mid;
			}
		}
		return new NumericRange(
				dimensionDefinition.denormalize(floor),
				dimensionDefinition.denormalize(ceiling));
	}

	public static byte[] encode(
			final double[] normalizedValues,
			final int bitsPerDimension,
			final int numDimensions ) {
		final BitSet[] bitSets = new BitSet[numDimensions];

		for (int d = 0; d < numDimensions; d++) {
			bitSets[d] = getBits(
					normalizedValues[d],
					0,
					1,
					bitsPerDimension);
		}
		int usedBits = bitsPerDimension * numDimensions;
		int usedBytes = (int) Math.ceil(usedBits / 8.0);
		int bitsetLength = (usedBytes * 8);
		int bitOffset = bitsetLength - usedBits;
		// round up to a bitset divisible by 8
		final BitSet combinedBitSet = new BitSet(
				bitsetLength);
		int j = bitOffset;
		for (int i = 0; i < bitsPerDimension; i++) {
			for (int d = 0; d < numDimensions; d++) {
				combinedBitSet.set(
						j++,
						bitSets[d].get(i));
			}
		}
		final byte[] littleEndianBytes = combinedBitSet.toByteArray();
		byte[] retVal = swapEndianFormat(littleEndianBytes);
		if (retVal.length < usedBytes) {
			return Arrays.copyOf(
					retVal,
					usedBytes);
		}
		return retVal;
	}

	public static byte[] swapEndianFormat(
			final byte[] b ) {
		final byte[] endianSwappedBytes = new byte[b.length];
		for (int i = 0; i < b.length; i++) {
			endianSwappedBytes[i] = swapEndianFormat(b[i]);
		}
		return endianSwappedBytes;
	}

	private static byte swapEndianFormat(
			final byte b ) {
		int converted = 0x00;
		converted ^= (b & 0b1000_0000) >> 7;
		converted ^= (b & 0b0100_0000) >> 5;
		converted ^= (b & 0b0010_0000) >> 3;
		converted ^= (b & 0b0001_0000) >> 1;
		converted ^= (b & 0b0000_1000) << 1;
		converted ^= (b & 0b0000_0100) << 3;
		converted ^= (b & 0b0000_0010) << 5;
		converted ^= (b & 0b0000_0001) << 7;
		return (byte) (converted & 0xFF);
	}

	private static BitSet getBits(
			final double value,
			double floor,
			double ceiling,
			final int bitsPerDimension ) {
		final BitSet buffer = new BitSet(
				bitsPerDimension);
		for (int i = 0; i < bitsPerDimension; i++) {
			final double mid = (floor + ceiling) / 2;
			if (value >= mid) {
				buffer.set(i);
				floor = mid;
			}
			else {
				ceiling = mid;
			}
		}
		return buffer;
	}

}
