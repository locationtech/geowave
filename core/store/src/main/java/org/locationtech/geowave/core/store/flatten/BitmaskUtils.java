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
package org.locationtech.geowave.core.store.flatten;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.CommonIndexValue;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Utility methods when dealing with bitmasks in Accumulo
 *
 * @since 0.9.1
 */
public class BitmaskUtils
{
	public static byte[] generateANDBitmask(
			final byte[] bitmask1,
			final byte[] bitmask2 ) {
		final byte[] result = new byte[Math.min(
				bitmask1.length,
				bitmask2.length)];
		for (int i = 0; i < result.length; i++) {
			result[i] = bitmask1[i];
			result[i] &= bitmask2[i];
		}
		return result;
	}

	public static boolean isAnyBitSet(
			final byte[] array ) {
		for (final byte b : array) {
			if (b != 0) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Generates a composite bitmask given a list of field positions. The
	 * composite bitmask represents a true bit for every positive field position
	 *
	 * For example, given field 0, field 1, and field 2 this method will return
	 * 00000111
	 *
	 * @param fieldPositions
	 *            a list of field positions
	 * @return a composite bitmask
	 */
	public static byte[] generateCompositeBitmask(
			final SortedSet<Integer> fieldPositions ) {
		final byte[] retVal = new byte[(fieldPositions.last() / 8) + 1];
		for (final Integer fieldPosition : fieldPositions) {
			final int bytePosition = fieldPosition / 8;
			final int bitPosition = fieldPosition % 8;
			retVal[bytePosition] |= (1 << bitPosition);
		}
		return retVal;
	}

	/**
	 * Generates a composite bitmask given a single field position. The
	 * composite bitmask represents a true bit for this field position
	 *
	 * For example, given field 2 this method will return 00000100
	 *
	 * @param fieldPosition
	 *            a field position
	 * @return a composite bitmask
	 */
	public static byte[] generateCompositeBitmask(
			final Integer fieldPosition ) {
		return generateCompositeBitmask(new TreeSet<Integer>(
				Collections.singleton(fieldPosition)));
	}

	private static LoadingCache<ByteArray, List<Integer>> fieldPositionCache = CacheBuilder.newBuilder().maximumSize(
			100).build(
			new CacheLoader<ByteArray, List<Integer>>() {

				@Override
				public List<Integer> load(
						ByteArray key )
						throws Exception {
					final List<Integer> fieldPositions = new ArrayList<>();
					int currentByte = 0;
					for (final byte singleByteBitMask : key.getBytes()) {
						for (int bit = 0; bit < 8; ++bit) {
							if (((singleByteBitMask >>> bit) & 0x1) == 1) {
								fieldPositions.add((currentByte * 8) + bit);
							}
						}
						currentByte++;
					}
					return fieldPositions;
				}

			});

	/**
	 * Iterates the set (true) bits within the given composite bitmask and
	 * generates a list of field positions.
	 *
	 * @param compositeBitmask
	 *            the composite bitmask
	 * @return a list of field positions
	 */
	public static List<Integer> getFieldPositions(
			final byte[] bitmask ) {
		return fieldPositionCache.getUnchecked(new ByteArray(
				bitmask));
	}

	/**
	 * Iterates the set (true) bits within the given composite bitmask and
	 * generates a list of field positions.
	 *
	 * @param compositeBitmask
	 *            the composite bitmask
	 * @return a list of field positions
	 */
	public static int getLowestFieldPosition(
			final byte[] bitmask ) {
		int currentByte = 0;
		for (final byte singleByteBitMask : bitmask) {
			for (int bit = 0; bit < 8; ++bit) {
				if (((singleByteBitMask >>> bit) & 0x1) == 1) {
					return (currentByte * 8) + bit;
				}
			}
			currentByte++;
		}
		return Integer.MAX_VALUE;
	}

	/**
	 * Generates a field subset bitmask for the given index, adapter, and fields
	 *
	 * @param indexModel
	 *            the index's CommonIndexModel
	 * @param fieldIds
	 *            the fields to include in the subset, as Strings
	 * @param adapterAssociatedWithFieldIds
	 *            the adapter for the type whose fields are being subsetted
	 * @return the field subset bitmask
	 */
	public static byte[] generateFieldSubsetBitmask(
			final CommonIndexModel indexModel,
			final String[] fieldNames,
			final DataTypeAdapter<?> adapterAssociatedWithFieldIds ) {
		final SortedSet<Integer> fieldPositions = new TreeSet<Integer>();

		// dimension fields must also be included
		for (final NumericDimensionField<? extends CommonIndexValue> dimension : indexModel.getDimensions()) {
			fieldPositions.add(adapterAssociatedWithFieldIds.getPositionOfOrderedField(
					indexModel,
					dimension.getFieldName()));
		}

		for (final String fieldName : fieldNames) {
			fieldPositions.add(adapterAssociatedWithFieldIds.getPositionOfOrderedField(
					indexModel,
					fieldName));
		}
		return generateCompositeBitmask(fieldPositions);
	}

	/**
	 * Generates a new value byte array representing a subset of fields of the
	 * given value
	 *
	 * @param value
	 *            the original column value
	 * @param originalBitmask
	 *            the bitmask from the column qualifier
	 * @param newBitmask
	 *            the field subset bitmask
	 * @return the subsetted value as a byte[]
	 */
	public static byte[] constructNewValue(
			final byte[] value,
			final byte[] originalBitmask,
			final byte[] newBitmask ) {

		final ByteBuffer originalBytes = ByteBuffer.wrap(value);
		final List<byte[]> valsToKeep = new ArrayList<>();
		int totalSize = 0;
		final List<Integer> originalPositions = getFieldPositions(originalBitmask);
		// convert list to set for quick contains()
		final Set<Integer> newPositions = new HashSet<Integer>(
				getFieldPositions(newBitmask));
		if (originalPositions.size() > 1) {
			for (final Integer originalPosition : originalPositions) {
				final int len = originalBytes.getInt();
				final byte[] val = new byte[len];
				originalBytes.get(val);
				if (newPositions.contains(originalPosition)) {
					valsToKeep.add(val);
					totalSize += len;
				}
			}
		}
		else if (!newPositions.isEmpty()) {
			// this shouldn't happen because we should already catch the case
			// where the bitmask is unchanged
			return value;
		}
		else {
			// and this shouldn't happen because we should already catch the
			// case where the resultant bitmask is empty
			return null;
		}
		if (valsToKeep.size() == 1) {
			final ByteBuffer retVal = ByteBuffer.allocate(totalSize);
			retVal.put(valsToKeep.get(0));
			return retVal.array();
		}
		final ByteBuffer retVal = ByteBuffer.allocate((valsToKeep.size() * 4) + totalSize);
		for (final byte[] val : valsToKeep) {
			retVal.putInt(val.length);
			retVal.put(val);
		}
		return retVal.array();
	}

}
