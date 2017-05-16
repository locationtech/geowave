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
package mil.nga.giat.geowave.core.index.sfc.tiered;

import java.util.Arrays;

import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SFCDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory;
import mil.nga.giat.geowave.core.index.sfc.SpaceFillingCurve;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType;

import com.google.common.collect.ImmutableBiMap;

/**
 * A factory for creating TieredSFCIndexStrategy using various approaches for
 * breaking down the bits of precision per tier
 * 
 */
public class TieredSFCIndexFactory
{
	private static int DEFAULT_NUM_TIERS = 11;

	/**
	 * Used to create a Single Tier Index Strategy. For example, this would be
	 * used to generate a strategy that has Point type spatial data.
	 * 
	 * @param dimensionDefs
	 *            an array of SFC Dimension Definition objects
	 * @param sfc
	 *            the type of space filling curve (e.g. Hilbert)
	 * @return an Index Strategy object with a single tier
	 */
	static public TieredSFCIndexStrategy createSingleTierStrategy(
			final SFCDimensionDefinition[] dimensionDefs,
			final SFCType sfc ) {
		final SpaceFillingCurve[] orderedSfcs = new SpaceFillingCurve[] {
			SFCFactory.createSpaceFillingCurve(
					dimensionDefs,
					sfc)
		};
		// unwrap SFC dimension definitions
		final NumericDimensionDefinition[] baseDefinitions = new NumericDimensionDefinition[dimensionDefs.length];
		int maxBitsOfPrecision = Integer.MIN_VALUE;
		for (int d = 0; d < baseDefinitions.length; d++) {
			baseDefinitions[d] = dimensionDefs[d].getDimensionDefinition();
			maxBitsOfPrecision = Math.max(
					dimensionDefs[d].getBitsOfPrecision(),
					maxBitsOfPrecision);
		}
		return new TieredSFCIndexStrategy(
				baseDefinitions,
				orderedSfcs,
				ImmutableBiMap.of(
						0,
						(byte) maxBitsOfPrecision));
	}

	/**
	 * Used to create a Single Tier Index Strategy. For example, this would be
	 * used to generate a strategy that has Point type spatial data.
	 * 
	 * @param dimensionDefs
	 *            an array of SFC Dimension Definition objects
	 * @param sfc
	 *            the type of space filling curve (e.g. Hilbert)
	 * @return an Index Strategy object with a single tier
	 */
	static public TieredSFCIndexStrategy createSingleTierStrategy(
			final NumericDimensionDefinition[] baseDefinitions,
			final int[] maxBitsPerDimension,
			final SFCType sfc ) {
		final SFCDimensionDefinition[] sfcDimensions = new SFCDimensionDefinition[baseDefinitions.length];
		int maxBitsOfPrecision = Integer.MIN_VALUE;
		for (int d = 0; d < baseDefinitions.length; d++) {
			sfcDimensions[d] = new SFCDimensionDefinition(
					baseDefinitions[d],
					maxBitsPerDimension[d]);
			maxBitsOfPrecision = Math.max(
					maxBitsPerDimension[d],
					maxBitsOfPrecision);
		}

		final SpaceFillingCurve[] orderedSfcs = new SpaceFillingCurve[] {
			SFCFactory.createSpaceFillingCurve(
					sfcDimensions,
					sfc)
		};

		return new TieredSFCIndexStrategy(
				baseDefinitions,
				orderedSfcs,
				ImmutableBiMap.of(
						0,
						(byte) maxBitsOfPrecision));
	}

	static public TieredSFCIndexStrategy createFullIncrementalTieredStrategy(
			final NumericDimensionDefinition[] baseDefinitions,
			final int[] maxBitsPerDimension,
			final SFCType sfcType ) {
		return createFullIncrementalTieredStrategy(
				baseDefinitions,
				maxBitsPerDimension,
				sfcType,
				null);
	}

	/**
	 * 
	 * @param baseDefinitions
	 *            an array of Numeric Dimension Definitions
	 * @param maxBitsPerDimension
	 *            the max cardinality for the Index Strategy
	 * @param sfcType
	 *            the type of space filling curve (e.g. Hilbert)
	 * @param maxEstimatedDuplicatedIds
	 *            the max number of duplicate SFC IDs
	 * @return an Index Strategy object with a tier for every incremental
	 *         cardinality between the lowest max bits of precision and 0
	 */
	static public TieredSFCIndexStrategy createFullIncrementalTieredStrategy(
			final NumericDimensionDefinition[] baseDefinitions,
			final int[] maxBitsPerDimension,
			final SFCType sfcType,
			Long maxEstimatedDuplicatedIds ) {
		if (maxBitsPerDimension.length == 0) {
			final ImmutableBiMap<Integer, Byte> emptyMap = ImmutableBiMap.of();
			return new TieredSFCIndexStrategy(
					baseDefinitions,
					new SpaceFillingCurve[] {},
					emptyMap);
		}
		int numIndices = Integer.MAX_VALUE;
		for (final int element : maxBitsPerDimension) {
			numIndices = Math.min(
					numIndices,
					element + 1);
		}
		final SpaceFillingCurve[] spaceFillingCurves = new SpaceFillingCurve[numIndices];
		final ImmutableBiMap.Builder<Integer, Byte> sfcIndexToTier = ImmutableBiMap.builder();
		for (int sfcIndex = 0; sfcIndex < numIndices; sfcIndex++) {
			final SFCDimensionDefinition[] sfcDimensions = new SFCDimensionDefinition[baseDefinitions.length];
			int maxBitsOfPrecision = Integer.MIN_VALUE;
			for (int d = 0; d < baseDefinitions.length; d++) {
				final int bitsOfPrecision = maxBitsPerDimension[d] - (numIndices - sfcIndex - 1);
				maxBitsOfPrecision = Math.max(
						bitsOfPrecision,
						maxBitsOfPrecision);
				sfcDimensions[d] = new SFCDimensionDefinition(
						baseDefinitions[d],
						bitsOfPrecision);
			}
			sfcIndexToTier.put(
					sfcIndex,
					(byte) maxBitsOfPrecision);

			spaceFillingCurves[sfcIndex] = SFCFactory.createSpaceFillingCurve(
					sfcDimensions,
					sfcType);

		}
		if (maxEstimatedDuplicatedIds != null && maxEstimatedDuplicatedIds > 0) {
			return new TieredSFCIndexStrategy(
					baseDefinitions,
					spaceFillingCurves,
					sfcIndexToTier.build(),
					maxEstimatedDuplicatedIds);
		}
		return new TieredSFCIndexStrategy(
				baseDefinitions,
				spaceFillingCurves,
				sfcIndexToTier.build());
	}

	/**
	 * 
	 * @param baseDefinitions
	 *            an array of Numeric Dimension Definitions
	 * @param maxBitsPerDimension
	 *            the max cardinality for the Index Strategy
	 * @param sfcType
	 *            the type of space filling curve (e.g. Hilbert)
	 * @return an Index Strategy object with a equal interval tiers
	 */
	static public TieredSFCIndexStrategy createEqualIntervalPrecisionTieredStrategy(
			final NumericDimensionDefinition[] baseDefinitions,
			final int[] maxBitsPerDimension,
			final SFCType sfcType ) {
		return createEqualIntervalPrecisionTieredStrategy(
				baseDefinitions,
				maxBitsPerDimension,
				sfcType,
				DEFAULT_NUM_TIERS);
	}

	/**
	 * 
	 * @param baseDefinitions
	 *            an array of Numeric Dimension Definitions
	 * @param maxBitsPerDimension
	 *            the max cardinality for the Index Strategy
	 * @param sfcType
	 *            the type of space filling curve (e.g. Hilbert)
	 * @param numTiers
	 *            the number of tiers of the Index Strategy
	 * @return an Index Strategy object with a specified number of tiers
	 */
	static public TieredSFCIndexStrategy createEqualIntervalPrecisionTieredStrategy(
			final NumericDimensionDefinition[] baseDefinitions,
			final int[] maxBitsPerDimension,
			final SFCType sfcType,
			final int numIndices ) {
		// Subtracting one from the number tiers prevents an extra tier. If
		// we decide to create a catch-all, then we can ignore the subtraction.
		final SpaceFillingCurve[] spaceFillingCurves = new SpaceFillingCurve[numIndices];
		final ImmutableBiMap.Builder<Integer, Byte> sfcIndexToTier = ImmutableBiMap.builder();
		for (int sfcIndex = 0; sfcIndex < numIndices; sfcIndex++) {
			final SFCDimensionDefinition[] sfcDimensions = new SFCDimensionDefinition[baseDefinitions.length];
			int maxBitsOfPrecision = Integer.MIN_VALUE;
			for (int d = 0; d < baseDefinitions.length; d++) {
				int bitsOfPrecision;
				if (numIndices == 1) {
					bitsOfPrecision = maxBitsPerDimension[d];
				}
				else {
					final double bitPrecisionIncrement = ((double) maxBitsPerDimension[d] / (numIndices - 1));
					bitsOfPrecision = (int) (bitPrecisionIncrement * sfcIndex);
				}
				maxBitsOfPrecision = Math.max(
						bitsOfPrecision,
						maxBitsOfPrecision);
				sfcDimensions[d] = new SFCDimensionDefinition(
						baseDefinitions[d],
						bitsOfPrecision);
			}
			sfcIndexToTier.put(
					sfcIndex,
					(byte) maxBitsOfPrecision);
			spaceFillingCurves[sfcIndex] = SFCFactory.createSpaceFillingCurve(
					sfcDimensions,
					sfcType);

		}

		return new TieredSFCIndexStrategy(
				baseDefinitions,
				spaceFillingCurves,
				sfcIndexToTier.build());
	}

	/**
	 * 
	 * @param orderedDimensionDefinitions
	 *            an array of Numeric Dimension Definitions
	 * @param bitsPerDimensionPerLevel
	 * @param sfcType
	 *            the type of space filling curve (e.g. Hilbert)
	 * @return an Index Strategy object with a specified number of tiers
	 */
	static public TieredSFCIndexStrategy createDefinedPrecisionTieredStrategy(
			final NumericDimensionDefinition[] orderedDimensionDefinitions,
			final int[][] bitsPerDimensionPerLevel,
			final SFCType sfcType ) {
		Integer numLevels = null;
		for (final int[] element : bitsPerDimensionPerLevel) {
			if (numLevels == null) {
				numLevels = element.length;
			}
			else {
				numLevels = Math.min(
						numLevels,
						element.length);
			}

			Arrays.sort(element);
		}
		if (numLevels == null) {
			numLevels = 0;
		}

		final SpaceFillingCurve[] orderedSFCTiers = new SpaceFillingCurve[numLevels];
		final int numDimensions = orderedDimensionDefinitions.length;
		final ImmutableBiMap.Builder<Integer, Byte> sfcIndexToTier = ImmutableBiMap.builder();
		for (int l = 0; l < numLevels; l++) {
			final SFCDimensionDefinition[] sfcDimensions = new SFCDimensionDefinition[numDimensions];
			int maxBitsOfPrecision = Integer.MIN_VALUE;
			for (int d = 0; d < numDimensions; d++) {
				sfcDimensions[d] = new SFCDimensionDefinition(
						orderedDimensionDefinitions[d],
						bitsPerDimensionPerLevel[d][l]);
				maxBitsOfPrecision = Math.max(
						bitsPerDimensionPerLevel[d][l],
						maxBitsOfPrecision);
			}
			sfcIndexToTier.put(
					l,
					(byte) maxBitsOfPrecision);
			orderedSFCTiers[l] = SFCFactory.createSpaceFillingCurve(
					sfcDimensions,
					sfcType);
		}
		return new TieredSFCIndexStrategy(
				orderedDimensionDefinitions,
				orderedSFCTiers,
				sfcIndexToTier.build());
	}

}
