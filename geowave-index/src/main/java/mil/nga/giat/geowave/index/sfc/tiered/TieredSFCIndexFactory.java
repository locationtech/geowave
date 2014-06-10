package mil.nga.giat.geowave.index.sfc.tiered;

import java.util.Arrays;

import mil.nga.giat.geowave.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.index.sfc.SFCDimensionDefinition;
import mil.nga.giat.geowave.index.sfc.SFCFactory;
import mil.nga.giat.geowave.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.index.sfc.SpaceFillingCurve;

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
			SFCDimensionDefinition[] dimensionDefs,
			SFCType sfc ) {
		SpaceFillingCurve[] orderedSfcs = new SpaceFillingCurve[] {
			SFCFactory.createSpaceFillingCurve(
					dimensionDefs,
					sfc)
		};
		// unwrap SFC dimension definitions
		NumericDimensionDefinition[] baseDefinitions = new NumericDimensionDefinition[dimensionDefs.length];
		for (int d = 0; d < baseDefinitions.length; d++) {
			baseDefinitions[d] = dimensionDefs[d].getDimensionDefinition();
		}
		return new TieredSFCIndexStrategy(
				baseDefinitions,
				orderedSfcs);
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
			NumericDimensionDefinition[] baseDefinitions,
			int[] maxBitsPerDimension,
			SFCType sfcType ) {
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
			NumericDimensionDefinition[] baseDefinitions,
			int[] maxBitsPerDimension,
			SFCType sfcType,
			int numTiers ) {
		// Subtracting one from the number tiers prevents an extra tier. If
		// we decide to create a catch-all, then we can ignore the subtraction.
		SpaceFillingCurve[] spaceFillingCurves = new SpaceFillingCurve[numTiers];

		for (int tier = 0; tier < numTiers; tier++) {
			SFCDimensionDefinition[] sfcDimensions = new SFCDimensionDefinition[baseDefinitions.length];

			for (int d = 0; d < baseDefinitions.length; d++) {
				int bitsOfPrecision;
				if (numTiers == 1) {
					bitsOfPrecision = maxBitsPerDimension[d];
				}
				else {
					double bitPrecisionIncrement = ((double) maxBitsPerDimension[d] / (numTiers - 1));
					bitsOfPrecision = (int) (bitPrecisionIncrement * tier);
				}
				sfcDimensions[d] = new SFCDimensionDefinition(
						baseDefinitions[d],
						bitsOfPrecision);
			}

			spaceFillingCurves[tier] = SFCFactory.createSpaceFillingCurve(
					sfcDimensions,
					sfcType);

		}

		return new TieredSFCIndexStrategy(
				baseDefinitions,
				spaceFillingCurves);
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
			NumericDimensionDefinition[] orderedDimensionDefinitions,
			int[][] bitsPerDimensionPerLevel,
			SFCType sfcType ) {
		Integer numLevels = null;
		for (int d = 0; d < bitsPerDimensionPerLevel.length; d++) {
			if (numLevels == null) {
				numLevels = bitsPerDimensionPerLevel[d].length;
			}
			else {
				numLevels = Math.min(
						numLevels,
						bitsPerDimensionPerLevel[d].length);
			}

			Arrays.sort(bitsPerDimensionPerLevel[d]);
		}
		if (numLevels == null) {
			numLevels = 0;
		}

		SpaceFillingCurve[] orderedSFCTiers = new SpaceFillingCurve[numLevels];
		int numDimensions = orderedDimensionDefinitions.length;
		for (int l = 0; l < numLevels; l++) {
			SFCDimensionDefinition[] sfcDimensions = new SFCDimensionDefinition[numDimensions];
			for (int d = 0; d < numDimensions; d++) {
				sfcDimensions[d] = new SFCDimensionDefinition(
						orderedDimensionDefinitions[d],
						bitsPerDimensionPerLevel[d][l]);
			}
			orderedSFCTiers[l] = SFCFactory.createSpaceFillingCurve(
					sfcDimensions,
					sfcType);
		}
		return new TieredSFCIndexStrategy(
				orderedDimensionDefinitions,
				orderedSFCTiers);
	}

}
