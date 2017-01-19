package mil.nga.giat.geowave.core.index.sfc.xz;

import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.SFCFactory.SFCType;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import mil.nga.giat.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy;

/**
 * A factory for creating a Hierachical XZ Index strategy with a
 * TieredSFCIndexStrategy substrategy using various approaches for breaking down
 * the bits of precision per tier
 * 
 */
public class XZHierarchicalIndexFactory
{

	static public XZHierarchicalIndexStrategy createFullIncrementalTieredStrategy(
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
	static public XZHierarchicalIndexStrategy createFullIncrementalTieredStrategy(
			final NumericDimensionDefinition[] baseDefinitions,
			final int[] maxBitsPerDimension,
			final SFCType sfcType,
			Long maxEstimatedDuplicatedIds ) {

		TieredSFCIndexStrategy rasterStrategy = TieredSFCIndexFactory.createFullIncrementalTieredStrategy(
				baseDefinitions,
				maxBitsPerDimension,
				sfcType,
				maxEstimatedDuplicatedIds);

		return new XZHierarchicalIndexStrategy(
				baseDefinitions,
				rasterStrategy,
				maxBitsPerDimension);
	}

}
