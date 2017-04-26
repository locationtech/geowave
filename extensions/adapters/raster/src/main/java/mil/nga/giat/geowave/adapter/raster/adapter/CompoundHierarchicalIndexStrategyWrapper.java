package mil.nga.giat.geowave.adapter.raster.adapter;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.CompoundIndexStrategy;
import mil.nga.giat.geowave.core.index.Coordinate;
import mil.nga.giat.geowave.core.index.HierarchicalNumericIndexStrategy;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRanges;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinates;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.simple.RoundRobinKeyIndexStrategy;

/**
 * This class wraps the first occurrence of a hierarchical index within a
 * compound index such that sub strategies within the hierarchy are replaced
 * maintaining the rest of the structure of the compound index
 *
 *
 */
public class CompoundHierarchicalIndexStrategyWrapper implements
		HierarchicalNumericIndexStrategy
{
	private final static Logger LOGGER = LoggerFactory.getLogger(CompoundHierarchicalIndexStrategyWrapper.class);
	private List<CompoundIndexStrategy> parentStrategies;
	private HierarchicalNumericIndexStrategy firstHierarchicalStrategy;

	public CompoundHierarchicalIndexStrategyWrapper(
			final List<CompoundIndexStrategy> parentStrategies,
			final HierarchicalNumericIndexStrategy firstHierarchicalStrategy ) {
		this.parentStrategies = parentStrategies;
		this.firstHierarchicalStrategy = firstHierarchicalStrategy;
	}

	protected CompoundHierarchicalIndexStrategyWrapper() {
		super();
	}

	@Override
	public SubStrategy[] getSubStrategies() {
		// for these substrategies we need to replace the last parent strategy's
		// hierarchical index strategy with the underlying substrategy index
		// strategy
		final SubStrategy[] subStrategies = firstHierarchicalStrategy.getSubStrategies();
		final SubStrategy[] retVal = new SubStrategy[subStrategies.length];

		for (int i = 0; i < subStrategies.length; i++) {
			NumericIndexStrategy currentStrategyToBeReplaced = firstHierarchicalStrategy;
			NumericIndexStrategy currentStrategyReplacement = subStrategies[i].getIndexStrategy();
			for (int j = parentStrategies.size() - 1; j >= 0; j--) {
				// traverse parents in reverse order
				final CompoundIndexStrategy parent = parentStrategies.get(j);
				if (parent.getPrimarySubStrategy().equals(
						currentStrategyToBeReplaced)) {
					// replace primary
					currentStrategyReplacement = new CompoundIndexStrategy(
							currentStrategyReplacement,
							parent.getSecondarySubStrategy());
				}
				else {
					// replace secondary
					currentStrategyReplacement = new CompoundIndexStrategy(
							parent.getPrimarySubStrategy(),
							currentStrategyReplacement);
				}

				currentStrategyToBeReplaced = parent;
			}
			retVal[i] = new SubStrategy(
					currentStrategyReplacement,
					subStrategies[i].getPrefix());
		}
		return retVal;
	}

	@Override
	public byte[] toBinary() {
		return PersistenceUtils.toBinary(parentStrategies.get(0));
	}

	@Override
	public List<ByteArrayRange> getQueryRanges(
			final MultiDimensionalNumericData indexedRange,
			final IndexMetaData... hints ) {
		return parentStrategies.get(
				0).getQueryRanges(
				indexedRange,
				hints);
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final CompoundIndexStrategy rootStrategy = PersistenceUtils.fromBinary(
				bytes,
				CompoundIndexStrategy.class);
		parentStrategies = new ArrayList<CompoundIndexStrategy>();
		// discover hierarchy
		firstHierarchicalStrategy = findHierarchicalStrategy(
				rootStrategy,
				parentStrategies);
	}

	@Override
	public List<ByteArrayRange> getQueryRanges(
			final MultiDimensionalNumericData indexedRange,
			final int maxEstimatedRangeDecomposition,
			final IndexMetaData... hints ) {
		return parentStrategies.get(
				0).getQueryRanges(
				indexedRange,
				maxEstimatedRangeDecomposition,
				hints);
	}

	@Override
	public NumericDimensionDefinition[] getOrderedDimensionDefinitions() {
		return parentStrategies.get(
				0).getOrderedDimensionDefinitions();
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			final MultiDimensionalNumericData indexedData ) {
		return parentStrategies.get(
				0).getInsertionIds(
				indexedData);
	}

	@Override
	public double[] getHighestPrecisionIdRangePerDimension() {
		return parentStrategies.get(
				0).getHighestPrecisionIdRangePerDimension();
	}

	@Override
	public int getByteOffsetFromDimensionalIndex() {
		return parentStrategies.get(
				0).getByteOffsetFromDimensionalIndex();
	}

	@Override
	public List<ByteArrayId> getInsertionIds(
			final MultiDimensionalNumericData indexedData,
			final int maxEstimatedDuplicateIds ) {
		return parentStrategies.get(
				0).getInsertionIds(
				indexedData,
				maxEstimatedDuplicateIds);
	}

	@Override
	public MultiDimensionalNumericData getRangeForId(
			final ByteArrayId insertionId ) {
		return parentStrategies.get(
				0).getRangeForId(
				insertionId);
	}

	@Override
	public String getId() {
		return parentStrategies.get(
				0).getId();
	}

	@Override
	public Set<ByteArrayId> getNaturalSplits() {
		return parentStrategies.get(
				0).getNaturalSplits();
	}

	@Override
	public List<IndexMetaData> createMetaData() {
		return parentStrategies.get(
				0).createMetaData();
	}

	public static HierarchicalNumericIndexStrategy findHierarchicalStrategy(
			final NumericIndexStrategy indexStrategy ) {
		final List<CompoundIndexStrategy> parentStrategies = new ArrayList<CompoundIndexStrategy>();
		final HierarchicalNumericIndexStrategy firstHierarchicalStrategy = findHierarchicalStrategy(
				indexStrategy,
				parentStrategies);
		if (firstHierarchicalStrategy == null) {
			return null;
		}
		else if (parentStrategies.isEmpty()) {
			return firstHierarchicalStrategy;
		}
		else {
			return new CompoundHierarchicalIndexStrategyWrapper(
					parentStrategies,
					firstHierarchicalStrategy);
		}
	}

	public static HierarchicalNumericIndexStrategy findHierarchicalStrategy(
			final NumericIndexStrategy indexStrategy,
			final List<CompoundIndexStrategy> parentStrategies ) {
		if (indexStrategy instanceof HierarchicalNumericIndexStrategy) {
			return (HierarchicalNumericIndexStrategy) indexStrategy;
		}
		if (indexStrategy instanceof CompoundIndexStrategy) {
			final NumericIndexStrategy primaryIndex = ((CompoundIndexStrategy) indexStrategy).getPrimarySubStrategy();
			final NumericIndexStrategy secondaryIndex = ((CompoundIndexStrategy) indexStrategy)
					.getSecondarySubStrategy();
			// warn if round robin is used
			if (primaryIndex instanceof RoundRobinKeyIndexStrategy) {
				LOGGER.warn("Round Robin partitioning won't work correctly with raster merge strategies");
			}
			else if (secondaryIndex instanceof RoundRobinKeyIndexStrategy) {
				LOGGER.warn("Round Robin partitioning won't work correctly with raster merge strategies");
			}
			final HierarchicalNumericIndexStrategy primary = findHierarchicalStrategy(primaryIndex);

			if (primary != null) {
				// add it to beginning because we are recursing back from the
				// leaf strategy up to the parent
				parentStrategies.add(
						0,
						(CompoundIndexStrategy) indexStrategy);
				return primary;
			}
			final HierarchicalNumericIndexStrategy secondary = findHierarchicalStrategy(secondaryIndex);
			if (secondary != null) {
				// add it to beginning because we are recursing back from the
				// leaf strategy up to the parent
				parentStrategies.add(
						0,
						(CompoundIndexStrategy) indexStrategy);
				return secondary;
			}
		}
		return null;
	}

	@Override
	public MultiDimensionalCoordinates getCoordinatesPerDimension(
			ByteArrayId insertionId ) {
		return parentStrategies.get(
				0).getCoordinatesPerDimension(
				insertionId);
	}

	@Override
	public MultiDimensionalCoordinateRanges[] getCoordinateRangesPerDimension(
			MultiDimensionalNumericData dataRange,
			IndexMetaData... hints ) {
		return parentStrategies.get(
				0).getCoordinateRangesPerDimension(
				dataRange,
				hints);
	}
}
