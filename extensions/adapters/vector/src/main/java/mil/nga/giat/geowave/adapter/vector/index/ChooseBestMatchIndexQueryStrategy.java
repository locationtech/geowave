package mil.nga.giat.geowave.adapter.vector.index;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.core.store.query.BasicQuery;

import org.opengis.feature.simple.SimpleFeature;

public class ChooseBestMatchIndexQueryStrategy implements
		IndexQueryStrategySPI
{
	public static final String NAME = "Best Match";

	@Override
	public String toString() {
		return NAME;
	}

	/**
	 * Constraints that are empty indicate full table scan. A full table scan
	 * occurs if ANY one dimension is unbounded.
	 * 
	 * @param constraints
	 * @return true if any one dimension is unbounded
	 */
	public static final boolean isFullTableScan(
			final List<MultiDimensionalNumericData> constraints ) {
		for (final MultiDimensionalNumericData constraint : constraints) {
			if (constraint.isEmpty()) {
				return false;
			}
		}
		return constraints.isEmpty();
	}

	@Override
	public CloseableIterator<Index<?, ?>> getIndices(
			final Map<ByteArrayId, DataStatistics<SimpleFeature>> stats,
			final BasicQuery query,
			final CloseableIterator<Index<?, ?>> indices ) {
		return new CloseableIterator<Index<?, ?>>() {
			PrimaryIndex nextIdx = null;
			boolean done = false;

			@Override
			public boolean hasNext() {
				long min = Long.MAX_VALUE;
				PrimaryIndex bestIdx = null;

				while (!done && indices.hasNext()) {
					final Index<?, ?> nextChoosenIdx = indices.next();
					if (nextChoosenIdx instanceof PrimaryIndex) {
						nextIdx = (PrimaryIndex) nextChoosenIdx;
						final List<MultiDimensionalNumericData> constraints = query.getIndexConstraints(nextIdx.getIndexStrategy());
						if (isFullTableScan(constraints)) {
							// keep this is as a default in case all indices
							// result in a full table scan
							if (bestIdx == null) {
								bestIdx = nextIdx;
							}
						}
						else {
							final List<ByteArrayRange> ranges = DataStoreUtils.constraintsToByteArrayRanges(
									constraints,
									nextIdx.getIndexStrategy(),
									5000);
							final long temp = DataStoreUtils.cardinality(
									nextIdx,
									stats,
									ranges);
							if (temp < min) {
								bestIdx = nextIdx;
								min = temp;
							}
						}
					}
				}
				nextIdx = bestIdx;
				done = true;
				return nextIdx != null;
			}

			@Override
			public Index<?, ?> next()
					throws NoSuchElementException {
				if (nextIdx == null) {
					throw new NoSuchElementException();
				}
				final Index<?, ?> returnVal = nextIdx;
				nextIdx = null;
				return returnVal;
			}

			@Override
			public void remove() {}

			@Override
			public void close()
					throws IOException {
				indices.close();
			}
		};
	}

	private static boolean isSpatialTemporal(
			final PrimaryIndex index ) {
		if ((index == null) || (index.getIndexStrategy() == null) || (index.getIndexStrategy().getOrderedDimensionDefinitions() == null)) {
			return false;
		}
		final NumericDimensionDefinition[] dimensions = index.getIndexStrategy().getOrderedDimensionDefinitions();
		if (dimensions.length < 3) {
			return false;
		}
		boolean hasLat = false, hasLon = false, hasTime = false;
		for (final NumericDimensionDefinition definition : dimensions) {
			if (definition instanceof TimeDefinition) {
				hasTime = true;
			}
			else if (definition instanceof LatitudeDefinition) {
				hasLat = true;
			}
			else if (definition instanceof LongitudeDefinition) {
				hasLon = true;
			}
		}
		return hasTime && hasLat && hasLon;
	}

	private static boolean isSpatial(
			final PrimaryIndex index ) {
		if ((index == null) || (index.getIndexStrategy() == null) || (index.getIndexStrategy().getOrderedDimensionDefinitions() == null)) {
			return false;
		}
		final NumericDimensionDefinition[] dimensions = index.getIndexStrategy().getOrderedDimensionDefinitions();
		if (dimensions.length != 2) {
			return false;
		}
		boolean hasLat = false, hasLon = false;
		for (final NumericDimensionDefinition definition : dimensions) {
			if (definition instanceof LatitudeDefinition) {
				hasLat = true;
			}
			else if (definition instanceof LongitudeDefinition) {
				hasLon = true;
			}
		}
		return hasLat && hasLon;
	}
}
