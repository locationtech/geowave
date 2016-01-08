package mil.nga.giat.geowave.adapter.vector.index;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.BasicQuery;

import org.opengis.feature.simple.SimpleFeature;

public class ChooseHeuristicMatchIndexQueryStrategy implements
		IndexQueryStrategySPI
{
	public static final String NAME = "Heuristic Match";

	@Override
	public String toString() {
		return NAME;
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
				double min = Long.MAX_VALUE;
				PrimaryIndex bestIdx = null;
				while (!done && indices.hasNext()) {
					final Index<?, ?> nextChoosenIdx = indices.next();
					if (nextChoosenIdx instanceof PrimaryIndex) {
						nextIdx = (PrimaryIndex) nextChoosenIdx;
						final List<MultiDimensionalNumericData> queryRanges = query.getIndexConstraints(nextIdx.getIndexStrategy());
						if (ChooseBestMatchIndexQueryStrategy.isFullTableScan(queryRanges)) {
							// keep this is as a default in case all indices
							// result in a full table scan
							if (bestIdx == null) {
								bestIdx = nextIdx;
							}
						}
						else {
							double totalMax = 0;
							for (final MultiDimensionalNumericData qr : queryRanges) {
								totalMax += getCostLogic(
										nextIdx.getIndexStrategy(),
										nextIdx.getIndexModel(),
										qr);
							}
							if (totalMax < min) {
								min = totalMax;
								bestIdx = nextIdx;
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

	private static final double getCostLogic(
			final NumericIndexStrategy indexStrategy,
			final CommonIndexModel indexModel,
			final MultiDimensionalNumericData data ) {
		double result = Long.MAX_VALUE;
		final double rangePerDimension[] = indexStrategy.getHighestPrecisionIdRangePerDimension();
		final double bitsPerDimension[] = getBitsPerDimension(
				indexModel,
				rangePerDimension);
		for (int d = 0; d < rangePerDimension.length; d++) {
			final double range = (data.getMaxValuesPerDimension()[d] - data.getMinValuesPerDimension()[d]);
			// ratio of bits used to bits required
			final double temp = log2(Math.ceil(range / rangePerDimension[d])) / bitsPerDimension[d];
			result = Math.min(
					temp,
					result);
		}
		return result * rangePerDimension.length;
	}

	private static final double[] getBitsPerDimension(
			final CommonIndexModel indexModel,
			final double[] rangePerDimension ) {
		final NumericDimensionField dim[] = indexModel.getDimensions();
		double result[] = new double[rangePerDimension.length];
		for (int d = 0; d < rangePerDimension.length; d++) {
			result[d] += Math.ceil(log2((dim[d].getRange() / rangePerDimension[d])));
		}
		return result;
	}

	private static double log2(
			double v ) {
		return Math.log(v) / Math.log(2);
	}
}
