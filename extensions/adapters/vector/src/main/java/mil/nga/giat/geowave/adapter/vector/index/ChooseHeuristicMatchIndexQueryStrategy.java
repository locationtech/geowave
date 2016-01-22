package mil.nga.giat.geowave.adapter.vector.index;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.IndexUtils;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
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
				double indexMax = -1;
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
								final double[] dataRangePerDimension = new double[qr.getDimensionCount()];
								for (int d = 0; d < dataRangePerDimension.length; d++) {
									dataRangePerDimension[d] = qr.getMaxValuesPerDimension()[d] - qr.getMinValuesPerDimension()[d];
								}
								totalMax += IndexUtils.getDimensionalBitsUsed(
										nextIdx.getIndexStrategy(),
										dataRangePerDimension);
							}
							if (totalMax > indexMax) {
								indexMax = totalMax;
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
}
