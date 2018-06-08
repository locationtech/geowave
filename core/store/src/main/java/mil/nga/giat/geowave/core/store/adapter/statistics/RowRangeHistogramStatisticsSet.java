package mil.nga.giat.geowave.core.store.adapter.statistics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.ByteUtils;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;

/**
 * 
 * This class really just needs to get ingest callbacks and collect individual
 * RowRangeHistogramStatistics per partition. It should never be persisted as
 * is, and is only a DataStatistic to match the current interfaces for the
 * lowest impact mechanism to store histograms per partition instead of all
 * together
 *
 * @param <T>
 *            The type of the row to keep statistics on
 */
public class RowRangeHistogramStatisticsSet<T> extends
		AbstractDataStatistics<T> implements
		DataStatisticsSet<T>
{
	private Map<ByteArrayId, RowRangeHistogramStatistics<T>> histogramPerPartition = new HashMap<>();

	public RowRangeHistogramStatisticsSet() {
		super();
	}

	public RowRangeHistogramStatisticsSet(
			ByteArrayId dataAdapterId,
			ByteArrayId indexId ) {
		super(
				dataAdapterId,
				indexId);
	}

	private synchronized RowRangeHistogramStatistics<T> getPartitionStatistic(
			final ByteArrayId partitionKey ) {
		RowRangeHistogramStatistics<T> histogram = histogramPerPartition.get(partitionKey);
		if (histogram == null) {
			histogram = new RowRangeHistogramStatistics<>(
					dataAdapterId,
					statisticsId,
					partitionKey);
			histogramPerPartition.put(
					partitionKey,
					histogram);
		}
		return histogram;
	}

	@Override
	public void merge(
			Mergeable merge ) {
		throw new UnsupportedOperationException(
				"Merge should never be called");
	}

	@Override
	public byte[] toBinary() {
		throw new UnsupportedOperationException(
				"toBinary should never be called");
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {
		throw new UnsupportedOperationException(
				"fromBinary should never be called");
	}

	@Override
	public void entryIngested(
			T entry,
			GeoWaveRow... rows ) {
		if (rows != null) {
			// call entry ingested once per row
			for (final GeoWaveRow row : rows) {
				getPartitionStatistic(
						getPartitionKey(row.getPartitionKey())).entryIngested(
						entry,
						row);
			}
		}
	}

	@Override
	public DataStatistics<T>[] getStatisticsSet() {
		return histogramPerPartition.values().toArray(
				new DataStatistics[histogramPerPartition.size()]);
	}

	protected static ByteArrayId getPartitionKey(
			final byte[] partitionBytes ) {
		return ((partitionBytes == null) || (partitionBytes.length == 0)) ? null : new ByteArrayId(
				partitionBytes);
	}

}
