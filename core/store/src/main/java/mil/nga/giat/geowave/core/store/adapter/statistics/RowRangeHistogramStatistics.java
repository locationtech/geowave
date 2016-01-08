package mil.nga.giat.geowave.core.store.adapter.statistics;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.ByteUtils;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.NumericHistogram;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.NumericHistogramFactory;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.MinimalBinDistanceHistogram.MinimalBinDistanceHistogramFactory;

import org.apache.commons.lang3.ArrayUtils;

/**
 * Dynamic histogram provide very high accuracy for CDF and quantiles over the a
 * numeric attribute.
 * 
 */
public class RowRangeHistogramStatistics<T> extends
		AbstractDataStatistics<T>
{
	public static final ByteArrayId STATS_ID = new ByteArrayId(
			"RANGE_HISTOGRAM_");
	private static final NumericHistogramFactory HistFactory = new MinimalBinDistanceHistogramFactory();
	NumericHistogram histogram = HistFactory.create(1024);

	protected RowRangeHistogramStatistics() {
		super();
	}

	public RowRangeHistogramStatistics(
			final ByteArrayId adapterId,
			final ByteArrayId indexId ) {
		super(
				adapterId,
				composeId(indexId));
	}

	public RowRangeHistogramStatistics(
			final ByteArrayId adapterId,
			final ByteArrayId indexId,
			NumericHistogramFactory factory,
			int bins ) {
		super(
				adapterId,
				composeId(indexId));
		histogram = factory.create(bins);
	}

	public RowRangeHistogramStatistics(
			final ByteArrayId adapterId,
			final ByteArrayId indexId,
			int bins ) {
		super(
				adapterId,
				composeId(indexId));
		histogram = HistFactory.create(bins);
	}

	public static ByteArrayId composeId(
			ByteArrayId indexId ) {
		return new ByteArrayId(
				ArrayUtils.addAll(
						STATS_ID.getBytes(),
						indexId.getBytes()));
	}

	@Override
	public DataStatistics<T> duplicate() {
		return new RowRangeHistogramStatistics<T>(
				dataAdapterId,
				decomposeFromId(statisticsId),
				this.histogram.getNumBins());// indexId
	}

	public static ByteArrayId decomposeFromId(
			final ByteArrayId id ) {
		int idLength = id.getBytes().length - STATS_ID.getBytes().length;
		byte[] idBytes = new byte[idLength];
		System.arraycopy(
				id.getBytes(),
				STATS_ID.getBytes().length,
				idBytes,
				0,
				idLength);
		return new ByteArrayId(
				idBytes);
	}

	public boolean isSet() {
		return false;
	}

	public double cardinality(
			byte[] start,
			byte[] end ) {
		return this.histogram.sum(
				ByteUtils.toDouble(end),
				true) - this.histogram.sum(
				ByteUtils.toDouble(start),
				false);
	}

	public byte[][] quantile(
			final int bins ) {
		final byte[][] result = new byte[bins][];
		final double binSize = 1.0 / bins;
		for (int bin = 0; bin < bins; bin++) {
			result[bin] = quantile(binSize * (bin + 1));
		}
		return result;
	}

	public long[] count(
			final int bins ) {
		return histogram.count(bins);
	}

	public double cdf(
			final byte[] id ) {
		return cdf(ByteUtils.toDouble(id));
	}

	private double cdf(
			double val ) {
		return histogram.cdf(val);
	}

	public byte[] quantile(
			final double percentage ) {
		return ByteUtils.toBytes(histogram.quantile((percentage)));
	}

	public double percentPopulationOverRange(
			final byte[] start,
			final byte[] stop ) {
		return cdf(stop) - cdf(start);
	}

	public long getLeftMostCount() {
		return (long) Math.ceil(histogram.sum(
				histogram.getMinValue(),
				true));
	}

	public long totalSampleSize() {
		return histogram.getTotalCount();
	}

	@Override
	public void merge(
			final Mergeable mergeable ) {
		if (mergeable instanceof RowRangeHistogramStatistics) {
			histogram.merge(((RowRangeHistogramStatistics<?>) mergeable).histogram);
		}
	}

	@Override
	public byte[] toBinary() {

		final ByteBuffer buffer = super.binaryBuffer(histogram.bufferSize() + 5);
		// buffer out an
		histogram.toBinary(buffer);
		return buffer.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = super.binaryBuffer(bytes);
		histogram.fromBinary(buffer);
	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		for (final ByteArrayId ids : entryInfo.getRowIds()) {
			final byte[] idBytes = ids.getBytes();
			add(ByteUtils.toDouble(idBytes));

		}
	}

	protected void add(
			double num ) {
		histogram.add(
				1,
				num);
	}

	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(
				"histogram[index=").append(
				super.statisticsId.getString());
		buffer.append(", bins={");
		for (byte[] v : this.quantile(10)) {
			buffer.append(ByteUtils.toDouble(v));
			buffer.append(' ');
		}
		buffer.deleteCharAt(buffer.length() - 1);
		buffer.append(", counts={");
		for (long v : this.count(10)) {
			buffer.append(
					v).append(
					' ');
		}
		buffer.deleteCharAt(buffer.length() - 1);
		buffer.append("}]");
		buffer.append("}]");
		return buffer.toString();
	}

}
