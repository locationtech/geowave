package mil.nga.giat.geowave.core.store.adapter.statistics;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.ByteUtils;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.MinimalBinDistanceHistogram.MinimalBinDistanceHistogramFactory;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.NumericHistogram;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.NumericHistogramFactory;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

/**
 * Dynamic histogram provide very high accuracy for CDF and quantiles over the a
 * numeric attribute.
 * 
 */
public class RowRangeHistogramStatistics<T> extends
		AbstractDataStatistics<T>
{
	public static final ByteArrayId STATS_TYPE = new ByteArrayId(
			"ROW_RANGE_HISTOGRAM");
	private static final NumericHistogramFactory HistFactory = new MinimalBinDistanceHistogramFactory();
	NumericHistogram histogram = HistFactory.create(1024);

	protected RowRangeHistogramStatistics() {
		super();
	}

	public RowRangeHistogramStatistics(
			final ByteArrayId dataAdapterId,
			final ByteArrayId statisticsId ) {
		super(
				dataAdapterId,
				composeId(statisticsId));
	}

	public RowRangeHistogramStatistics(
			final ByteArrayId dataAdapterId,
			final ByteArrayId indexId,
			NumericHistogramFactory factory,
			int bins ) {
		super(
				dataAdapterId,
				composeId(indexId));
		histogram = factory.create(bins);
	}

	public RowRangeHistogramStatistics(
			final ByteArrayId dataAdapterId,
			final ByteArrayId statisticsId,
			int bins ) {
		super(
				dataAdapterId,
				composeId(statisticsId));
		histogram = HistFactory.create(bins);
	}

	public static ByteArrayId composeId(
			ByteArrayId statisticsId ) {
		return composeId(
				STATS_TYPE.getString(),
				statisticsId.getString());
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
		// Need to account for length of type and of the separator
		int lengthOfNonId = STATS_TYPE.getBytes().length + STATS_ID_SEPARATOR.length();
		int idLength = id.getBytes().length - lengthOfNonId;
		byte[] idBytes = new byte[idLength];
		System.arraycopy(
				id.getBytes(),
				lengthOfNonId,
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

	public double[] quantile(
			final int bins ) {
		final double[] result = new double[bins];
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

	public double quantile(
			final double percentage ) {
		return histogram.quantile((percentage));
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
		for (double v : this.quantile(10)) {
			buffer.append(v);
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

	/**
	 * Convert Row Range Numeric statistics to a JSON object
	 */

	public JSONObject toJSONObject()
			throws JSONException {
		JSONObject jo = new JSONObject();
		jo.put(
				"type",
				STATS_TYPE.getString());

		jo.put(
				"statisticsID",
				statisticsId.getString());

		jo.put(
				"range_min",
				histogram.getMinValue());
		jo.put(
				"range_max",
				histogram.getMaxValue());
		jo.put(
				"totalCount",
				histogram.getTotalCount());
		JSONArray binsArray = new JSONArray();
		for (final double v : this.quantile(10)) {
			binsArray.add(v);
		}
		jo.put(
				"bins",
				binsArray);

		JSONArray countsArray = new JSONArray();
		for (final long v : count(10)) {
			countsArray.add(v);
		}
		jo.put(
				"counts",
				countsArray);

		return jo;
	}

}
