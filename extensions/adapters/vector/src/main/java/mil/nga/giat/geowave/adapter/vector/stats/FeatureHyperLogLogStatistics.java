package mil.nga.giat.geowave.adapter.vector.stats;

import java.io.IOException;
import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opengis.feature.simple.SimpleFeature;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

/**
 * Hyperloglog provides an estimated cardinality of the number of unique values
 * for an attribute.
 * 
 * 
 */
public class FeatureHyperLogLogStatistics extends
		AbstractDataStatistics<SimpleFeature> implements
		FeatureStatistic
{
	private final static Logger LOGGER = LoggerFactory.getLogger(FeatureHyperLogLogStatistics.class);
	public static final ByteArrayId STATS_TYPE = new ByteArrayId(
			"ATT_HYPERLLP");

	private HyperLogLogPlus loglog;
	private int precision;

	protected FeatureHyperLogLogStatistics() {
		super();
	}

	/**
	 * 
	 * @param dataAdapterId
	 * @param fieldName
	 * @param precision
	 *            number of bits to support counting. 2^p is the maximum count
	 *            value per distinct value. 1 <= p <= 32
	 */
	public FeatureHyperLogLogStatistics(
			final ByteArrayId dataAdapterId,
			final String statisticsId,
			int precision ) {
		super(
				dataAdapterId,
				composeId(
						STATS_TYPE.getString(),
						statisticsId));
		loglog = new HyperLogLogPlus(
				precision);
		this.precision = precision;
	}

	public static final ByteArrayId composeId(
			final String statisticsId ) {
		return composeId(
				STATS_TYPE.getString(),
				statisticsId);
	}

	@Override
	public String getFieldName() {
		return decomposeNameFromId(getStatisticsId());
	}

	@Override
	public DataStatistics<SimpleFeature> duplicate() {
		return new FeatureHyperLogLogStatistics(
				dataAdapterId,
				getFieldName(),
				precision);
	}

	public long cardinality() {
		return loglog.cardinality();
	}

	@Override
	public void merge(
			final Mergeable mergeable ) {
		if (mergeable instanceof FeatureHyperLogLogStatistics) {
			try {
				loglog = (HyperLogLogPlus) ((FeatureHyperLogLogStatistics) mergeable).loglog.merge(loglog);
			}
			catch (CardinalityMergeException e) {
				throw new RuntimeException(
						"Unable to merge counters",
						e);
			}
		}

	}

	@Override
	public byte[] toBinary() {

		try {
			byte[] data = loglog.getBytes();

			final ByteBuffer buffer = super.binaryBuffer(4 + data.length);
			buffer.putInt(data.length);
			buffer.put(data);
			return buffer.array();
		}
		catch (IOException e) {
			LOGGER.error(
					"Exception while writing statistic",
					e);
		}
		return new byte[0];
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = super.binaryBuffer(bytes);
		final byte[] data = new byte[buffer.getInt()];
		buffer.get(data);
		try {
			loglog = HyperLogLogPlus.Builder.build(data);
		}
		catch (IOException e) {
			LOGGER.error(
					"Exception while reading statistic",
					e);
		}
	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			final SimpleFeature entry ) {
		final Object o = entry.getAttribute(getFieldName());
		if (o == null) {
			return;
		}
		loglog.offer(o.toString());
	}

	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(
				"hyperloglog[adapter=").append(
				super.getDataAdapterId().getString());
		buffer.append(
				", field=").append(
				getFieldName());
		buffer.append(
				", cardinality=").append(
				loglog.cardinality());
		buffer.append("]");
		return buffer.toString();
	}

	/**
	 * Convert FeatureCountMinSketch statistics to a JSON object
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
				"field_identifier",
				getFieldName());

		jo.put(
				"cardinality",
				loglog.cardinality());

		jo.put(
				"precision",
				precision);

		return jo;
	}

	public static class FeatureHyperLogLogConfig implements
			StatsConfig<SimpleFeature>
	{
		/**
		 * 
		 */
		private static final long serialVersionUID = 6309383518148391565L;
		private int precision = 16;

		public FeatureHyperLogLogConfig() {

		}

		public FeatureHyperLogLogConfig(
				int precision ) {
			super();
			this.precision = precision;
		}

		public int getPrecision() {
			return precision;
		}

		public void setPrecision(
				int precision ) {
			this.precision = precision;
		}

		@Override
		public DataStatistics<SimpleFeature> create(
				final ByteArrayId dataAdapterId,
				final String fieldName ) {
			return new FeatureHyperLogLogStatistics(
					dataAdapterId,
					fieldName,
					precision);
		}
	}

}
