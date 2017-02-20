package mil.nga.giat.geowave.adapter.vector.stats;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;
import org.opengis.feature.simple.SimpleFeature;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;

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
	private final static Logger LOGGER = Logger.getLogger(FeatureHyperLogLogStatistics.class);
	public static final String STATS_TYPE = "ATT_HYPERLLP";

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
			final String fieldName,
			final int precision ) {
		super(
				dataAdapterId,
				composeId(
						STATS_TYPE,
						fieldName));
		loglog = new HyperLogLogPlus(
				precision);
		this.precision = precision;
	}

	public static final ByteArrayId composeId(
			final String fieldName ) {
		return composeId(
				STATS_TYPE,
				fieldName);
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
			catch (final CardinalityMergeException e) {
				throw new RuntimeException(
						"Unable to merge counters",
						e);
			}
		}

	}

	@Override
	public byte[] toBinary() {

		try {
			final byte[] data = loglog.getBytes();

			final ByteBuffer buffer = super.binaryBuffer(4 + data.length);
			buffer.putInt(data.length);
			buffer.put(data);
			return buffer.array();
		}
		catch (final IOException e) {
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
		catch (final IOException e) {
			LOGGER.error(
					"Exception while reading statistic",
					e);
		}
	}

	@Override
	public void entryIngested(
			final SimpleFeature entry,
			final GeoWaveRow... rows ) {
		final Object o = entry.getAttribute(getFieldName());
		if (o == null) {
			return;
		}
		loglog.offer(o.toString());
	}

	@Override
	public String toString() {
		final StringBuffer buffer = new StringBuffer();
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
				final int precision ) {
			super();
			this.precision = precision;
		}

		public int getPrecision() {
			return precision;
		}

		public void setPrecision(
				final int precision ) {
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
