package mil.nga.giat.geowave.adapter.vector.stats;

import java.nio.ByteBuffer;

import org.opengis.feature.simple.SimpleFeature;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.clearspring.analytics.stream.frequency.FrequencyMergeException;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;

/**
 *
 * Maintains an estimate of how may of each attribute value occurs in a set of
 * data.
 *
 * Default values:
 *
 * Error factor of 0.001 with probability 0.98 of retrieving a correct estimate.
 * The Algorithm does not under-state the estimate.
 *
 */
public class FeatureCountMinSketchStatistics extends
		AbstractDataStatistics<SimpleFeature> implements
		FeatureStatistic
{
	public static final String STATS_TYPE = "ATT_SKETCH";
	private CountMinSketch sketch = null;;

	protected FeatureCountMinSketchStatistics() {
		super();
		sketch = new CountMinSketch(
				0.001,
				0.98,
				7364181);
	}

	public FeatureCountMinSketchStatistics(
			final ByteArrayId dataAdapterId,
			final String fieldName ) {
		super(
				dataAdapterId,
				composeId(
						STATS_TYPE,
						fieldName));
		sketch = new CountMinSketch(
				0.001,
				0.98,
				7364181);
	}

	public FeatureCountMinSketchStatistics(
			final ByteArrayId dataAdapterId,
			final String fieldName,
			final double errorFactor,
			final double probabilityOfCorrectness ) {
		super(
				dataAdapterId,
				composeId(
						STATS_TYPE,
						fieldName));
		sketch = new CountMinSketch(
				errorFactor,
				probabilityOfCorrectness,
				7364181);
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
		return new FeatureCountMinSketchStatistics(
				dataAdapterId,
				getFieldName());
	}

	public long totalSampleSize() {
		return sketch.size();
	}

	public long count(
			final String item ) {
		return sketch.estimateCount(item);
	}

	@Override
	public void merge(
			final Mergeable mergeable ) {
		if (mergeable instanceof FeatureCountMinSketchStatistics) {
			try {
				sketch = CountMinSketch.merge(
						sketch,
						((FeatureCountMinSketchStatistics) mergeable).sketch);
			}
			catch (final FrequencyMergeException e) {
				throw new RuntimeException(
						"Unable to merge sketches",
						e);
			}
		}

	}

	@Override
	public byte[] toBinary() {
		final byte[] data = CountMinSketch.serialize(sketch);
		final ByteBuffer buffer = super.binaryBuffer(4 + data.length);
		buffer.putInt(data.length);
		buffer.put(data);
		return buffer.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = super.binaryBuffer(bytes);
		final byte[] data = new byte[buffer.getInt()];
		buffer.get(data);
		sketch = CountMinSketch.deserialize(data);
	}

	@Override
	public void entryIngested(
			final SimpleFeature entry,
			final GeoWaveRow... rows ) {
		final Object o = entry.getAttribute(getFieldName());
		if (o == null) {
			return;
		}
		sketch.add(
				o.toString(),
				1);
	}

	@Override
	public String toString() {
		final StringBuffer buffer = new StringBuffer();
		buffer.append(
				"sketch[adapter=").append(
				super.getDataAdapterId().getString());
		buffer.append(
				", field=").append(
				getFieldName());
		buffer.append(
				", size=").append(
				sketch.size());
		buffer.append("]");
		return buffer.toString();
	}

	public static class FeatureCountMinSketchConfig implements
			StatsConfig<SimpleFeature>
	{
		/**
		 *
		 */
		private static final long serialVersionUID = 6309383518148391565L;
		private double errorFactor;
		private double probabilityOfCorrectness;

		public FeatureCountMinSketchConfig() {

		}

		public FeatureCountMinSketchConfig(
				final double errorFactor,
				final double probabilityOfCorrectness ) {
			super();
			this.errorFactor = errorFactor;
			this.probabilityOfCorrectness = probabilityOfCorrectness;
		}

		public void setErrorFactor(
				final double errorFactor ) {
			this.errorFactor = errorFactor;
		}

		public void setProbabilityOfCorrectness(
				final double probabilityOfCorrectness ) {
			this.probabilityOfCorrectness = probabilityOfCorrectness;
		}

		public double getErrorFactor() {
			return errorFactor;
		}

		public double getProbabilityOfCorrectness() {
			return probabilityOfCorrectness;
		}

		@Override
		public DataStatistics<SimpleFeature> create(
				final ByteArrayId dataAdapterId,
				final String fieldName ) {
			return new FeatureCountMinSketchStatistics(
					dataAdapterId,
					fieldName,
					errorFactor,
					probabilityOfCorrectness);
		}
	}
}