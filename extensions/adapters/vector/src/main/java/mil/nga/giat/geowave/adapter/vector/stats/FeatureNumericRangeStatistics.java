package mil.nga.giat.geowave.adapter.vector.stats;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.NumericRangeDataStatistics;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.opengis.feature.simple.SimpleFeature;

public class FeatureNumericRangeStatistics extends
		NumericRangeDataStatistics<SimpleFeature> implements
		FeatureStatistic
{
	public static final ByteArrayId STATS_TYPE = new ByteArrayId(
			"FEATURE_NUMERIC_RANGE");

	protected FeatureNumericRangeStatistics() {
		super();
	}

	public FeatureNumericRangeStatistics(
			final ByteArrayId dataAdapterId,
			final String fieldName ) {
		super(
				dataAdapterId,
				composeId(
						STATS_TYPE.getString(),
						fieldName));
	}

	public static final ByteArrayId composeId(
			final String fieldName ) {
		return composeId(
				STATS_TYPE.getString(),
				fieldName);
	}

	@Override
	public String getFieldName() {
		return decomposeNameFromId(getStatisticsId());
	}

	@Override
	protected NumericRange getRange(
			final SimpleFeature entry ) {

		final Object o = entry.getAttribute(getFieldName());
		if (o == null) return null;
		final long num = ((Number) o).longValue();
		return new NumericRange(
				num,
				num);
	}

	@Override
	public DataStatistics<SimpleFeature> duplicate() {
		return new FeatureNumericRangeStatistics(
				dataAdapterId,
				getFieldName());
	}

	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(
				"range[adapter=").append(
				super.getDataAdapterId().getString());
		buffer.append(
				", field=").append(
				getFieldName());
		if (isSet()) {
			buffer.append(
					", min=").append(
					getMin());
			buffer.append(
					", max=").append(
					getMax());
		}
		else {
			buffer.append(", No Values");
		}
		buffer.append("]");
		return buffer.toString();
	}

	/**
	 * Convert Feature Numeric Range statistics to a JSON object
	 */

	public JSONObject toJSONObject()
			throws JSONException {
		JSONObject jo = new JSONObject();
		jo.put(
				"type",
				STATS_TYPE.getString());
		jo.put(
				"field_identifier",
				getFieldName());

		if (!isSet()) {
			jo.put(
					"range",
					"No Values");
		}
		else {
			jo.put(
					"range_min",
					getMin());
			jo.put(
					"range_max",
					getMax());
		}

		return jo;
	}

	public static class FeatureNumericRangeConfig implements
			StatsConfig<SimpleFeature>
	{
		/**
		 * 
		 */
		private static final long serialVersionUID = 6309383518148391565L;

		@Override
		public DataStatistics<SimpleFeature> create(
				final ByteArrayId dataAdapterId,
				final String fieldName ) {
			return new FeatureNumericRangeStatistics(
					dataAdapterId,
					fieldName);
		}
	}
}
