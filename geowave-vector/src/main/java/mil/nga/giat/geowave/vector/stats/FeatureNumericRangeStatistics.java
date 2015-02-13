package mil.nga.giat.geowave.vector.stats;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.store.adapter.statistics.NumericRangeDataStatistics;

import org.opengis.feature.simple.SimpleFeature;

public class FeatureNumericRangeStatistics extends
		NumericRangeDataStatistics<SimpleFeature> implements
		FeatureStatistic
{
	public static final String STATS_TYPE = "RANGE";

	protected FeatureNumericRangeStatistics() {
		super();
	}

	public FeatureNumericRangeStatistics(
			final ByteArrayId dataAdapterId,
			final String fieldName ) {
		super(
				dataAdapterId,
				composeId(
						STATS_TYPE,
						fieldName));
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

}
