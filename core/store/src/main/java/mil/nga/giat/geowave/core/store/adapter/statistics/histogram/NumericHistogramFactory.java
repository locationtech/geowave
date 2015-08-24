package mil.nga.giat.geowave.core.store.adapter.statistics.histogram;

public interface NumericHistogramFactory
{
	public NumericHistogram create(
			int bins );

	public NumericHistogram create(
			int bins,
			double minValue,
			double maxValue );
}