package mil.nga.giat.geowave.store.adapter.statistics;

import java.util.Date;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.query.TemporalRange;

abstract public class TimeRangeDataStatistics<T> extends
		NumericRangeDataStatistics<T>
{
	public final static String STATS_TYPE = "TIME_RANGE";

	protected TimeRangeDataStatistics() {
		super();
	}

	public TimeRangeDataStatistics(
			final ByteArrayId dataAdapterId,
			final String fieldId ) {
		super(
				dataAdapterId,
				composeId(
						STATS_TYPE,
						fieldId));
	}


	public TemporalRange asTemporalRange() {
		return new TemporalRange(
				new Date(
						(long) this.getMin()),
				new Date(
						(long) this.getMax()));
	}
}
