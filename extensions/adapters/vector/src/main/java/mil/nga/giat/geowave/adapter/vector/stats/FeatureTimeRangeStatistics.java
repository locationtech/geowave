package mil.nga.giat.geowave.adapter.vector.stats;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import mil.nga.giat.geowave.core.geotime.store.statistics.TimeRangeDataStatistics;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;

import org.opengis.feature.simple.SimpleFeature;

public class FeatureTimeRangeStatistics extends
		TimeRangeDataStatistics<SimpleFeature> implements
		FeatureStatistic
{

	protected FeatureTimeRangeStatistics() {
		super();
	}

	public FeatureTimeRangeStatistics(
			final ByteArrayId dataAdapterId,
			final String fieldName ) {
		super(
				dataAdapterId,
				fieldName);
	}

	public static final ByteArrayId composeId(
			final String fieldName ) {
		return composeId(
				STATS_TYPE.getString(),
				fieldName);
	}

	@Override
	public String getFieldName() {
		return decomposeNameFromId(this.getStatisticsId());
	}

	public Date getMaxTime() {
		Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		c.setTimeInMillis((long) this.getMax());
		return c.getTime();
	}

	public Date getMinTime() {
		Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		c.setTimeInMillis((long) this.getMin());
		return c.getTime();
	}

	@Override
	protected NumericRange getRange(
			final SimpleFeature entry ) {

		final Object o = entry.getAttribute(getFieldName());
		Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		if (o == null) return null;
		if (o instanceof Date) {
			c.setTime((Date) o);
		}
		else if (o instanceof Calendar) {
			c.setTime(((Calendar) c).getTime());
		}
		else if (o instanceof Number) {
			c.setTimeInMillis(((Number) o).longValue());
		}
		final long time = c.getTimeInMillis();
		return new NumericRange(
				time,
				time);
	}

	public DataStatistics<SimpleFeature> duplicate() {
		return new FeatureTimeRangeStatistics(
				this.dataAdapterId,
				getFieldName());
	}

	public String toString() {
		StringBuffer buffer = new StringBuffer();
		Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		c.setTimeInMillis((long) getMin());
		Date min = c.getTime();
		c.setTimeInMillis((long) getMax());
		Date max = c.getTime();
		buffer.append(
				"range[adapter=").append(
				super.getDataAdapterId().getString());
		buffer.append(
				", field=").append(
				getFieldName());
		if (isSet()) {
			buffer.append(
					", min=").append(
					min);
			buffer.append(
					", max=").append(
					max);

		}
		else {
			buffer.append(", No Values");
		}
		buffer.append("]");
		return buffer.toString();
	}
}
