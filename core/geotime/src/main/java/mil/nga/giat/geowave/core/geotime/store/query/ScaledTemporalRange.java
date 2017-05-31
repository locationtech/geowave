package mil.nga.giat.geowave.core.geotime.store.query;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class ScaledTemporalRange implements
		Serializable
{
	private static final long serialVersionUID = 1L;
	private static long MILLIS_PER_DAY = 86400000;
	private static long DEFAULT_TIME_RANGE = 365L * MILLIS_PER_DAY; // one year

	private Date startTime = null;
	private Date endTime = null;

	// Default to lat bounds
	private double minVal = 0.0;
	private double maxVal = 180.0;

	private long timeRange = DEFAULT_TIME_RANGE;
	private double timeScale;

	private Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

	public ScaledTemporalRange() {
		updateTimeScale();
	}

	public void setTimeRange(
			Date startTime,
			Date endTime ) {
		this.startTime = startTime;
		this.endTime = endTime;

		updateTimeScale();
	}

	public void setTimeRange(
			long millis ) {
		this.timeRange = millis;
		this.startTime = null;
		this.endTime = null;

		updateTimeScale();
	}

	public void setValueRange(
			double minVal,
			double maxVal ) {
		this.minVal = minVal;
		this.maxVal = maxVal;

		updateTimeScale();
	}

	public void setTimeScale(
			double timeScale ) {
		this.timeScale = timeScale;
	}

	private void updateTimeScale() {
		timeScale = (maxVal - minVal) / (double) getTimeRangeMillis();
	}

	public double getTimeScale() {
		return timeScale;
	}

	public long getTimeRangeMillis() {
		if (startTime == null || endTime == null) {
			return timeRange;
		}

		return endTime.getTime() - startTime.getTime();
	}

	public double timeToValue(
			Date time ) {
		long deltaTime = time.getTime() - getTimeMin();

		return minVal + ((double) deltaTime * timeScale);
	}

	public Date valueToTime(
			double timeVal ) {
		long timeMillis = (long) (timeVal / timeScale) + getTimeMin();
		cal.setTimeInMillis(timeMillis);

		return cal.getTime();
	}

	private long getTimeMin() {
		if (startTime != null) {
			return startTime.getTime();
		}

		return 0L;
	}

	public Date getStartTime() {
		return startTime;
	}

	public void setStartTime(
			Date startTime ) {
		this.startTime = startTime;
	}

	public Date getEndTime() {
		return endTime;
	}

	public void setEndTime(
			Date endTime ) {
		this.endTime = endTime;
	}
}
