package mil.nga.giat.geowave.format.stanag4676.parser.model;

public class MotionEventPoint extends
		TrackPoint
{
	public long eventEndTime;

	public long getEndTime() {
		return eventEndTime;
	}

	public void setEndTime(
			long eventEndTime ) {
		this.eventEndTime = eventEndTime;
	}
}
