package mil.nga.giat.geowave.format.stanag4676;

import java.io.Serializable;
import java.util.Comparator;

public class ComparatorTrackEventWritable implements
		Comparator<TrackEventWritable>,
		Serializable
{

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(
			final TrackEventWritable obj1,
			final TrackEventWritable obj2 ) {
		return obj1.TimeStamp.compareTo(obj2.TimeStamp);
	}
}
