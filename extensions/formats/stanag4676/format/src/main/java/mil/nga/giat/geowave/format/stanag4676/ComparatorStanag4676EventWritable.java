package mil.nga.giat.geowave.format.stanag4676;

import java.io.Serializable;
import java.util.Comparator;

public class ComparatorStanag4676EventWritable implements
		Comparator<Stanag4676EventWritable>,
		Serializable
{

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(
			final Stanag4676EventWritable obj1,
			final Stanag4676EventWritable obj2 ) {
		return obj1.TimeStamp.compareTo(obj2.TimeStamp);
	}
}
