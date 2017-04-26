package mil.nga.giat.geowave.core.geotime;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains a set of Temporal utility methods that are generally
 * useful throughout the GeoWave core codebase.
 */
public class TimeUtils
{
	private final static Logger LOGGER = LoggerFactory.getLogger(TimeUtils.class);

	/**
	 * Convert a calendar object to a long in the form of milliseconds since the
	 * epoch of January 1, 1970. The time is converted to GMT if it is not
	 * already in that timezone so that all times will be in a standard
	 * timezone.
	 * 
	 * @param cal
	 *            The calendar object
	 * @return The time in milliseconds
	 */
	public static long calendarToGMTMillis(
			Calendar cal ) {
		// get Date object representing this Calendar's time value, millisecond
		// offset from the Epoch, January 1, 1970 00:00:00.000 GMT (Gregorian)
		Date date = cal.getTime();
		// Returns the number of milliseconds since January 1, 1970, 00:00:00
		// GMT represented by this Date object.
		long time = date.getTime();
		return time;
	}

	/**
	 * Get the time in millis of this temporal object (either numeric
	 * interpreted as millisecond time in GMT, Date, or Calendar)
	 * 
	 * @param timeObj
	 *            The temporal object
	 * @return The time in milliseconds since the epoch in GMT
	 */
	public static long getTimeMillis(
			Object timeObj ) {
		// handle dates, calendars, and Numbers only
		if (timeObj != null) {
			if (timeObj instanceof Calendar) {
				return calendarToGMTMillis(((Calendar) timeObj));
			}
			else if (timeObj instanceof Date) {
				return ((Date) timeObj).getTime();
			}
			else if (timeObj instanceof Number) {
				return ((Number) timeObj).longValue();
			}
			else {
				LOGGER.warn("Time value '" + timeObj + "' of type '" + timeObj.getClass()
						+ "' is not of expected temporal type");
			}
		}
		return -1;
	}

	/**
	 * Determine if this class is a supported temporal class. Numeric classes
	 * are not determined to be temporal in this case even though they can be
	 * interpreted as milliseconds because we do not want to be over-selective
	 * and mis-interpret numeric fields
	 * 
	 * @param bindingClass
	 *            The binding class of the attribute
	 * @return A flag indicating whether the class is temporal
	 */
	public static boolean isTemporal(
			Class<?> bindingClass ) {
		// because Longs can also be numeric, just allow Dates and Calendars
		// class bindings to be temporal
		return (Calendar.class.isAssignableFrom(bindingClass) || Date.class.isAssignableFrom(bindingClass));
	}

	/**
	 * Instantiates the class type as a new object with the temporal value being
	 * the longVal interpreted as milliseconds since the epoch in GMT
	 * 
	 * @param bindingClass
	 *            The class to try to instantiate for this time value. Currently
	 *            java.util.Calendar, java.util.Date, and java.lang.Long are
	 *            supported.
	 * @param longVal
	 *            A value to be interpreted as milliseconds since the epoch in
	 *            GMT
	 * @return An instance of the binding class with the value interpreted from
	 *         longVal
	 */
	public static Object getTimeValue(
			Class<?> bindingClass,
			long longVal ) {
		if (longVal < 0) {
			// indicator that the time value is null;
			return null;
		}
		if (Calendar.class.isAssignableFrom(bindingClass)) {
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
			cal.setTimeInMillis(longVal);
			return cal;
		}
		else if (Date.class.isAssignableFrom(bindingClass)) {
			return new Date(
					longVal);
		}
		else if (Long.class.isAssignableFrom(bindingClass)) {
			return Long.valueOf(longVal);
		}
		LOGGER.warn("Numeric value '" + longVal + "' of type '" + bindingClass + "' is not of expected temporal type");
		return null;
	}
}
