/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.geotime.util;

import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.locationtech.geowave.core.geotime.store.query.TemporalConstraints;
import org.locationtech.geowave.core.geotime.store.query.TemporalConstraintsSet;
import org.locationtech.geowave.core.geotime.util.TimeDescriptors.TimeDescriptorConfiguration;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.Interval;

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
			final Calendar cal ) {
		// get Date object representing this Calendar's time value, millisecond
		// offset from the Epoch, January 1, 1970 00:00:00.000 GMT (Gregorian)
		final Date date = cal.getTime();
		// Returns the number of milliseconds since January 1, 1970, 00:00:00
		// GMT represented by this Date object.
		final long time = date.getTime();
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
			final Object timeObj ) {
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
			final Class<?> bindingClass ) {
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
			final Class<?> bindingClass,
			final long longVal ) {
		if (longVal < 0) {
			// indicator that the time value is null;
			return null;
		}
		if (Calendar.class.isAssignableFrom(bindingClass)) {
			final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
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

	public static TemporalConstraints getTemporalConstraintsForDescriptors(
			final TimeDescriptors timeDescriptors,
			final TemporalConstraintsSet timeBoundsSet ) {
		if ((timeBoundsSet == null) || timeBoundsSet.isEmpty()) {
			return new TemporalConstraints();
		}

		if ((timeDescriptors.getStartRange() != null) && (timeDescriptors.getEndRange() != null)) {
			return composeRangeTemporalConstraints(
					timeDescriptors,
					timeBoundsSet);
		}
		else if ((timeDescriptors.getTime() != null)
				&& timeBoundsSet.hasConstraintsFor(timeDescriptors.getTime().getLocalName())) {
			return timeBoundsSet.getConstraintsFor(timeDescriptors.getTime().getLocalName());
		}

		return new TemporalConstraints();
	}

	/**
	 * Compose temporal constraints given the constraint set and the descriptors
	 * for the index.
	 *
	 * @param timeDescriptors
	 * @param constraintsSet
	 * @return null if the constraints does not have the fields required by the
	 *         time descriptors
	 */
	public static TemporalConstraints composeRangeTemporalConstraints(
			final TimeDescriptors timeDescriptors,
			final TemporalConstraintsSet constraintsSet ) {

		if ((timeDescriptors.getEndRange() != null) && (timeDescriptors.getStartRange() != null)) {
			final String ename = timeDescriptors.getEndRange().getLocalName();
			final String sname = timeDescriptors.getStartRange().getLocalName();

			if (constraintsSet.hasConstraintsForRange(
					sname,
					ename)) {
				return constraintsSet.getConstraintsForRange(
						sname,
						ename);
			}

		}
		else if ((timeDescriptors.getTime() != null)
				&& constraintsSet.hasConstraintsFor(timeDescriptors.getTime().getLocalName())) {
			return constraintsSet.getConstraintsFor(timeDescriptors.getTime().getLocalName());
		}
		return new TemporalConstraints();
	}

	public static Interval getInterval(
			final SimpleFeature entry,
			final String fieldName ) {

		final Object o = entry.getAttribute(fieldName);
		final Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		if (o == null) {
			return null;
		}
		if (o instanceof Date) {
			c.setTime((Date) o);
		}
		else if (o instanceof Calendar) {
			c.setTime(c.getTime());
		}
		else if (o instanceof Number) {
			c.setTimeInMillis(((Number) o).longValue());
		}
		final Instant time = Instant.ofEpochMilli(c.getTimeInMillis());
		return Interval.of(
				time,
				time);
	}

	/**
	 * Determine if a time or range descriptor is set. If so, then use it,
	 * otherwise infer.
	 *
	 * @param persistType
	 *            - FeatureType that will be scanned for TimeAttributes
	 * @return
	 */
	public static final TimeDescriptors inferTimeAttributeDescriptor(
			final SimpleFeatureType persistType ) {

		final TimeDescriptorConfiguration config = new TimeDescriptorConfiguration(
				persistType);
		final TimeDescriptors timeDescriptors = new TimeDescriptors(
				persistType,
				config);

		// Up the meta-data so that it is clear and visible any inference that
		// has occurred here. Also, this is critical to
		// serialization/deserialization

		config.updateType(persistType);
		return timeDescriptors;
	}
}
