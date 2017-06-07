/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.geotime.store.field;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.junit.Assert;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;

import org.junit.Before;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class GeoTimeReaderWriterTest
{
	private Geometry geometryExpected;
	private Geometry[] geometryArrayExpected;
	private Date dateExpected;
	private Date[] dateArrayExpected;
	private Calendar calendarExpected;
	private Calendar[] calendarArrayExpected;

	@Before
	public void init() {
		geometryExpected = new GeometryFactory().createPoint(new Coordinate(
				25,
				32));
		geometryArrayExpected = new Geometry[] {
			new GeometryFactory().createPoint(new Coordinate(
					25,
					32)),
			new GeometryFactory().createPoint(new Coordinate(
					26,
					33)),
			new GeometryFactory().createPoint(new Coordinate(
					27,
					34)),
			new GeometryFactory().createPoint(new Coordinate(
					28,
					35))
		};
		dateExpected = new Date();
		dateArrayExpected = new Date[] {
			new Date(),
			null,
			new Date(
					0),
			null
		};
		calendarExpected = new GregorianCalendar();
		calendarExpected.setTimeZone(TimeZone.getTimeZone("GMT"));
		final Calendar cal1 = new GregorianCalendar();
		cal1.setTimeZone(TimeZone.getTimeZone("GMT"));
		final Calendar cal2 = new GregorianCalendar();
		cal2.setTimeZone(TimeZone.getTimeZone("GMT"));
		calendarArrayExpected = new Calendar[] {
			cal1,
			null,
			cal2,
			null
		};
	}

	public void testGeoTimeReadWrite() {
		byte[] value;
		// test Geometry reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Geometry.class).writeField(
				geometryExpected);
		final Geometry geometryActual = FieldUtils.getDefaultReaderForClass(
				Geometry.class).readField(
				value);
		// TODO develop the "equals" test for Geometry
		Assert.assertEquals(
				"FAILED test of Geometry reader/writer",
				geometryExpected,
				geometryActual);

		// test Geometry Array reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Geometry[].class).writeField(
				geometryArrayExpected);
		final Geometry[] geometryArrayActual = FieldUtils.getDefaultReaderForClass(
				Geometry[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED test of String Array reader/writer",
				Arrays.deepEquals(
						geometryArrayExpected,
						geometryArrayActual));

		// test Date reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Date.class).writeField(
				dateExpected);
		final Date dateActual = FieldUtils.getDefaultReaderForClass(
				Date.class).readField(
				value);
		Assert.assertEquals(
				"FAILED test of Date reader/writer",
				dateExpected,
				dateActual);

		// test Date Array reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Date[].class).writeField(
				dateArrayExpected);
		final Date[] dateArrayActual = FieldUtils.getDefaultReaderForClass(
				Date[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED test of Date Array reader/writer",
				Arrays.deepEquals(
						dateArrayExpected,
						dateArrayActual));

		// test Calendar reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Calendar.class).writeField(
				calendarExpected);
		final Calendar calendarActual = FieldUtils.getDefaultReaderForClass(
				Calendar.class).readField(
				value);
		Assert.assertEquals(
				"FAILED test of Calendar reader/writer",
				calendarExpected,
				calendarActual);

		// test Calendar Array reader/writer
		value = FieldUtils.getDefaultWriterForClass(
				Calendar[].class).writeField(
				calendarArrayExpected);
		final Calendar[] calendarArrayActual = FieldUtils.getDefaultReaderForClass(
				Calendar[].class).readField(
				value);
		Assert.assertTrue(
				"FAILED test of Calendar Array reader/writer",
				Arrays.deepEquals(
						calendarArrayExpected,
						calendarArrayActual));
	}
}
