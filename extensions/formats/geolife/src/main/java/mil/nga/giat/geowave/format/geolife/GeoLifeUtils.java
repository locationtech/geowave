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
package mil.nga.giat.geowave.format.geolife;

import java.io.File;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Geometry;

/**
 * This is a convenience class for performing common GPX static utility methods
 * such as schema validation, file parsing, and SimpleFeatureType definition.
 */
public class GeoLifeUtils
{

	private static final ThreadLocal<DateFormat> dateFormat = new ThreadLocal<DateFormat>() {
		@Override
		protected DateFormat initialValue() {
			return new SimpleDateFormat(
					"yyyy-MM-dd HH:mm:ss");
		}
	};

	public static Date parseDate(
			String source )
			throws ParseException {
		return dateFormat.get().parse(
				source);
	}

	public static final String GEOLIFE_POINT_FEATURE = "geolifepoint";
	public static final String GEOLIFE_TRACK_FEATURE = "geolifetrack";

	public static SimpleFeatureType createGeoLifeTrackDataType() {

		final SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder();
		simpleFeatureTypeBuilder.setName(GEOLIFE_TRACK_FEATURE);

		final AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();

		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Geometry.class).nillable(
				true).buildDescriptor(
				"geometry"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Date.class).nillable(
				true).buildDescriptor(
				"StartTimeStamp"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Date.class).nillable(
				true).buildDescriptor(
				"EndTimeStamp"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Long.class).nillable(
				true).buildDescriptor(
				"Duration"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Long.class).nillable(
				true).buildDescriptor(
				"NumberPoints"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				String.class).nillable(
				true).buildDescriptor(
				"TrackId"));
		return simpleFeatureTypeBuilder.buildFeatureType();

	}

	public static SimpleFeatureType createGeoLifePointDataType() {

		final SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder();
		simpleFeatureTypeBuilder.setName(GEOLIFE_POINT_FEATURE);

		final AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();

		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Geometry.class).nillable(
				false).buildDescriptor(
				"geometry"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				String.class).nillable(
				false).buildDescriptor(
				"trackid"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Integer.class).nillable(
				true).buildDescriptor(
				"pointinstance"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Date.class).nillable(
				true).buildDescriptor(
				"Timestamp"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Double.class).nillable(
				true).buildDescriptor(
				"Latitude"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Double.class).nillable(
				true).buildDescriptor(
				"Longitude"));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Double.class).nillable(
				true).buildDescriptor(
				"Elevation"));

		return simpleFeatureTypeBuilder.buildFeatureType();

	}

	public static boolean validate(
			final URL file ) {
		return true;
	}
}
