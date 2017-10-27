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
package mil.nga.giat.geowave.format.gdelt;

import java.io.File;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Point;

/**
 * This is a convenience class for performing common GDELT static utility
 * methods such as schema validation, file parsing, and SimpleFeatureType
 * definition.
 */
public class GDELTUtils
{

	private static final ThreadLocal<DateFormat> dateFormat = new ThreadLocal<DateFormat>() {
		@Override
		protected DateFormat initialValue() {
			return new SimpleDateFormat(
					"yyyyMMdd");
		}
	};

	public static Date parseDate(
			final String source )
			throws ParseException {
		return dateFormat.get().parse(
				source);
	}

	public static final int GDELT_MIN_COLUMNS = 57;
	public static final int GDELT_MAX_COLUMNS = 58;

	public static final String GDELT_EVENT_FEATURE = "gdeltevent";

	// "Core" fields
	public static final String GDELT_GEOMETRY_ATTRIBUTE = "geometry";

	public static final String GDELT_EVENT_ID_ATTRIBUTE = "eventid";
	public static final int GDELT_EVENT_ID_COLUMN_ID = 0;

	public static final String GDELT_TIMESTAMP_ATTRIBUTE = "Timestamp";
	public static final int GDELT_TIMESTAMP_COLUMN_ID = 1;

	public static final String GDELT_LATITUDE_ATTRIBUTE = "Latitude";
	public static final String GDELT_LONGITUDE_ATTRIBUTE = "Longitude";
	public static final int GDELT_ACTION_GEO_TYPE_COLUMN_ID = 49;
	private static final int GDELT_ACTION_LATITUDE_COLUMN_ID = 53;
	private static final int GDELT_ACTION_LONGITUDE_COLUMN_ID = 54;

	public static final String ACTOR_1_NAME_ATTRIBUTE = "actor1Name";
	public static final int ACTOR_1_NAME_COLUMN_ID = 6;

	public static final String ACTOR_2_NAME_ATTRIBUTE = "actor2Name";
	public static final int ACTOR_2_NAME_COLUMN_ID = 16;

	public static final String ACTION_COUNTRY_CODE_ATTRIBUTE = "countryCode";
	public static final int ACTION_COUNTRY_CODE_COLUMN_ID = 51;

	public static final String SOURCE_URL_ATTRIBUTE = "sourceUrl";
	public static final int SOURCE_URL_COLUMN_ID = 57;

	// "Supplemental" fields
	public static final String ACTOR_1_COUNTRY_CODE_ATTRIBUTE = "actor1CountryCode";
	public static final int ACTOR_1_COUNTRY_CODE_COLUMN_ID = 37;

	public static final String ACTOR_2_COUNTRY_CODE_ATTRIBUTE = "actor2CountryCode";
	public static final int ACTOR_2_COUNTRY_CODE_COLUMN_ID = 44;

	public static final String NUM_MENTIONS_ATTRIBUTE = "numMentions";
	public static final int NUM_MENTIONS_COLUMN_ID = 31;

	public static final String NUM_SOURCES_ATTRIBUTE = "numSources";
	public static final int NUM_SOURCES_COLUMN_ID = 32;

	public static final String NUM_ARTICLES_ATTRIBUTE = "numArticles";
	public static final int NUM_ARTICLES_COLUMN_ID = 33;

	public static final String AVG_TONE_ATTRIBUTE = "avgTone";
	public static final int AVG_TONE_COLUMN_ID = 34;

	public static SimpleFeatureType createGDELTEventDataType(
			final boolean includeSupplementalFields ) {

		final SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder();
		simpleFeatureTypeBuilder.setName(GDELT_EVENT_FEATURE);

		final AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();

		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Point.class).nillable(
				false).buildDescriptor(
				GDELT_GEOMETRY_ATTRIBUTE));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Integer.class).nillable(
				false).buildDescriptor(
				GDELT_EVENT_ID_ATTRIBUTE));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Date.class).nillable(
				false).buildDescriptor(
				GDELT_TIMESTAMP_ATTRIBUTE));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Double.class).nillable(
				false).buildDescriptor(
				GDELT_LATITUDE_ATTRIBUTE));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Double.class).nillable(
				false).buildDescriptor(
				GDELT_LONGITUDE_ATTRIBUTE));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				String.class).nillable(
				true).buildDescriptor(
				ACTOR_1_NAME_ATTRIBUTE));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				String.class).nillable(
				true).buildDescriptor(
				ACTOR_2_NAME_ATTRIBUTE));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				String.class).nillable(
				true).buildDescriptor(
				ACTION_COUNTRY_CODE_ATTRIBUTE));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				String.class).nillable(
				true).buildDescriptor(
				SOURCE_URL_ATTRIBUTE));
		if (includeSupplementalFields) {
			simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
					String.class).nillable(
					true).buildDescriptor(
					ACTOR_1_COUNTRY_CODE_ATTRIBUTE));
			simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
					String.class).nillable(
					true).buildDescriptor(
					ACTOR_2_COUNTRY_CODE_ATTRIBUTE));
			simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
					Integer.class).nillable(
					false).buildDescriptor(
					NUM_MENTIONS_ATTRIBUTE));
			simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
					Integer.class).nillable(
					false).buildDescriptor(
					NUM_SOURCES_ATTRIBUTE));
			simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
					Integer.class).nillable(
					false).buildDescriptor(
					NUM_ARTICLES_ATTRIBUTE));
			simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
					Double.class).nillable(
					false).buildDescriptor(
					AVG_TONE_ATTRIBUTE));
		}

		return simpleFeatureTypeBuilder.buildFeatureType();

	}

	public static Pair<Double, Double> parseLatLon(
			final String[] vals ) {

		final String latString = vals[GDELTUtils.GDELT_ACTION_LATITUDE_COLUMN_ID];
		final String lonString = vals[GDELTUtils.GDELT_ACTION_LONGITUDE_COLUMN_ID];
		if ((latString == null) || (lonString == null) || latString.trim().isEmpty() || lonString.trim().isEmpty()) {
			return null;
		}
		final Double lat = Double.parseDouble(latString);
		final Double lon = Double.parseDouble(lonString);

		return Pair.of(
				lat,
				lon);

	}

	public static boolean validate(
			final URL file ) {
		return FilenameUtils.getName(
				file.getPath()).toLowerCase(
				Locale.ENGLISH).matches(
				"\\d{8}\\.export\\.csv\\.zip") || FilenameUtils.getName(
				file.getPath()).toLowerCase(
				Locale.ENGLISH).matches(
				"\\d{4,6}\\.zip");
	}
}
