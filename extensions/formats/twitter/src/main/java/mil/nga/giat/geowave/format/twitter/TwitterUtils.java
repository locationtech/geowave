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
package mil.nga.giat.geowave.format.twitter;

import java.io.File;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.io.FilenameUtils;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Point;

/**
 * This is a convenience class for performing common Twitter static utility
 * methods such as schema validation, file parsing, and SimpleFeatureType
 * definition.
 */
public class TwitterUtils
{

	private static final ThreadLocal<DateFormat> dateFormat = new ThreadLocal<DateFormat>() {
		@Override
		protected DateFormat initialValue() {
			return new SimpleDateFormat(
					"EEE MMM dd HH:mm:ss Z yyyy");
		}
	};

	public static Date parseDate(
			final String source )
			throws ParseException {
		return dateFormat.get().parse(
				source);
	}

	public static final String TWITTER_SFT_NAME = "twitter";

	public static final String TWITTER_GEOMETRY_ATTRIBUTE = "geom";
	public static final String TWITTER_DTG_ATTRIBUTE = "dtg";

	public static final String TWITTER_USERID_ATTRIBUTE = "user_id";
	public static final String TWITTER_USERNAME_ATTRIBUTE = "user_name";
	public static final String TWITTER_TEXT_ATTRIBUTE = "text";
	public static final String TWITTER_INREPLYTOUSER_ATTRIBUTE = "in_reply_to_user_id";
	public static final String TWITTER_INREPLYTOSTATUS_ATTRIBUTE = "in_reply_to_status_id";
	public static final String TWITTER_RETWEETCOUNT_ATTRIBUTE = "retweet_count";
	public static final String TWITTER_LANG_ATTRIBUTE = "lang";

	public static SimpleFeatureType createTwitterEventDataType() {

		final SimpleFeatureTypeBuilder simpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder();
		simpleFeatureTypeBuilder.setName(TWITTER_SFT_NAME);

		final AttributeTypeBuilder attributeTypeBuilder = new AttributeTypeBuilder();

		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				String.class).nillable(
				true).buildDescriptor(
				TWITTER_USERID_ATTRIBUTE));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				String.class).nillable(
				true).buildDescriptor(
				TWITTER_USERNAME_ATTRIBUTE));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				String.class).nillable(
				true).buildDescriptor(
				TWITTER_TEXT_ATTRIBUTE));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				String.class).nillable(
				true).buildDescriptor(
				TWITTER_INREPLYTOUSER_ATTRIBUTE));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				String.class).nillable(
				true).buildDescriptor(
				TWITTER_INREPLYTOSTATUS_ATTRIBUTE));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Integer.class).nillable(
				true).buildDescriptor(
				TWITTER_RETWEETCOUNT_ATTRIBUTE));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				String.class).nillable(
				true).buildDescriptor(
				TWITTER_LANG_ATTRIBUTE));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Date.class).nillable(
				false).buildDescriptor(
				TWITTER_DTG_ATTRIBUTE));
		simpleFeatureTypeBuilder.add(attributeTypeBuilder.binding(
				Point.class).nillable(
				false).buildDescriptor(
				TWITTER_GEOMETRY_ATTRIBUTE));

		return simpleFeatureTypeBuilder.buildFeatureType();

	}

	public static boolean validate(
			final URL file ) {
		return FilenameUtils.getName(
				file.getPath()).toLowerCase(
				Locale.ENGLISH).matches(
				"\\d{8}-\\d{6}\\.txt\\.gz");
	}
}
