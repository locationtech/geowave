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
package mil.nga.giat.geowave.analytic.mapreduce.kde.compare;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.analytic.mapreduce.kde.GaussianCellMapper;
import mil.nga.giat.geowave.analytic.mapreduce.kde.GaussianFilter;
import mil.nga.giat.geowave.analytic.mapreduce.kde.GaussianFilter.ValueRange;

import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Point;

public class ComparisonGaussianCellMapper extends
		GaussianCellMapper
{
	protected static final String TIME_ATTRIBUTE_KEY = "TIME_ATTRIBUTE";
	private String timeAttribute;
	private final Map<Integer, LevelStore> winterLevelStoreMap = new HashMap<Integer, LevelStore>();

	@Override
	protected void setup(
			final Context context )
			throws IOException,
			InterruptedException {
		super.setup(context);
		timeAttribute = context.getConfiguration().get(
				TIME_ATTRIBUTE_KEY);
	}

	@Override
	protected void populateLevelStore(
			final org.apache.hadoop.mapreduce.Mapper.Context context,
			final int numXPosts,
			final int numYPosts,
			final int level ) {
		super.populateLevelStore(
				context,
				numXPosts,
				numYPosts,
				level);

		winterLevelStoreMap.put(
				level,
				new LevelStore(
						numXPosts,
						numYPosts,
						new NegativeCellIdCounter(
								context,
								level,
								minLevel,
								maxLevel)));
	}

	@Override
	protected void incrementLevelStore(
			final int level,
			final Point pt,
			final SimpleFeature feature,
			final ValueRange[] valueRangePerDimension ) {
		final Object obj = feature.getAttribute(timeAttribute);
		if ((obj != null) && (obj instanceof Date)) {
			double contribution = 0;
			LevelStore levelStore = null;
			final Calendar cal = Calendar.getInstance();
			cal.setTime((Date) obj);
			// the seasonal variance algorithm we'll use will apply a gaussian
			// function to winter months (October - March), incrementing the
			// winter counter
			// and apply a gaussian function to April and September incrementing
			// the summer counter
			// the other months increment the summer counter
			final int featureMonth = cal.get(Calendar.MONTH);
			if (featureMonth < 3) {
				final Calendar baseDate = Calendar.getInstance();
				baseDate.set(
						cal.get(Calendar.YEAR),
						0,
						0,
						0,
						0,
						0);
				final double deltaTime = cal.getTime().getTime() - baseDate.getTime().getTime();
				// now normalize so the value is between 0 and 3 (somewhat
				// arbitrary but e^-(x*x) asymptotically approaches 0 near 3 and
				// -3)
				final Calendar maxDate = Calendar.getInstance();
				maxDate.set(
						cal.get(Calendar.YEAR),
						3,
						0,
						0,
						0,
						0);
				final double normalizedTime = (deltaTime * 3)
						/ (maxDate.getTimeInMillis() - baseDate.getTimeInMillis());
				contribution = Math.pow(
						Math.E,
						-(normalizedTime * normalizedTime));
				levelStore = winterLevelStoreMap.get(level);
			}
			else if (featureMonth > 8) {
				final Calendar baseDate = Calendar.getInstance();
				baseDate.set(
						cal.get(Calendar.YEAR) + 1,
						0,
						0,
						0,
						0,
						0);
				final double deltaTime = baseDate.getTime().getTime() - cal.getTime().getTime();
				// now normalize so the value is between 0 and 3 (somewhat
				// arbitrary but e^-(x*x) asymptotically approaches 0 near 3 and
				// -3)
				final Calendar minDate = Calendar.getInstance();
				minDate.set(
						cal.get(Calendar.YEAR),
						9,
						0,
						0,
						0,
						0);
				final double normalizedTime = (deltaTime * 3)
						/ (baseDate.getTimeInMillis() - minDate.getTimeInMillis());
				contribution = Math.pow(
						Math.E,
						-(normalizedTime * normalizedTime));
				levelStore = winterLevelStoreMap.get(level);
			}
			else if ((featureMonth == 3) || (featureMonth == 8)) {
				final Calendar maxDate = Calendar.getInstance();
				maxDate.set(
						cal.get(Calendar.YEAR),
						featureMonth + 1,
						0,
						0,
						0,
						0);
				final double deltaTime;

				final Calendar minDate = Calendar.getInstance();
				minDate.set(
						cal.get(Calendar.YEAR),
						featureMonth,
						0,
						0,
						0,
						0);
				if (featureMonth == 3) {
					deltaTime = maxDate.getTime().getTime() - cal.getTime().getTime();
				}
				else {
					deltaTime = cal.getTime().getTime() - minDate.getTime().getTime();
				}

				final double normalizedTime = (deltaTime * 3) / (maxDate.getTimeInMillis() - minDate.getTimeInMillis());
				contribution = Math.pow(
						Math.E,
						-(normalizedTime * normalizedTime));
				levelStore = levelStoreMap.get(level);
			}
			else {
				contribution = 1;
				levelStore = levelStoreMap.get(level);
			}

			GaussianFilter.incrementPt(
					pt.getY(),
					pt.getX(),
					levelStore.counter,
					levelStore.numXPosts,
					levelStore.numYPosts,
					contribution,
					valueRangePerDimension);
		}
	}
}
