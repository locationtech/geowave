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

import mil.nga.giat.geowave.analytic.mapreduce.kde.KDEJobRunner;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ComparisonCombiningStatsReducer extends
		Reducer<LongWritable, DoubleWritable, ComparisonCellData, LongWritable>
{

	protected int minLevel;
	protected int maxLevel;
	protected int numLevels;

	@Override
	protected void setup(
			final Context context )
			throws IOException,
			InterruptedException {
		minLevel = context.getConfiguration().getInt(
				KDEJobRunner.MIN_LEVEL_KEY,
				1);
		maxLevel = context.getConfiguration().getInt(
				KDEJobRunner.MAX_LEVEL_KEY,
				25);
		numLevels = (maxLevel - minLevel) + 1;
		super.setup(context);
	}

	@Override
	public void reduce(
			final LongWritable key,
			final Iterable<DoubleWritable> values,
			final Context context )
			throws IOException,
			InterruptedException {
		double summer = 0;
		double winter = 0;
		for (final DoubleWritable v : values) {
			if (v.get() < 0) {
				winter = -v.get();
			}
			else {
				summer = v.get();
			}
		}
		context.write(
				new ComparisonCellData(
						summer,
						winter),
				key);
		collectStats(
				key.get(),
				context);
	}

	protected void collectStats(
			final long key,
			final Context context ) {
		final long level = (key % numLevels) + minLevel;
		context.getCounter(
				"Entries per level",
				"level " + Long.toString(level)).increment(
				1);
	}
}
