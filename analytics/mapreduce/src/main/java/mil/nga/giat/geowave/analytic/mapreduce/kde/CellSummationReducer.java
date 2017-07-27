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
package mil.nga.giat.geowave.analytic.mapreduce.kde;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CellSummationReducer extends
		Reducer<LongWritable, DoubleWritable, DoubleWritable, LongWritable>
{
	private final Map<Long, Double> maxPerLevel = new HashMap<Long, Double>();
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
		double sum = 0.0;

		for (final DoubleWritable value : values) {
			sum += value.get();
		}
		context.write(
				new DoubleWritable(
						sum),
				key);
		collectStats(
				key,
				sum,
				context);
	}

	protected void collectStats(
			final LongWritable key,
			final double sum,
			final Context context ) {
		final long level = (key.get() % numLevels) + minLevel;
		Double max = maxPerLevel.get(level);
		if ((max == null) || (sum > max)) {
			max = sum;
			maxPerLevel.put(
					level,
					max);
		}
		context.getCounter(
				"Entries per level",
				"level " + Long.toString(level)).increment(
				1);
	}

	@Override
	protected void cleanup(
			final org.apache.hadoop.mapreduce.Reducer.Context context )
			throws IOException,
			InterruptedException {
		for (final Entry<Long, Double> e : maxPerLevel.entrySet()) {
			context.write(
					new DoubleWritable(
							-e.getValue()),
					new LongWritable(
							e.getKey() - minLevel));
		}
		super.cleanup(context);
	}

}
