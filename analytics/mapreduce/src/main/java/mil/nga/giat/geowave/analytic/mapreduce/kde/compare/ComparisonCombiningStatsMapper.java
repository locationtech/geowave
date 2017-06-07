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

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class ComparisonCombiningStatsMapper extends
		Mapper<LongWritable, DoubleWritable, LongWritable, DoubleWritable>
{

	@Override
	protected void map(
			final LongWritable key,
			final DoubleWritable value,
			final org.apache.hadoop.mapreduce.Mapper.Context context )
			throws IOException,
			InterruptedException {
		long positiveKey = key.get();
		double adjustedValue = value.get();
		if (positiveKey < 0) {
			positiveKey = -positiveKey - 1;
			adjustedValue *= -1;
		}
		super.map(
				new LongWritable(
						positiveKey),
				new DoubleWritable(
						adjustedValue),
				context);
	}

}
