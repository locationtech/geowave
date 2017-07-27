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

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class IdentityMapper extends
		Mapper<DoubleWritable, LongWritable, DoubleWritable, LongWritable>
{
	@Override
	protected void map(
			final DoubleWritable key,
			final LongWritable value,
			final org.apache.hadoop.mapreduce.Mapper.Context context )
			throws IOException,
			InterruptedException {
		context.write(
				key,
				value);
	}
}
