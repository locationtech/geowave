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
package org.locationtech.geowave.analytic.mapreduce.kde.compare;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class ComparisonCombinedLevelPartitioner extends
		Partitioner<DoubleWritable, LongWritable>
{
	@Override
	public int getPartition(
			final DoubleWritable key,
			final LongWritable value,
			final int numReduceTasks ) {
		return getPartition(
				value.get(),
				numReduceTasks);
	}

	protected int getPartition(
			final long positiveCellId,
			final int numReduceTasks ) {
		return (int) (positiveCellId % numReduceTasks);
	}
}
