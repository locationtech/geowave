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

import mil.nga.giat.geowave.analytic.mapreduce.kde.LevelPartitioner;

import org.apache.hadoop.io.LongWritable;

abstract public class ComparisonLevelPartitioner<T> extends
		LevelPartitioner<T>
{

	@Override
	public int getPartition(
			final T key,
			final LongWritable value,
			final int numReduceTasks ) {
		final int reduceTasksPerSeason = numReduceTasks / 2;
		if (value.get() < 0) {
			// let the winter (cell ID < 0) get the second half of partitions
			return getPartition(
					-value.get() - 1,
					reduceTasksPerSeason) + reduceTasksPerSeason;
		}
		else {
			// let the summer (cell ID >= 0) get the first set of partitions
			return getPartition(
					value.get(),
					reduceTasksPerSeason);
		}
	}

}
