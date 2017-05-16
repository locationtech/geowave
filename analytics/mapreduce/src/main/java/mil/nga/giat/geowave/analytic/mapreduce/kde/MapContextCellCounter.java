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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapContextCellCounter implements
		CellCounter
{
	private static final Logger LOGGER = LoggerFactory.getLogger(MapContextCellCounter.class);

	private final Context context;
	private final long minLevel;
	private final long maxLevel;
	private final long numLevels;
	private final long level;

	public MapContextCellCounter(
			final Context context,
			final long level,
			final long minLevel,
			final long maxLevel ) {
		this.context = context;
		this.level = level;
		this.minLevel = minLevel;
		this.maxLevel = maxLevel;
		numLevels = (maxLevel - minLevel) + 1;
	}

	@Override
	public void increment(
			final long cellId,
			final double weight ) {
		if (weight > 0) {
			try {
				context.write(
						new LongWritable(
								getCellId(cellId)),
						new DoubleWritable(
								weight));
			}
			catch (IOException | InterruptedException e) {
				LOGGER.error(
						"Unable to write",
						e);
			}
		}
	}

	protected long getCellId(
			final long cellId ) {
		return (cellId * numLevels) + (level - minLevel);
	}

}
