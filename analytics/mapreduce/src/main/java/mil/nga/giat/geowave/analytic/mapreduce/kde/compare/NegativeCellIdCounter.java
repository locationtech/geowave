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

import mil.nga.giat.geowave.analytic.mapreduce.kde.MapContextCellCounter;

import org.apache.hadoop.mapreduce.Mapper.Context;

public class NegativeCellIdCounter extends
		MapContextCellCounter
{

	public NegativeCellIdCounter(
			final Context context,
			final long level,
			final long minLevel,
			final long maxLevel ) {
		super(
				context,
				level,
				minLevel,
				maxLevel);
	}

	@Override
	protected long getCellId(
			final long cellId ) {
		return -super.getCellId(cellId) - 1;
	}

}
