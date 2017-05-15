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
package mil.nga.giat.geowave.datastore.hbase.mapreduce;

import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.mapreduce.splits.GeoWaveInputSplit;
import mil.nga.giat.geowave.mapreduce.splits.RangeLocationPair;

public class GeoWaveHBaseInputSplit extends
		GeoWaveInputSplit
{
	public GeoWaveHBaseInputSplit() {
		super();
	}

	public GeoWaveHBaseInputSplit(
			final Map<PrimaryIndex, List<RangeLocationPair>> splitInfo,
			final String[] locations ) {
		super(
				splitInfo,
				locations);
	}

	@Override
	protected RangeLocationPair getRangeLocationPairInstance() {
		return HBaseSplitsProvider.defaultConstructRangeLocationPair();
	}
}
