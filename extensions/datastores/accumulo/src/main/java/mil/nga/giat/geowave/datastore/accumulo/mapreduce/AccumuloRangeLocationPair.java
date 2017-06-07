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
package mil.nga.giat.geowave.datastore.accumulo.mapreduce;

import org.apache.accumulo.core.data.Range;

import mil.nga.giat.geowave.mapreduce.splits.GeoWaveRowRange;
import mil.nga.giat.geowave.mapreduce.splits.RangeLocationPair;

public class AccumuloRangeLocationPair extends
		RangeLocationPair
{
	public AccumuloRangeLocationPair(
			final GeoWaveRowRange range,
			final String location,
			final double cardinality ) {
		super(
				range,
				location,
				cardinality);
	}

	public AccumuloRangeLocationPair() {
		super();
	}

	@Override
	protected GeoWaveRowRange buildRowRangeInstance() {
		return new AccumuloRowRange(
				new Range());
	}
}
