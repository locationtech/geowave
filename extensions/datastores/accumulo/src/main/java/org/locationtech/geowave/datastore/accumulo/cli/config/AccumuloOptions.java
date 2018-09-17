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
package org.locationtech.geowave.datastore.accumulo.cli.config;

import org.locationtech.geowave.core.store.BaseDataStoreOptions;
import org.locationtech.geowave.datastore.accumulo.util.AccumuloUtils;

import com.beust.jcommander.Parameter;

/**
 * This class can be used to modify the behavior of the Accumulo Data Store.
 *
 */
public class AccumuloOptions extends
		BaseDataStoreOptions
{
	@Parameter(names = "--useLocalityGroups", hidden = true, arity = 1)
	protected boolean useLocalityGroups = true;

	public boolean isUseLocalityGroups() {
		return useLocalityGroups;
	}

	public void setUseLocalityGroups(
			final boolean useLocalityGroups ) {
		this.useLocalityGroups = useLocalityGroups;
	}

	@Override
	protected int defaultMaxRangeDecomposition() {
		return AccumuloUtils.ACCUMULO_DEFAULT_MAX_RANGE_DECOMPOSITION;
	}

	@Override
	protected int defaultAggregationMaxRangeDecomposition() {
		return AccumuloUtils.ACCUMULO_DEFAULT_AGGREGATION_MAX_RANGE_DECOMPOSITION;
	}
}
