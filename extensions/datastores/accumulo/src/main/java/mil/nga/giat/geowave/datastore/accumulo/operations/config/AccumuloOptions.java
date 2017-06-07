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
package mil.nga.giat.geowave.datastore.accumulo.operations.config;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.core.store.BaseDataStoreOptions;

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
}
