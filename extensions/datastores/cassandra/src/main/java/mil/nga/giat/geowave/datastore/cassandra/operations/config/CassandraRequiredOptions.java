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
package mil.nga.giat.geowave.datastore.cassandra.operations.config;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.datastore.cassandra.CassandraStoreFactoryFamily;

public class CassandraRequiredOptions extends
		StoreFactoryOptions
{
	@Parameter(names = "--contactPoints", required = true, description = "A single contact point or a comma delimited set of contact points to connect to the Cassandra cluster.")
	private String contactPoints;

	@ParametersDelegate
	private final CassandraOptions additionalOptions = new CassandraOptions();

	@Override
	public StoreFactoryFamilySpi getStoreFactory() {
		return new CassandraStoreFactoryFamily();
	}

	public String getContactPoint() {
		return contactPoints;
	}

	public void setContactPoint(
			final String contactPoints ) {
		this.contactPoints = contactPoints;
	}

	@Override
	public DataStoreOptions getStoreOptions() {
		return additionalOptions;
	}
}
