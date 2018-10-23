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
package org.locationtech.geowave.datastore.cassandra.config;

import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.datastore.cassandra.CassandraStoreFactoryFamily;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class CassandraRequiredOptions extends
		StoreFactoryOptions
{
	@Parameter(names = "--contactPoints", required = true, description = "A single contact point or a comma delimited set of contact points to connect to the Cassandra cluster.")
	private String contactPoints;

	@ParametersDelegate
	private CassandraOptions additionalOptions = new CassandraOptions();

	public CassandraRequiredOptions() {}

	public CassandraRequiredOptions(
			String contactPoints,
			String gwNamespace,
			CassandraOptions additionalOptions ) {
		super(
				gwNamespace);
		this.contactPoints = contactPoints;
		this.additionalOptions = additionalOptions;
	}

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
