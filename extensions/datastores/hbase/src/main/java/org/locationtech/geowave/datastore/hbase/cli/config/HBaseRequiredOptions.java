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
package org.locationtech.geowave.datastore.hbase.cli.config;

import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.datastore.hbase.HBaseStoreFactoryFamily;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class HBaseRequiredOptions extends
		StoreFactoryOptions
{

	public static final String ZOOKEEPER_CONFIG_KEY = "zookeeper";

	@Parameter(names = {
		"-z",
		"--" + ZOOKEEPER_CONFIG_KEY
	}, description = "A comma-separated list of zookeeper servers that an HBase instance is using", required = true)
	private String zookeeper;

	@ParametersDelegate
	private HBaseOptions additionalOptions = new HBaseOptions();

	public HBaseRequiredOptions() {}

	public HBaseRequiredOptions(
			String zookeeper,
			String gwNamespace,
			HBaseOptions additionalOptions ) {
		super(
				gwNamespace);
		this.zookeeper = zookeeper;
		this.additionalOptions = additionalOptions;
	}

	public String getZookeeper() {
		return zookeeper;
	}

	public void setZookeeper(
			final String zookeeper ) {
		this.zookeeper = zookeeper;
	}

	public void setStoreOptions(
			final HBaseOptions additionalOptions ) {
		this.additionalOptions = additionalOptions;
	}

	@Override
	public StoreFactoryFamilySpi getStoreFactory() {
		return new HBaseStoreFactoryFamily();
	}

	@Override
	public DataStoreOptions getStoreOptions() {
		return additionalOptions;
	}
}
