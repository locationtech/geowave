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
package mil.nga.giat.geowave.datastore.hbase.operations.config;

import org.apache.hadoop.hbase.HConstants;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.core.store.BaseDataStoreOptions;

public class HBaseOptions extends
		BaseDataStoreOptions
{
	public static final String COPROCESSOR_JAR_KEY = "coprocessorJar";

	@Parameter(names = "--disableServer", description = "Disable all custom GeoWave server side processing, to include coprocessors and custom filters.  This will alleviate the requirement to have a GeoWave jar on HBase's region server classpath.")
	protected boolean disableServiceSide = false;

	@Parameter(names = "--scanCacheSize")
	protected int scanCacheSize = HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING;

	protected boolean disableCustomFilters = false;

	protected boolean disableCoprocessors = false;

	@Parameter(names = "--disableVerifyCoprocessors")
	protected boolean disableVerifyCoprocessors = false;

	protected boolean bigTable = false;

	@Parameter(names = {
		"--" + COPROCESSOR_JAR_KEY
	}, description = "Path (HDFS URL) to the jar containing coprocessor classes")
	private String coprocessorJar;

	public void setBigTable(
			boolean bigTable ) {
		this.bigTable = bigTable;
		if (bigTable) {
			setServerSideDisabled(true);
		}
	}

	public boolean isBigTable() {
		return bigTable;
	}

	public boolean isServerSideDisabled() {
		return disableServiceSide;
	}

	public void setServerSideDisabled(
			final boolean disableServiceSide ) {
		this.disableServiceSide = disableServiceSide;
	}

	public int getScanCacheSize() {
		return scanCacheSize;
	}

	public void setScanCacheSize(
			final int scanCacheSize ) {
		this.scanCacheSize = scanCacheSize;
	}

	public boolean isEnableCustomFilters() {
		return !disableCustomFilters && !disableServiceSide;
	}

	public void setEnableCustomFilters(
			final boolean enableCustomFilters ) {
		this.disableCustomFilters = !enableCustomFilters;
	}

	public boolean isEnableCoprocessors() {
		return !disableCoprocessors && !disableServiceSide;
	}

	public void setEnableCoprocessors(
			final boolean enableCoprocessors ) {
		this.disableCoprocessors = !enableCoprocessors;
	}

	public boolean isVerifyCoprocessors() {
		return !disableVerifyCoprocessors && !disableServiceSide;
	}

	public void setVerifyCoprocessors(
			final boolean verifyCoprocessors ) {
		this.disableVerifyCoprocessors = !verifyCoprocessors;
	}

	public String getCoprocessorJar() {
		return coprocessorJar;
	}

	public void setCoprocessorJar(
			final String coprocessorJar ) {
		this.coprocessorJar = coprocessorJar;
	}
}
