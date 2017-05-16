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
package mil.nga.giat.geowave.datastore.bigtable.operations.config;

import org.apache.hadoop.hbase.HConstants;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.store.BaseDataStoreOptions;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.datastore.bigtable.BigTableStoreFactoryFamily;
import mil.nga.giat.geowave.datastore.hbase.HBaseStoreFactoryFamily;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseOptions;

public class BigTableOptions extends
		StoreFactoryOptions
{

	public static final String DEFAULT_PROJECT_ID = "geowave-bigtable-project-id";
	public static final String DEFAULT_INSTANCE_ID = "geowave-bigtable-instance-id";

	@Parameter(names = "--scanCacheSize")
	protected int scanCacheSize = HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING;
	@Parameter(names = "--projectId")
	protected String projectId = DEFAULT_PROJECT_ID;
	@Parameter(names = "--instanceId")
	protected String instanceId = DEFAULT_INSTANCE_ID;
	private final HBaseOptions internalHBaseOptions = new InternalHBaseOptions();

	@ParametersDelegate
	private final BaseDataStoreOptions additionalOptions = new BaseDataStoreOptions();

	@Override
	public StoreFactoryFamilySpi getStoreFactory() {
		return new BigTableStoreFactoryFamily();
	}

	public String getProjectId() {
		return projectId;
	}

	public void setProjectId(
			final String projectId ) {
		this.projectId = projectId;
	}

	public String getInstanceId() {
		return instanceId;
	}

	public void setInstanceId(
			final String instanceId ) {
		this.instanceId = instanceId;
	}

	public HBaseOptions getHBaseOptions() {
		return internalHBaseOptions;
	}

	private class InternalHBaseOptions extends
			HBaseOptions
	{

		public InternalHBaseOptions() {
			super();
			// all the necessary methods are overridden, but just to be extra
			// explicit setBigTable(true);
			setBigTable(true);
		}

		@Override
		public boolean isServerSideDisabled() {
			return true;
		}

		@Override
		public boolean isBigTable() {
			return true;
		}

		@Override
		public int getScanCacheSize() {
			return BigTableOptions.this.scanCacheSize;
		}

		@Override
		public boolean isEnableCustomFilters() {
			return false;
		}

		@Override
		public boolean isEnableCoprocessors() {
			return false;
		}

		@Override
		public boolean isVerifyCoprocessors() {
			return false;
		}

		// delegate other methods to the BigTable's additional options

		@Override
		public boolean isPersistDataStatistics() {
			return additionalOptions.isPersistDataStatistics();
		}

		@Override
		public void setPersistDataStatistics(
				final boolean persistDataStatistics ) {
			additionalOptions.setPersistDataStatistics(persistDataStatistics);
		}

		@Override
		public boolean isPersistAdapter() {
			return additionalOptions.isPersistAdapter();
		}

		@Override
		public void setPersistAdapter(
				final boolean persistAdapter ) {
			additionalOptions.setPersistAdapter(persistAdapter);
		}

		@Override
		public boolean isPersistIndex() {
			return additionalOptions.isPersistIndex();
		}

		@Override
		public void setPersistIndex(
				final boolean persistIndex ) {
			additionalOptions.setPersistIndex(persistIndex);
		}

		@Override
		public boolean isCreateTable() {
			return additionalOptions.isCreateTable();
		}

		@Override
		public void setCreateTable(
				final boolean createTable ) {
			additionalOptions.setCreateTable(createTable);
		}

		@Override
		public boolean isUseAltIndex() {
			return additionalOptions.isUseAltIndex();
		}

		@Override
		public void setUseAltIndex(
				final boolean useAltIndex ) {
			additionalOptions.setUseAltIndex(useAltIndex);
		}

		@Override
		public boolean isEnableBlockCache() {
			return additionalOptions.isEnableBlockCache();
		}

		@Override
		public void setEnableBlockCache(
				final boolean enableBlockCache ) {
			additionalOptions.setEnableBlockCache(enableBlockCache);
		}

	}
}
