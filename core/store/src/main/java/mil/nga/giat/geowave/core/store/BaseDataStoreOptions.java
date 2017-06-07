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
package mil.nga.giat.geowave.core.store;

import com.beust.jcommander.Parameter;

public class BaseDataStoreOptions implements
		DataStoreOptions
{
	@Parameter(names = "--persistAdapter", hidden = true, arity = 1)
	protected boolean persistAdapter = true;

	@Parameter(names = "--persistIndex", hidden = true, arity = 1)
	protected boolean persistIndex = true;

	@Parameter(names = "--persistDataStatistics", hidden = true, arity = 1)
	protected boolean persistDataStatistics = true;

	@Parameter(names = "--createTable", hidden = true, arity = 1)
	protected boolean createTable = true;

	@Parameter(names = "--useAltIndex", hidden = true, arity = 1)
	protected boolean useAltIndex = false;

	@Parameter(names = "--enableBlockCache", hidden = true, arity = 1)
	protected boolean enableBlockCache = true;

	@Override
	public boolean isPersistDataStatistics() {
		return persistDataStatistics;
	}

	public void setPersistDataStatistics(
			final boolean persistDataStatistics ) {
		this.persistDataStatistics = persistDataStatistics;
	}

	@Override
	public boolean isPersistAdapter() {
		return persistAdapter;
	}

	public void setPersistAdapter(
			final boolean persistAdapter ) {
		this.persistAdapter = persistAdapter;
	}

	@Override
	public boolean isPersistIndex() {
		return persistIndex;
	}

	public void setPersistIndex(
			final boolean persistIndex ) {
		this.persistIndex = persistIndex;
	}

	@Override
	public boolean isCreateTable() {
		return createTable;
	}

	public void setCreateTable(
			final boolean createTable ) {
		this.createTable = createTable;
	}

	@Override
	public boolean isUseAltIndex() {
		return useAltIndex;
	}

	public void setUseAltIndex(
			final boolean useAltIndex ) {
		this.useAltIndex = useAltIndex;
	}

	@Override
	public boolean isEnableBlockCache() {
		return enableBlockCache;
	}

	public void setEnableBlockCache(
			final boolean enableBlockCache ) {
		this.enableBlockCache = enableBlockCache;
	}

}
