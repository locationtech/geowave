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
package mil.nga.giat.geowave.adapter.vector.export;

import java.util.List;

import com.beust.jcommander.Parameter;

public class VectorExportOptions
{
	protected static final int DEFAULT_BATCH_SIZE = 10000;

	@Parameter(names = "--cqlFilter", description = "Filter exported data based on CQL filter")
	private String cqlFilter;

	@Parameter(names = "--adapterIds", description = "Comma separated list of adapter Ids")
	private List<String> adapterIds;

	@Parameter(names = "--indexId", description = "The index to export from")
	private String indexId;

	@Parameter(names = "--batchSize", description = "Records to process at a time")
	private int batchSize = DEFAULT_BATCH_SIZE;

	public String getCqlFilter() {
		return cqlFilter;
	}

	public List<String> getAdapterIds() {
		return adapterIds;
	}

	public String getIndexId() {
		return indexId;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public void setCqlFilter(
			String cqlFilter ) {
		this.cqlFilter = cqlFilter;
	}

	public void setAdapterIds(
			List<String> adapterIds ) {
		this.adapterIds = adapterIds;
	}

	public void setIndexId(
			String indexId ) {
		this.indexId = indexId;
	}

	public void setBatchSize(
			int batchSize ) {
		this.batchSize = batchSize;
	}
}
