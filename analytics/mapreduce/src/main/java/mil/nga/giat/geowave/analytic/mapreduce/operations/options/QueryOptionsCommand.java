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
package mil.nga.giat.geowave.analytic.mapreduce.operations.options;

import java.util.List;

import com.beust.jcommander.Parameter;

public class QueryOptionsCommand
{

	@Parameter(names = "--auth", description = "The comma-separated list of authorizations used during extract; by default all authorizations are used.")
	private List<String> authorizations;

	@Parameter(names = "--adapters", required = true, description = "The comma-separated list of data adapters to query; by default all adapters are used.")
	private List<String> adapterIds = null;

	@Parameter(names = "--index", description = "The specific index to query; by default one is chosen for each adapter.")
	private String indexId = null;

	public QueryOptionsCommand() {}

	public List<String> getAuthorizations() {
		return authorizations;
	}

	public void setAuthorizations(
			List<String> authorizations ) {
		this.authorizations = authorizations;
	}

	public List<String> getAdapterIds() {
		return adapterIds;
	}

	public void setAdapterIds(
			List<String> adapterIds ) {
		this.adapterIds = adapterIds;
	}

	public String getIndexId() {
		return indexId;
	}

	public void setIndexId(
			String indexId ) {
		this.indexId = indexId;
	}
}
