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
package org.locationtech.geowave.analytic.mapreduce.operations.options;

import com.beust.jcommander.Parameter;

public class QueryOptionsCommand
{

	@Parameter(names = "--auth", description = "The comma-separated list of authorizations used during extract; by default all authorizations are used.")
	private String[] authorizations;

	@Parameter(names = "--typeNames", required = true, description = "The comma-separated list of data typess to query; by default all data types are used.")
	private String[] typeNames = null;

	@Parameter(names = "--indexName", description = "The specific index to query; by default one is chosen for each adapter.")
	private String indexName = null;

	public QueryOptionsCommand() {}

	public String[] getAuthorizations() {
		return authorizations;
	}

	public void setAuthorizations(
			final String[] authorizations ) {
		this.authorizations = authorizations;
	}

	public String[] getTypeNames() {
		return typeNames;
	}

	public void setTypeNames(
			final String[] typeNames ) {
		this.typeNames = typeNames;
	}

	public String getIndexName() {
		return indexName;
	}

	public void setIndexName(
			final String indexName ) {
		this.indexName = indexName;
	}
}
