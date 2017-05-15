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
package mil.nga.giat.geowave.core.store.operations.remote.options;

import com.beust.jcommander.Parameter;

public class StatsCommandLineOptions
{

	public StatsCommandLineOptions() {}

	@Parameter(names = "--auth", description = "The authorizations used for the statistics calculation as a subset of the accumulo user authorization; by default all authorizations are used.")
	private String authorizations;

	@Parameter(names = "--json", description = "Output in JSON format.")
	private boolean jsonFormatFlag;

	public String getAuthorizations() {
		return authorizations;
	}

	public void setAuthorizations(
			String authorizations ) {
		this.authorizations = authorizations;
	}

	public boolean getJsonFormatFlag() {
		return this.jsonFormatFlag;
	}

	public void setJsonFormatFlag(
			boolean jsonFormatFlag ) {
		this.jsonFormatFlag = jsonFormatFlag;
	}
}
