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
package mil.nga.giat.geowave.datastore.accumulo.split;

import com.beust.jcommander.Parameter;

public class SplitCommandLineOptions
{
	@Parameter(names = "--indexId", description = "The geowave index ID (optional; default is all indices)")
	private String indexId;

	@Parameter(names = "--num", required = true, description = "The number of partitions (or entries)")
	private long number;

	public String getIndexId() {
		return indexId;
	}

	public long getNumber() {
		return number;
	}

	public void setIndexId(
			String indexId ) {
		this.indexId = indexId;
	}

	public void setNumber(
			long number ) {
		this.number = number;
	}
}
