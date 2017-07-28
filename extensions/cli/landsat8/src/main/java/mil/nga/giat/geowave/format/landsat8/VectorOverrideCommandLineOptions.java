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
package mil.nga.giat.geowave.format.landsat8;

import com.beust.jcommander.Parameter;

public class VectorOverrideCommandLineOptions
{
	@Parameter(names = "--vectorstore", description = "By ingesting as both vectors and rasters you may want to ingest into different stores.  This will override the store for vector output.")
	private String vectorStore;
	@Parameter(names = "--vectorindex", description = "By ingesting as both vectors and rasters you may want each indexed differently.  This will override the index used for vector output.")
	private String vectorIndex;

	public VectorOverrideCommandLineOptions() {}

	public String getVectorStore() {
		return vectorStore;
	}

	public String getVectorIndex() {
		return vectorIndex;
	}

	public void setVectorStore(
			String vectorStore ) {
		this.vectorStore = vectorStore;
	}

	public void setVectorIndex(
			String vectorIndex ) {
		this.vectorIndex = vectorIndex;
	}
}
