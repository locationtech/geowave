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

public class Landsat8DownloadCommandLineOptions
{
	@Parameter(names = "--overwrite", description = "An option to overwrite images that are ingested in the local workspace directory.  By default it will keep an existing image rather than downloading it again.")
	private boolean overwriteIfExists;

	public Landsat8DownloadCommandLineOptions() {}

	public boolean isOverwriteIfExists() {
		return overwriteIfExists;
	}

	public void setOverwriteIfExists(
			boolean overwriteIfExists ) {
		this.overwriteIfExists = overwriteIfExists;
	}
}
