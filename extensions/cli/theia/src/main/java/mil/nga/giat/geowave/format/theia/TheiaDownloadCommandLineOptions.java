/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.format.theia;

import com.beust.jcommander.Parameter;

public class TheiaDownloadCommandLineOptions
{
	@Parameter(names = "--userident", description = "email address to authentificate when downloading Theia imagery.", required = true)
	private String userIdent;
	@Parameter(names = "--password", description = "Password to authentificate when downloading Theia imagery.", required = true)
	private String password;
	@Parameter(names = "--overwrite", description = "An option to overwrite images that are ingested in the local workspace directory.  By default it will keep an existing image rather than downloading it again.")
	private boolean overwriteIfExists;

	public TheiaDownloadCommandLineOptions() {}

	public String getUserIdent() {
		return userIdent;
	}

	public String getPassword() {
		return password;
	}

	public boolean isOverwriteIfExists() {
		return overwriteIfExists;
	}

	public void setUserIdent(
			String userIdent ) {
		this.userIdent = userIdent;
	}

	public void setPassword(
			String password ) {
		this.password = password;
	}

	public void setOverwriteIfExists(
			boolean overwriteIfExists ) {
		this.overwriteIfExists = overwriteIfExists;
	}
}
