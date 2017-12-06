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
package mil.nga.giat.geowave.core.ingest.local;

import java.io.Serializable;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

/**
 * This class encapsulates all of the options and parsed values specific to
 * directing the ingestion framework to a local file system. The user must set
 * an input file or directory and can set a list of extensions to narrow the
 * ingestion to. The process will recurse a directory and filter by the
 * extensions if provided.
 */
public class LocalInputCommandLineOptions implements
		Serializable
{
	@Parameter(names = {
		"-x",
		"--extension"
	}, description = "individual or comma-delimited set of file extensions to accept (optional)", converter = SplitConverter.class)
	private String[] extensions;

	@Parameter(names = {
		"-f",
		"--formats"
	}, description = "Explicitly set the ingest formats by name (or multiple comma-delimited formats), if not set all available ingest formats will be used")
	private String formats;

	public String[] getExtensions() {
		return extensions;
	}

	public String getFormats() {
		return formats;
	}

	public static class SplitConverter implements
			IStringConverter<String[]>
	{
		@Override
		public String[] convert(
				String value ) {
			return value.trim().split(
					",");
		}
	}

	public void setExtensions(
			String[] extensions ) {
		this.extensions = extensions;
	}

	public void setFormats(
			String formats ) {
		this.formats = formats;
	}
}
