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
package mil.nga.giat.geowave.cli.osm.parser;

import com.beust.jcommander.Parameter;

public class OsmPbfParserOptions
{

	@Parameter(names = "--extension", description = "PBF File extension")
	private String extension = ".pbf";

	private String ingestDirectory;

	private String hdfsBasePath;

	private String nameNode;

	public OsmPbfParserOptions() {
		super();
	}

	public String getExtension() {
		return extension;
	}

	public void setExtension(
			String extension ) {
		this.extension = extension;
	}

	public String getIngestDirectory() {
		return ingestDirectory;
	}

	public void setIngestDirectory(
			String ingestDirectory ) {
		this.ingestDirectory = ingestDirectory;
	}

	public String getHdfsBasePath() {
		return hdfsBasePath;
	}

	public void setHdfsBasePath(
			String hdfsBasePath ) {
		this.hdfsBasePath = hdfsBasePath;
	}

	public String getNameNode() {
		return nameNode;
	}

	public void setNameNode(
			String nameNode ) {
		this.nameNode = nameNode;
	}

	public String getNodesBasePath() {
		return hdfsBasePath + "/nodes";
	}

	public String getWaysBasePath() {
		return hdfsBasePath + "/ways";
	}

	public String getRelationsBasePath() {
		return hdfsBasePath + "/relations";
	}
}
