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
package mil.nga.giat.geowave.format.stanag4676.parser;

import java.io.IOException;

import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackMessage;
import mil.nga.giat.geowave.format.stanag4676.parser.model.TrackRun;

public interface TrackWriter
{

	public void setEncoder(
			TrackEncoder encoder );

	public void initialize(
			TrackRun run );

	public void write(
			TrackRun run )
			throws IOException;

	public void write(
			TrackMessage msg )
			throws IOException;

}
