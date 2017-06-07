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
package mil.nga.giat.geowave.mapreduce.operations;

import mil.nga.giat.geowave.core.cli.converters.GeoWaveBaseConverter;

/**
 * This class will ensure that the hdfs parameter is in the correct format.
 */
public class HdfsHostPortConverter extends
		GeoWaveBaseConverter<String>
{
	public HdfsHostPortConverter(
			String optionName ) {
		super(
				optionName);
	}

	@Override
	public String convert(
			String hdfsHostPort ) {
		if (!hdfsHostPort.contains("://")) {
			hdfsHostPort = "hdfs://" + hdfsHostPort;
		}
		return hdfsHostPort;
	}

	@Override
	public boolean isRequired() {
		return true;
	}
}
