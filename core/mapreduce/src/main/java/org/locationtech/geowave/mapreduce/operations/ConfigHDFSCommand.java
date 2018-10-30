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
package org.locationtech.geowave.mapreduce.operations;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.cli.operations.config.ConfigSection;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "hdfs", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Create a local configuration for HDFS")
public class ConfigHDFSCommand extends
		ServiceEnabledCommand<Void>
{
	/**
	 * Return "200 OK" for the config HDFS command.
	 */
	@Override
	public Boolean successStatusIs200() {
		return true;
	}

	private static final String HDFS_DEFAULTFS_PREFIX = "hdfs.defaultFS";
	private static final String HDFS_DEFAULTFS_URL = HDFS_DEFAULTFS_PREFIX + ".url";

	@Parameter(description = "<HDFS DefaultFS URL>")
	private List<String> parameters = new ArrayList<String>();
	private String url = null;

	@Override
	public boolean prepare(
			OperationParams params ) {
		boolean retval = true;
		retval |= super.prepare(params);

		return retval;
	}

	@Override
	public void execute(
			OperationParams params )
			throws Exception {
		computeResults(params);
	}

	public static String getHdfsUrl(
			Properties configProperties ) {
		String hdfsFSUrl = configProperties.getProperty(ConfigHDFSCommand.HDFS_DEFAULTFS_URL);

		if (hdfsFSUrl == null) {
			throw new ParameterException(
					"HDFS DefaultFS URL is empty. Config using \"geowave config hdfs <hdfs DefaultFS>\"");
		}

		if (!hdfsFSUrl.contains("://")) {
			hdfsFSUrl = "hdfs://" + hdfsFSUrl;
		}
		return hdfsFSUrl;
	}

	public void setHdfsUrlParameter(
			String hdfsFsUrl ) {
		parameters = new ArrayList<String>();
		parameters.add(hdfsFsUrl);
	}

	@Override
	public Void computeResults(
			OperationParams params )
			throws Exception {
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires argument: <HDFS DefaultFS URL> (HDFS hostname:port or namenode HA nameservice, eg: sandbox.hortonworks.com:8020 )");
		}
		url = parameters.get(0);
		Properties existingProps = getGeoWaveConfigProperties(params);

		// all switches are optional
		if (url != null) {
			existingProps.setProperty(
					HDFS_DEFAULTFS_URL,
					url);
		}

		// Write properties file
		ConfigOptions.writeProperties(
				getGeoWaveConfigFile(params),
				existingProps,
				this.getClass(),
				HDFS_DEFAULTFS_PREFIX);

		return null;
	}

}
