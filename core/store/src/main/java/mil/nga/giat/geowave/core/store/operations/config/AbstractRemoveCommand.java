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
package mil.nga.giat.geowave.core.store.operations.config;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.cli.api.ServiceStatus;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

/**
 * Common code for removing an entry from the properties file.
 */
public abstract class AbstractRemoveCommand extends
		ServiceEnabledCommand<String>
{

	@Parameter(description = "<name>", required = true, arity = 1)
	private List<String> parameters = new ArrayList<String>();

	protected String pattern = null;

	private ServiceStatus status = ServiceStatus.OK;

	public String getEntryName() {
		if (parameters.size() < 1) {
			throw new ParameterException(
					"Must specify entry name to delete");
		}

		return parameters.get(
				0).trim();
	}

	@Override
	public Pair<ServiceStatus, String> executeService(
			OperationParams params )
			throws Exception {
		String ret = computeResults(params);
		return ImmutablePair.of(
				status,
				ret);
	}

	public String computeResults(
			final OperationParams params,
			final String patternPrefix ) {
		// this ensures we are only exact-matching rather than using the prefix
		final String pattern = patternPrefix + ".";
		final Properties existingProps = getGeoWaveConfigProperties(params);

		// Find properties to remove
		final Set<String> keysToRemove = new HashSet<String>();
		for (final String key : existingProps.stringPropertyNames()) {
			if (key.startsWith(pattern)) {
				keysToRemove.add(key);
			}
		}

		int startSize = existingProps.size();

		// Remove each property.
		for (final String key : keysToRemove) {
			existingProps.remove(key);
		}

		// Write properties file
		ConfigOptions.writeProperties(
				getGeoWaveConfigFile(params),
				existingProps);
		int endSize = existingProps.size();

		if (endSize < startSize) {
			setStatus(ServiceStatus.OK);
			return patternPrefix + " successfully removed";
		}
		else {
			setStatus(ServiceStatus.NOT_FOUND);
			return patternPrefix + " does not exist";

		}
	}

	public ServiceStatus getStatus() {
		return status;
	}

	public void setStatus(
			ServiceStatus status ) {
		this.status = status;
	}

	public void setEntryName(
			final String entryName ) {
		parameters = new ArrayList<String>();
		parameters.add(entryName);
	}
}
