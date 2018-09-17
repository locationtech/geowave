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
package org.locationtech.geowave.service.rest;

import java.util.logging.Level;

import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.resource.Finder;
import org.restlet.resource.ServerResource;

public class GeoWaveOperationFinder extends
		Finder
{
	private final ServiceEnabledCommand<?> operation;
	private final String defaultConfigFile;

	public GeoWaveOperationFinder(
			final ServiceEnabledCommand<?> operation,
			String defaultConfigFile ) {
		this.operation = operation;
		this.defaultConfigFile = defaultConfigFile;

	}

	@Override
	public ServerResource create(
			final Class<? extends ServerResource> targetClass,
			final Request request,
			final Response response ) {
		try {
			return new GeoWaveOperationServiceWrapper<>(
					operation.getClass().newInstance(),
					defaultConfigFile);
		}
		catch (InstantiationException | IllegalAccessException e) {
			getLogger().log(
					Level.SEVERE,
					"Unable to instantiate Service Resource",
					e);
			return null;
		}
	}

	@Override
	public Class<? extends ServerResource> getTargetClass() {
		return GeoWaveOperationServiceWrapper.class;
	}

}
