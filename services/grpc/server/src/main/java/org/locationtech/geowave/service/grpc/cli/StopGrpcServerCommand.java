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
package org.locationtech.geowave.service.grpc.cli;

import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.service.grpc.GeoWaveGrpcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "stop", parentOperation = GrpcSection.class)
@Parameters(commandDescription = "terminates the GeoWave grpc server")
public class StopGrpcServerCommand extends
		DefaultOperation implements
		Command
{
	private static final Logger LOGGER = LoggerFactory.getLogger(StartGrpcServerCommand.class);

	/**
	 * Prep the driver & run the operation.
	 */
	@Override
	public void execute(
			final OperationParams params ) {

		LOGGER.info("Stopping GeoWave grpc server");
		GeoWaveGrpcServer.getInstance().stop();
	}
}
