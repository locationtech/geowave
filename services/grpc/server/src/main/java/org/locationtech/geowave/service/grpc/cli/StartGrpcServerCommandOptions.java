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

import com.beust.jcommander.Parameter;

public class StartGrpcServerCommandOptions
{
	@Parameter(names = {
		"-p",
		"--port"
	}, required = false, description = "The port to run on")
	private Integer port = 8980;

	@Parameter(names = {
		"-n",
		"--nonBlocking"
	}, required = false, description = "Should the service run as non-blocking or block until shutdown?")
	private Boolean nonBlocking = false;

	public Integer getPort() {
		return port;
	}

	public Boolean isNonBlocking() {
		return nonBlocking;
	}

	public void setPort(
			final Integer p ) {
		port = p;
	}

	public void setNonBlocking(
			final Boolean b ) {
		nonBlocking = b;
	}
}
