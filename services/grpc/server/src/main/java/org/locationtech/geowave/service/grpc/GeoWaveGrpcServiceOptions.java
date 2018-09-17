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
package org.locationtech.geowave.service.grpc;

import java.io.File;

import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;

//This class is intended to provide easily accessible global properties for gRPC clients and servers 
public class GeoWaveGrpcServiceOptions
{
	public static String host = "localhost"; // the ip or address that the
												// server resides at
	public static int port = 8090; // the client and server connection port
									// number

	// the config file that the service implementations will use
	public static File geowaveConfigFile = ConfigOptions.getDefaultPropertyFile();
}
