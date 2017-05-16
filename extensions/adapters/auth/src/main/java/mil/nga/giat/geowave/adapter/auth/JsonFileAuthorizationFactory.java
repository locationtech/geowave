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
package mil.nga.giat.geowave.adapter.auth;

import java.net.URL;

/**
 * Stores authorization data in a json file. Format: { "authorizationSet" : {
 * "fred" : ["auth1","auth2"], "barney" : ["auth1","auth3"] } }
 * 
 */
public class JsonFileAuthorizationFactory implements
		AuthorizationFactorySPI
{
	public AuthorizationSPI create(
			URL url ) {
		return new JsonFileAuthorizationProvider(
				url);
	}

	@Override
	public String toString() {
		return "jsonFile";
	}

}
