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
package org.locationtech.geowave.service.client;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.proxy.WebResourceFactory;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.locationtech.geowave.service.BaseService;

public class BaseServiceClient
{

	private final BaseService baseService;

	public BaseServiceClient(
			final String baseUrl ) {
		this(
				baseUrl,
				null,
				null);
	}

	public BaseServiceClient(
			final String baseUrl,
			String user,
			String password ) {

		baseService = WebResourceFactory.newResource(
				BaseService.class,
				ClientBuilder.newClient().register(
						MultiPartFeature.class).target(
						baseUrl));
	}

	public Response operation_status(
			final String id ) {

		final Response resp = baseService.operation_status(id);
		return resp;

	}
}
