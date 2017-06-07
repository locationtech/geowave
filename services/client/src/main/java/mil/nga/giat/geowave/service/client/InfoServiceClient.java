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
package mil.nga.giat.geowave.service.client;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.proxy.WebResourceFactory;

import mil.nga.giat.geowave.service.InfoService;
import net.sf.json.JSONObject;

public class InfoServiceClient
{

	private final InfoService infoService;

	public InfoServiceClient(
			final String baseUrl ) {
		infoService = WebResourceFactory.newResource(
				InfoService.class,
				ClientBuilder.newClient().target(
						baseUrl));
	}

	// public JSONObject getNamespaces() {
	// final Response resp = infoService.getNamespaces();
	// resp.bufferEntity();
	// return JSONObject.fromObject(resp.readEntity(String.class));
	// }

	public JSONObject getIndices(
			final String storeName ) {
		final Response resp = infoService.getIndices(storeName);
		resp.bufferEntity();
		return JSONObject.fromObject(resp.readEntity(String.class));
	}

	public JSONObject getAdapters(
			final String storeName ) {
		final Response resp = infoService.getAdapters(storeName);
		resp.bufferEntity();
		return JSONObject.fromObject(resp.readEntity(String.class));
	}
}
