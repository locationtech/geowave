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
import org.locationtech.geowave.service.RemoteService;

public class RemoteServiceClient
{
	private final RemoteService remoteService;

	public RemoteServiceClient(
			final String baseUrl ) {
		this(
				baseUrl,
				null,
				null);
	}

	public RemoteServiceClient(
			final String baseUrl,
			String user,
			String password ) {

		remoteService = WebResourceFactory.newResource(
				RemoteService.class,
				ClientBuilder.newClient().register(
						MultiPartFeature.class).target(
						baseUrl));
	}

	// }

	public Response listTypes(
			final String store_name ) {
		final Response resp = remoteService.listTypes(store_name);
		return resp;
	}

	public Response listIndices(
			final String store_name ) {
		final Response resp = remoteService.listIndices(store_name);
		return resp;
	}

	public Response version(
			final String storename ) {

		final Response resp = remoteService.version(storename);
		return resp;
	}

	public Response listStats(
			final String store_name ) {

		return listStats(
				store_name,
				null,
				null,
				null);
	}

	public Response listStats(
			final String store_name,
			final String typeName,
			final String authorizations,
			final Boolean jsonFormatFlag ) {
		final Response resp = remoteService.listStats(
				store_name,
				typeName,
				authorizations,
				jsonFormatFlag);
		return resp;
	}

	public Response calcStat(
			final String store_name,
			final String typeName,
			final String statId ) {

		return calcStat(
				store_name,
				typeName,
				statId,
				null,
				null);
	}

	public Response calcStat(
			final String store_name,
			final String typeName,
			final String statType,
			final String authorizations,
			final Boolean jsonFormatFlag ) {

		final Response resp = remoteService.calcStat(
				store_name,
				typeName,
				statType,
				authorizations,
				jsonFormatFlag);
		return resp;
	}

	public Response clear(
			final String store_name ) {

		final Response resp = remoteService.clear(store_name);
		return resp;
	}

	public Response recalcStats(
			final String store_name ) {

		return recalcStats(
				store_name,
				null,
				null,
				null);
	}

	public Response recalcStats(
			final String store_name,
			final String typeName,
			final String authorizations,
			final Boolean jsonFormatFlag ) {

		final Response resp = remoteService.recalcStats(
				store_name,
				typeName,
				authorizations,
				jsonFormatFlag);
		return resp;
	}

	public Response removeType(
			final String store_name,
			final String typeName ) {

		final Response resp = remoteService.removeType(
				store_name,
				typeName);
		return resp;
	}

	public Response removeStat(
			final String store_name,
			final String typeName,
			final String statType,
			final String authorizations,
			final Boolean jsonFormatFlag ) {

		final Response resp = remoteService.removeStat(
				store_name,
				typeName,
				statType,
				authorizations,
				jsonFormatFlag);
		return resp;
	}

	public Response removeStat(
			final String store_name,
			final String typeName,
			final String statType ) {
		return removeStat(
				store_name,
				typeName,
				statType,
				null,
				null);
	}
}
