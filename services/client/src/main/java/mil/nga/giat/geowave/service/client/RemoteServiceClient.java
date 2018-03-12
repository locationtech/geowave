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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import mil.nga.giat.geowave.service.ConfigService;
import mil.nga.giat.geowave.service.RemoteService;
import net.sf.json.JSONObject;

import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.client.proxy.WebResourceFactory;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;

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
		// ClientBuilder bldr = ClientBuilder.newBuilder();
		// if (user != null && password != null) {
		// HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic(
		// user,
		// password);
		// bldr.register(feature);
		// }
		remoteService = WebResourceFactory.newResource(
				RemoteService.class,
				ClientBuilder.newClient().register(
						MultiPartFeature.class).target(
						baseUrl));
	}

	// }

	public Response listAdapter(
			final String store_name ) {

		final Response resp = remoteService.listAdapter(store_name);
		return resp;
	}

	public Response listIndex(
			final String store_name ) {
		final Response resp = remoteService.listIndex(store_name);
		return resp;
	}

	public Response version(
			final String store_name ) {

		final Response resp = remoteService.version(store_name);
		return resp;
	}

	public Response listStats(
			final String store_name ) {

		return recalcStats(
				store_name,
				null,
				null,
				null);
	}

	public Response listStats(
			final String store_name,
			final String adapterId,
			final String authorizations,
			final Boolean jsonFormatFlag ) {
		final Response resp = remoteService.listStats(
				store_name,
				adapterId,
				authorizations,
				jsonFormatFlag);
		return resp;
	}

	public Response calcStat(
			final String store_name,
			final String adapterId,
			final String statId ) {

		return calcStat(
				store_name,
				adapterId,
				statId,
				null,
				null);
	}

	public Response calcStat(
			final String store_name,
			final String adapterId,
			final String statId,
			final String authorizations,
			final Boolean jsonFormatFlag ) {

		final Response resp = remoteService.calcStat(
				store_name,
				adapterId,
				statId,
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
			final String adapterId,
			final String authorizations,
			final Boolean jsonFormatFlag ) {

		final Response resp = remoteService.recalcStats(
				store_name,
				adapterId,
				authorizations,
				jsonFormatFlag);
		return resp;
	}

	public Response removeAdapter(
			final String store_name,
			final String adapterId ) {

		final Response resp = remoteService.removeAdapter(
				store_name,
				adapterId);
		return resp;
	}

	public Response removeStat(
			final String store_name,
			final String adapterId,
			final String statId,
			final String authorizations,
			final Boolean jsonFormatFlag ) {

		final Response resp = remoteService.removeStat(
				store_name,
				adapterId,
				statId,
				authorizations,
				jsonFormatFlag);
		return resp;
	}

	public Response removeStat(
			final String store_name,
			final String adapterId,
			final String statId ) {
		return removeStat(
				store_name,
				adapterId,
				statId,
				null,
				null);
	}
}
