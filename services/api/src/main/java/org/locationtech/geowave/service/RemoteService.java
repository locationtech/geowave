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
package org.locationtech.geowave.service;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Produces(MediaType.APPLICATION_JSON)
@Path("/v0/remote")
public interface RemoteService
{

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/listtypes")
	public Response listTypes(
			@QueryParam("store_name")
			final String store_name );

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/listindices")
	public Response listIndices(
			@QueryParam("store_name")
			final String store_name );

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/liststats")
	public Response listStats(
			@QueryParam("store_name") String store_name,
			@QueryParam("typeName") String typeName,
			@QueryParam("authorizations") String authorizations,
			@QueryParam("jsonFormatFlag") Boolean jsonFormatFlag );

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/version")
	public Response version(
			@QueryParam("store_name") String storename );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/calcstat")
	public Response calcStat(
			@QueryParam("store_name") String store_name,
			@QueryParam("datatype_name") String typeName,
			@QueryParam("stat_type") String statType,
			@QueryParam("authorizations") String authorizations,
			@QueryParam("jsonFormatFlag") Boolean jsonFormatFlag );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/clear")
	public Response clear(
			@QueryParam("store_name") String store_name );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/combinestats")
	public Response combineStats(
			@QueryParam("store_name") String store_name );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/recalcstats")
	public Response recalcStats(
			@QueryParam("store_name") String store_name,
			@QueryParam("typeName") String typeName,
			@QueryParam("authorizations") String authorizations,
			@QueryParam("jsonFormatFlag") Boolean jsonFormatFlag );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/rmtype")
	public Response removeType(
			@QueryParam("store_name") String store_name,
			@QueryParam("datatype_name") String typeName );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/rmstat")
	public Response removeStat(
			@QueryParam("store_name") String store_name,
			@QueryParam("datatype_name") String typeName,
			@QueryParam("stat_type") String statType,
			@QueryParam("authorizations") String authorizations,
			@QueryParam("jsonFormatFlag") Boolean jsonFormatFlag );
}
