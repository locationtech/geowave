package mil.nga.giat.geowave.service;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.media.multipart.FormDataMultiPart;

@Produces(MediaType.APPLICATION_JSON)
@Path("/config")
public interface ConfigService
{

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/list")
	public Response list();

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/addstore/hbase")
	public Response addHBaseStore(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/addstore/accumulo")
	public Response addAccumuloStore(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/addstore/bigtable")
	public Response addBigTableStore(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/addindex/spatial")
	public Response addSpatialIndex(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/addindex/spatial_temporal")
	public Response addSpatialTemporalIndex(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/geoserver")
	public Response configGeoServer(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/hdfs")
	public Response configHDFS(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/rmindex")
	public Response removeIndex(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/rmstore")
	public Response removeStore(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/rmindexgrp")
	public Response removeIndexGroup(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/set")
	public Response set(
			FormDataMultiPart multipart );
}
