package mil.nga.giat.geowave.service;

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
	@Path("/listadapter")
	public Response listAdapter(
			@QueryParam("store_name")
			final String store_name );

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/listindex")
	public Response listIndex(
			@QueryParam("store_name")
			final String store_name );

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/liststats")
	public Response listStats(
			@QueryParam("store_name") String store_name,
			@QueryParam("adapterId") String adapterId,
			@QueryParam("authorizations") String authorizations,
			@QueryParam("jsonFormatFlag") Boolean jsonFormatFlag );

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/version")
	public Response version(
			@QueryParam("store_name") String store_name );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/calcstat")
	public Response calcStat(
			@QueryParam("store_name") String store_name,
			@QueryParam("adapterId") String adapterId,
			@QueryParam("statId") String statId,
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
	@Path("/recalcstats")
	public Response recalcStats(
			@QueryParam("store_name") String store_name,
			@QueryParam("adapterId") String adapterId,
			@QueryParam("authorizations") String authorizations,
			@QueryParam("jsonFormatFlag") Boolean jsonFormatFlag );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/rmadapter")
	public Response removeAdapter(
			@QueryParam("store_name") String store_name,
			@QueryParam("adapterId") String adapterId );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/rmstat")
	public Response removeStat(
			@QueryParam("store_name") String store_name,
			@QueryParam("adapterId") String adapterId,
			@QueryParam("statId") String statId,
			@QueryParam("authorizations") String authorizations,
			@QueryParam("jsonFormatFlag") Boolean jsonFormatFlag );
}
