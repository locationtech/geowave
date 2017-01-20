package mil.nga.giat.geowave.service;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Path("/info")
public interface InfoService
{

	// lists the namespaces in geowave
	// @GET
	// @Produces(MediaType.APPLICATION_JSON)
	// @Path("/namespaces")
	// public Response getNamespaces();

	// lists the indices associated with the given namespace
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/indices/{storeName}")
	public Response getIndices(
			@PathParam("storeName")
			final String storeName );

	// lists the adapters associated with the given namespace
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/adapters/{storeName}")
	public Response getAdapters(
			@PathParam("storeName")
			final String storeName );
}
