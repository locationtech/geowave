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
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/namespaces")
	public Response getNamespaces();

	// lists the indices associated with the given namespace
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/namespaces/{namespace}/indices")
	public Response getIndices(
			@PathParam("namespace")
			String namespace );

	// lists the adapters associated with the given namespace
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/namespaces/{namespace}/adapters")
	public Response getAdapters(
			@PathParam("namespace")
			String namespace );
}
