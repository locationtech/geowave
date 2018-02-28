package mil.nga.giat.geowave.service;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Produces(MediaType.APPLICATION_JSON)
@Path("/v0")
public interface BaseService
{
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/operation_status")
	public Response operation_status(
			@QueryParam("id") String id );
}
