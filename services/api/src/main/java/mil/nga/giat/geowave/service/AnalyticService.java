package mil.nga.giat.geowave.service;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.media.multipart.FormDataMultiPart;

@Produces(MediaType.APPLICATION_JSON)
@Path("/analytic")
public interface AnalyticService
{
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/dbscan")
	public Response dbScan(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/kde")
	public Response kde(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/kmeansspark")
	public Response kmeansSpark(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/nn")
	public Response nearestNeighbor(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/sql")
	public Response sql(
			FormDataMultiPart multipart );
}
