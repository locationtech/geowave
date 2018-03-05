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
@Path("/ingest")
public interface IngestService
{

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/listplugins")
	public Response listPlugins();

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/kafkaToGW")
	public Response kafkaToGW(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/localToGW")
	public Response localToGW(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/localToHdfs")
	public Response localToHdfs(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/localToKafka")
	public Response localToKafka(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/localToMrGW")
	public Response localToMrGW(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/mrToGW")
	public Response mrToGW(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/sparkToGW")
	public Response sparkToGW(
			FormDataMultiPart multipart );
}
