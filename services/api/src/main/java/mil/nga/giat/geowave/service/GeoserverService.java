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
@Path("/geoserver")
public interface GeoserverService
{

	@GET
	@Path("/workspaces")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getWorkspaces();

	@POST
	@Path("/workspaces")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response createWorkspace(
			final FormDataMultiPart multiPart );

	@DELETE
	@Path("/workspaces/{workspace}")
	public Response deleteWorkspace(
			@PathParam("workspace")
			final String workspace );

	@GET
	@Path("/styles")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getStyles();

	@GET
	@Path("/styles/{styleName}")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public Response getStyle(
			@PathParam("styleName")
			final String styleName );

	@POST
	@Path("/styles")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response publishStyle(
			final FormDataMultiPart multiPart );

	@DELETE
	@Path("/styles/{styleName}")
	public Response deleteStyle(
			@PathParam("styleName")
			final String styleName );

	@GET
	@Path("/datastores")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getDatastores(
			@DefaultValue("")
			@QueryParam("workspace")
			String customWorkspace );

	@GET
	@Path("/datastores/{datastoreName}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getDatastore(
			@PathParam("datastoreName")
			final String datastoreName,
			@DefaultValue("")
			@QueryParam("workspace")
			String customWorkspace );

	@POST
	@Path("/datastores")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response publishDatastore(
			final FormDataMultiPart multiPart );

	@DELETE
	@Path("/datastores/{datastoreName}")
	public Response deleteDatastore(
			@PathParam("datastoreName")
			final String datastoreName,
			@DefaultValue("")
			@QueryParam("workspace")
			String customWorkspace );

	@GET
	@Path("/layers")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getLayers();

	@GET
	@Path("/layers/{layerName}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getLayer(
			@PathParam("layerName")
			final String layerName );

	@POST
	@Path("/layers")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response publishLayer(
			final FormDataMultiPart multiPart );

	@DELETE
	@Path("/layers/{layer}")
	public Response deleteLayer(
			@PathParam("layer")
			final String layerName );

}
