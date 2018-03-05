package mil.nga.giat.geowave.service;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.media.multipart.FormDataMultiPart;

@Produces(MediaType.APPLICATION_JSON)
@Path("/gs")
public interface GeoServerService
{

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/getcs")
	public Response getCoverageStore(
			FormDataMultiPart multipart );

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/getcv")
	public Response getCoverage(
			FormDataMultiPart multipart );

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/getds")
	public Response getDataStore(
			FormDataMultiPart multipart );

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/getfl")
	public Response getFeatureLayer(
			FormDataMultiPart multipart );

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/getsa")
	public Response getStoreAdapters(
			FormDataMultiPart multipart );

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/getstyle")
	public Response getStyle(
			FormDataMultiPart multipart );

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/listcs")
	public Response listCoverageStores(
			FormDataMultiPart multipart );

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/listcv")
	public Response listCoverages(
			FormDataMultiPart multipart );

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/listds")
	public Response listDataStores(
			FormDataMultiPart multipart );

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/listfl")
	public Response listFeatureLayers(
			FormDataMultiPart multipart );

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/liststyles")
	public Response listStyles();

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/listws")
	public Response listWorkspaces();

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/addcs")
	public Response addCoverageStore(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/addcv")
	public Response addCoverage(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/addds")
	public Response addDataStore(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/addfl")
	public Response addFeatureLayer(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/addlayer")
	public Response addLayer(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/addstyle")
	public Response addStyle(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/addws")
	public Response addWorkspace(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/rmcs")
	public Response removeCoverageStore(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/rmcv")
	public Response removeCoverage(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/rmds")
	public Response removeDataStore(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/rmfl")
	public Response removeFeatureLayer(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/rmstyle")
	public Response removeStyle(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/rmws")
	public Response removeWorkspace(
			FormDataMultiPart multipart );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Path("/setls")
	public Response setLayerStyle(
			FormDataMultiPart multipart );
}