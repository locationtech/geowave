package mil.nga.giat.geowave.service;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Produces(MediaType.APPLICATION_JSON)
@Path("/v0/gs")
public interface GeoServerService
{

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/getcs")
	public Response getCoverageStore(
			@QueryParam("coverage_store_name") String coverage_store_name,
			@QueryParam("workspace") String workspace );

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/getcv")
	public Response getCoverage(
			@QueryParam("cvg_store") String cvgstore,
			@QueryParam("coverage_name") String coverage_name,
			@QueryParam("workspace") String workspace );

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/getds")
	public Response getDataStore(
			@QueryParam("datastore_name") String datastore_name,
			@QueryParam("workspace") String workspace );

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/getfl")
	public Response getFeatureLayer(
			@QueryParam("layer_name") String layer_name );

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/getsa")
	public Response getStoreAdapters(
			@QueryParam("store_name") String store_name );

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/getstyle")
	public Response getStyle(
			@QueryParam("style_name") String style_name );

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/listcs")
	public Response listCoverageStores(
			@QueryParam("workspace") String workspace );

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/listcv")
	public Response listCoverages(
			@QueryParam("coverage_store_name") String coverage_store_name,
			@QueryParam("workspace") String workspace );

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/listds")
	public Response listDataStores(
			@QueryParam("workspace") String workspace );

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/listfl")
	public Response listFeatureLayers(
			@QueryParam("workspace") String workspace,
			@QueryParam("datastore") String datastore,
			@QueryParam("geowaveOnly") Boolean geowaveOnly );

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
	@Path("/addcs")
	public Response addCoverageStore(
			@QueryParam("GeoWave_store_name") String GeoWave_store_name,
			@QueryParam("workspace") String workspace,
			@QueryParam("equalizerHistogramOverride") Boolean equalizerHistogramOverride,
			@QueryParam("interpolationOverride") String interpolationOverride,
			@QueryParam("scaleTo8Bit") Boolean scaleTo8Bit );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/addcv")
	public Response addCoverage(
			@QueryParam("cvgstore") String cvgstore,
			@QueryParam("coverage_name") String coverage_name,
			@QueryParam("workspace") String workspace );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/addds")
	public Response addDataStore(
			@QueryParam("GeoWave_store_name") String GeoWave_store_name,
			@QueryParam("workspace") String workspace,
			@QueryParam("datastore") String datastore );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/addfl")
	public Response addFeatureLayer(
			@QueryParam("datastore") String datastore,
			@QueryParam("layer_name") String layer_name,
			@QueryParam("workspace") String workspace );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/addlayer")
	public Response addLayer(
			@QueryParam("GeoWave_store_name") String GeoWave_store_name,
			@QueryParam("workspace") String workspace,
			@QueryParam("addOption") String addOption,
			@QueryParam("adapterId") String adapterId,
			@QueryParam("style") String style );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/addstyle")
	public Response addStyle(
			@QueryParam("stylesld") String stylesld,
			@QueryParam("GeoWave_style_name") String GeoWave_style_name );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/addws")
	public Response addWorkspace(
			@QueryParam("workspace_name") String workspace_name );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/rmcs")
	public Response removeCoverageStore(
			@QueryParam("coverage_store_name") String coverage_store_name,
			@QueryParam("workspace") String workspace );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/rmcv")
	public Response removeCoverage(
			@QueryParam("cvgstore") String cvgstore,
			@QueryParam("coverage_name") String coverage_name,
			@QueryParam("workspace") String workspace );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/rmds")
	public Response removeDataStore(
			@QueryParam("datastore_name") String datastore_name,
			@QueryParam("workspace") String workspace );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/rmfl")
	public Response removeFeatureLayer(
			@QueryParam("layer_name") String layer_name );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/rmstyle")
	public Response removeStyle(
			@QueryParam("style_name") String style_name );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/rmws")
	public Response removeWorkspace(
			@QueryParam("workspace_name") String workspace_name );

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/setls")
	public Response setLayerStyle(
			@QueryParam("styleName") String styleName,
			@QueryParam("layer_name") String layer_name );
}