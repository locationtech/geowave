package mil.nga.giat.geowave.service.client;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.proxy.WebResourceFactory;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import mil.nga.giat.geowave.service.GeoServerService;

public class GeoServerServiceClient
{
	private final GeoServerService geoServerService;

	public GeoServerServiceClient(
			final String baseUrl ) {
		this(
				baseUrl,
				null,
				null);
	}

	public GeoServerServiceClient(
			final String baseUrl,
			String user,
			String password ) {
		// ClientBuilder bldr = ClientBuilder.newBuilder();
		// if (user != null && password != null) {
		// HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic(
		// user,
		// password);
		// bldr.register(feature);
		// }
		geoServerService = WebResourceFactory.newResource(
				GeoServerService.class,
				ClientBuilder.newClient().register(
						MultiPartFeature.class).target(
						baseUrl));
	}

	public Response getCoverageStore(
			final String coverage_store_name,
			final String workspace ) {

		final Response resp = geoServerService.getCoverageStore(
				coverage_store_name,
				workspace);
		return resp;
	}

	public Response getCoverageStore(
			final String coverage_store_name ) {
		return getCoverageStore(
				coverage_store_name,
				null);
	}

	public Response getCoverage(
			final String cvgstore,
			final String coverage_name,
			final String workspace ) {

		final Response resp = geoServerService.getCoverage(
				cvgstore,
				coverage_name,
				workspace);
		return resp;
	}

	public Response getCoverage(
			final String cvgstore,
			final String coverage_name ) {
		return getCoverage(
				cvgstore,
				coverage_name,
				null);
	}

	public Response getDataStore(
			final String datastore_name,
			final String workspace ) {
		final Response resp = geoServerService.getDataStore(
				datastore_name,
				workspace);
		return resp;
	}

	public Response getDataStore(
			final String datastore_name ) {
		return getDataStore(
				datastore_name,
				null);
	}

	public Response getFeatureLayer(
			final String layer_name ) {

		final Response resp = geoServerService.getFeatureLayer(layer_name);
		return resp;
	}

	public Response getStoreAdapters(
			final String store_name ) {

		final Response resp = geoServerService.getStoreAdapters(store_name);
		return resp;
	}

	public Response getStyle(
			final String style_name ) {

		final Response resp = geoServerService.getStyle(style_name);
		return resp;
	}

	public Response listCoverageStores(
			final String workspace ) {

		final Response resp = geoServerService.listCoverageStores(workspace);
		return resp;
	}

	public Response listCoverageStores() {
		return listCoverageStores(null);
	}

	public Response listCoverages(
			final String coverage_store_name,
			final String workspace ) {
		final Response resp = geoServerService.listCoverages(
				coverage_store_name,
				workspace);
		return resp;
	}

	public Response listCoverages(
			final String coverage_store_name ) {
		return listCoverages(
				coverage_store_name,
				null);
	}

	public Response listDataStores(
			final String workspace ) {
		final Response resp = geoServerService.listDataStores(workspace);
		return resp;
	}

	public Response listDataStores() {
		return listDataStores(null);
	}

	public Response listFeatureLayers(
			final String workspace,
			final String datastore,
			final Boolean geowaveOnly ) {

		final Response resp = geoServerService.listFeatureLayers(
				workspace,
				datastore,
				geowaveOnly);
		return resp;
	}

	public Response listFeatureLayers() {
		return listFeatureLayers(
				null,
				null,
				null);
	}

	public Response listStyles() {
		return geoServerService.listStyles();
	}

	public Response listWorkspaces() {
		return geoServerService.listWorkspaces();
	}

	// POST Requests
	public Response addCoverageStore(
			final String GeoWave_store_name,
			final String workspace,
			final Boolean equalizerHistogramOverride,
			final String interpolationOverride,
			final Boolean scaleTo8Bit ) {

		final Response resp = geoServerService.addCoverageStore(
				GeoWave_store_name,
				workspace,
				equalizerHistogramOverride,
				interpolationOverride,
				scaleTo8Bit);
		return resp;
	}

	public Response addCoverageStore(
			final String GeoWave_store_name ) {
		return addCoverageStore(
				GeoWave_store_name,
				null,
				null,
				null,
				null);
	}

	public Response addCoverage(
			final String cvgstore,
			final String coverage_name,
			final String workspace ) {

		final Response resp = geoServerService.addCoverage(
				cvgstore,
				coverage_name,
				workspace);
		return resp;
	}

	public Response addCoverage(
			final String cvgstore,
			final String coverage_name ) {
		return addCoverage(
				cvgstore,
				coverage_name,
				null);
	}

	public Response addDataStore(
			final String GeoWave_store_name,
			final String workspace,
			final String datastore ) {

		final Response resp = geoServerService.addDataStore(
				GeoWave_store_name,
				workspace,
				datastore);
		return resp;
	}

	public Response addDataStore(
			final String GeoWave_store_name ) {
		return addDataStore(
				GeoWave_store_name,
				null,
				null);
	}

	public Response addFeatureLayer(
			final String datastore,
			final String layer_name,
			final String workspace ) {

		final Response resp = geoServerService.addFeatureLayer(
				datastore,
				layer_name,
				workspace);
		return resp;
	}

	public Response addFeatureLayer(
			final String datastore,
			final String layer_name ) {
		return addFeatureLayer(
				datastore,
				layer_name,
				null);
	}

	public Response addLayer(
			final String GeoWave_store_name,
			final String workspace,
			final String addOption,
			final String adapterId,
			final String style ) {

		final Response resp = geoServerService.addLayer(
				GeoWave_store_name,
				workspace,
				addOption,
				adapterId,
				style);
		return resp;
	}

	public Response addLayer(
			final String GeoWave_store_name ) {
		return addLayer(
				GeoWave_store_name,
				null,
				null,
				null,
				null);
	}

	public Response addStyle(
			final String stylesld,
			final String GeoWave_style_name ) {

		final Response resp = geoServerService.addStyle(
				stylesld,
				GeoWave_style_name);
		return resp;
	}

	public Response addWorkspace(
			final String workspace_name ) {

		final Response resp = geoServerService.addWorkspace(workspace_name);
		return resp;
	}

	public Response removeCoverageStore(
			final String coverage_store_name,
			final String workspace ) {

		final Response resp = geoServerService.removeCoverageStore(
				coverage_store_name,
				workspace);
		return resp;
	}

	public Response removeCoverageStore(
			final String coverage_store_name ) {
		return removeCoverageStore(
				coverage_store_name,
				null);
	}

	public Response removeCoverage(
			final String cvgstore,
			final String coverage_name,
			final String workspace ) {

		final Response resp = geoServerService.removeCoverage(
				cvgstore,
				coverage_name,
				workspace);
		return resp;
	}

	public Response removeCoverage(
			final String cvgstore,
			final String coverage_name ) {
		return removeCoverage(
				cvgstore,
				coverage_name,
				null);
	}

	public Response removeDataStore(
			final String datastore_name,
			final String workspace ) {

		final Response resp = geoServerService.removeDataStore(
				datastore_name,
				workspace);
		return resp;
	}

	public Response removeDataStore(
			final String datastore_name ) {
		return removeDataStore(
				datastore_name,
				null);
	}

	public Response removeFeatureLayer(
			final String layer_name ) {

		final Response resp = geoServerService.removeFeatureLayer(layer_name);
		return resp;
	}

	public Response removeStyle(
			final String style_name ) {

		final Response resp = geoServerService.removeStyle(style_name);
		return resp;
	}

	public Response removeWorkspace(
			final String workspace_name ) {

		final Response resp = geoServerService.removeWorkspace(workspace_name);
		return resp;
	}

	public Response setLayerStyle(
			final String styleName,
			final String layer_name ) {

		final Response resp = geoServerService.setLayerStyle(
				styleName,
				layer_name);
		return resp;
	}
}
