package mil.nga.giat.geowave.service.client;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.proxy.WebResourceFactory;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
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
		final FormDataMultiPart multiPart = new FormDataMultiPart();
		multiPart.field(
				"coverage_store_name",
				coverage_store_name);
		if (workspace != null) {
			multiPart.field(
					"workspace",
					workspace);
		}
		final Response resp = geoServerService.getCoverageStore(multiPart);
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

		final FormDataMultiPart multiPart = new FormDataMultiPart();
		multiPart.field(
				"cvgstore",
				cvgstore);
		multiPart.field(
				"coverage_name",
				coverage_name);
		if (workspace != null) {
			multiPart.field(
					"workspace",
					workspace);
		}
		final Response resp = geoServerService.getCoverage(multiPart);
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
		final FormDataMultiPart multiPart = new FormDataMultiPart();
		multiPart.field(
				"datastore_name",
				datastore_name);
		if (workspace != null) {
			multiPart.field(
					"workspace",
					workspace);
		}
		final Response resp = geoServerService.getDataStore(multiPart);
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
		final FormDataMultiPart multiPart = new FormDataMultiPart();
		multiPart.field(
				"layer_name",
				layer_name);

		final Response resp = geoServerService.getFeatureLayer(multiPart);
		return resp;
	}

	public Response getStoreAdapters(
			final String store_name ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();
		multiPart.field(
				"store_name",
				store_name);

		final Response resp = geoServerService.getStoreAdapters(multiPart);
		return resp;
	}

	public Response getStyle(
			final String style_name ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();
		multiPart.field(
				"style_name",
				style_name);

		final Response resp = geoServerService.getStyle(multiPart);
		return resp;
	}

	public Response listCoverageStores(
			final String workspace ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();
		if (workspace != null) {
			multiPart.field(
					"workspace",
					workspace);
		}
		final Response resp = geoServerService.listCoverageStores(multiPart);
		return resp;
	}

	public Response listCoverageStores() {
		return listCoverageStores(null);
	}

	public Response listCoverages(
			final String coverage_store_name,
			final String workspace ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();
		multiPart.field(
				"coverage_store_name",
				coverage_store_name);
		if (workspace != null) {
			multiPart.field(
					"workspace",
					workspace);
		}
		final Response resp = geoServerService.listCoverages(multiPart);
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
		final FormDataMultiPart multiPart = new FormDataMultiPart();
		if (workspace != null) {
			multiPart.field(
					"workspace",
					workspace);
		}
		final Response resp = geoServerService.listDataStores(multiPart);
		return resp;
	}

	public Response listDataStores() {
		return listDataStores(null);
	}

	public Response listFeatureLayers(
			final String workspace,
			final String datastore,
			final Boolean geowaveOnly ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();
		if (workspace != null) {
			multiPart.field(
					"workspace",
					workspace);
		}
		if (datastore != null) {
			multiPart.field(
					"datastore",
					datastore);
		}
		if (geowaveOnly != null) {
			multiPart.field(
					"geowaveOnly",
					geowaveOnly.toString());
		}
		final Response resp = geoServerService.listFeatureLayers(multiPart);
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
		final FormDataMultiPart multiPart = new FormDataMultiPart();
		multiPart.field(
				"GeoWave_store_name",
				GeoWave_store_name);
		if (workspace != null) {
			multiPart.field(
					"workspace",
					workspace);
		}
		if (equalizerHistogramOverride != null) {
			multiPart.field(
					"equalizerHistogramOverride",
					equalizerHistogramOverride.toString());
		}
		if (workspace != null) {
			multiPart.field(
					"interpolationOverride",
					interpolationOverride);
		}
		if (workspace != null) {
			multiPart.field(
					"scaleTo8Bit",
					scaleTo8Bit.toString());
		}
		final Response resp = geoServerService.addCoverageStore(multiPart);
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
		final FormDataMultiPart multiPart = new FormDataMultiPart();
		multiPart.field(
				"cvgstore",
				cvgstore);
		multiPart.field(
				"coverage_name",
				coverage_name);
		if (workspace != null) {
			multiPart.field(
					"workspace",
					workspace);
		}

		final Response resp = geoServerService.addCoverage(multiPart);
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
		final FormDataMultiPart multiPart = new FormDataMultiPart();
		multiPart.field(
				"GeoWave_store_name",
				GeoWave_store_name);
		if (workspace != null) {
			multiPart.field(
					"workspace",
					workspace);
		}
		if (datastore != null) {
			multiPart.field(
					"datastore",
					datastore);
		}
		final Response resp = geoServerService.addDataStore(multiPart);
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
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		multiPart.field(
				"datastore",
				datastore);
		multiPart.field(
				"layer_name",
				layer_name);
		if (workspace != null) {
			multiPart.field(
					"workspace",
					workspace);
		}

		final Response resp = geoServerService.addFeatureLayer(multiPart);
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
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		multiPart.field(
				"GeoWave_store_name",
				GeoWave_store_name);

		if (workspace != null) {
			multiPart.field(
					"workspace",
					workspace);
		}
		if (addOption != null) {
			multiPart.field(
					"addOption",
					addOption);
		}
		if (adapterId != null) {
			multiPart.field(
					"adapterId",
					adapterId);
		}
		if (style != null) {
			multiPart.field(
					"style",
					style);
		}

		final Response resp = geoServerService.addLayer(multiPart);
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
			final String stylesid,
			final String GeoWave_store_name ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();
		multiPart.field(
				"stylesid",
				stylesid);
		multiPart.field(
				"GeoWave_store_name",
				GeoWave_store_name);
		final Response resp = geoServerService.addStyle(multiPart);
		return resp;
	}

	public Response addWorkspace(
			final String workspace_name ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();
		multiPart.field(
				"workspace_name",
				workspace_name);
		final Response resp = geoServerService.addWorkspace(multiPart);
		return resp;
	}

	public Response removeCoverageStore(
			final String coverage_store_name,
			final String workspace ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		multiPart.field(
				"coverage_store_name",
				coverage_store_name);
		if (workspace != null) {
			multiPart.field(
					"workspace",
					workspace);
		}

		final Response resp = geoServerService.removeCoverageStore(multiPart);
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
		final FormDataMultiPart multiPart = new FormDataMultiPart();
		multiPart.field(
				"cvgstore",
				cvgstore);
		multiPart.field(
				"coverage_name",
				coverage_name);
		if (workspace != null) {
			multiPart.field(
					"workspace",
					workspace);
		}

		final Response resp = geoServerService.removeCoverage(multiPart);
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
		final FormDataMultiPart multiPart = new FormDataMultiPart();
		multiPart.field(
				"datastore_name",
				datastore_name);
		if (workspace != null) {
			multiPart.field(
					"workspace",
					workspace);
		}

		final Response resp = geoServerService.removeDataStore(multiPart);
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
		final FormDataMultiPart multiPart = new FormDataMultiPart();
		multiPart.field(
				"layer_name",
				layer_name);
		final Response resp = geoServerService.removeFeatureLayer(multiPart);
		return resp;
	}

	public Response removeStyle(
			final String style_name ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();
		multiPart.field(
				"style_name",
				style_name);
		final Response resp = geoServerService.removeStyle(multiPart);
		return resp;
	}

	public Response removeWorkspace(
			final String workspace_name ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();
		multiPart.field(
				"workspace_name",
				workspace_name);
		final Response resp = geoServerService.removeWorkspace(multiPart);
		return resp;
	}

	public Response setLayerStyle(
			final String styleName,
			final String layer_name ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();
		multiPart.field(
				"styleName",
				styleName);
		multiPart.field(
				"layer_name",
				layer_name);
		final Response resp = geoServerService.setLayerStyle(multiPart);
		return resp;
	}
}
