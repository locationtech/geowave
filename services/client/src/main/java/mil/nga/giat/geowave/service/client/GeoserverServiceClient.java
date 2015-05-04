package mil.nga.giat.geowave.service.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import mil.nga.giat.geowave.service.GeoserverService;
import net.sf.json.JSONObject;

import org.glassfish.jersey.client.proxy.WebResourceFactory;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;

public class GeoserverServiceClient
{
	private final GeoserverService geoserverService;

	public GeoserverServiceClient(
			final String baseUrl ) {

		geoserverService = WebResourceFactory.newResource(
				GeoserverService.class,
				ClientBuilder.newBuilder().register(
						MultiPartFeature.class).build().target(
						baseUrl));
	}

	public JSONObject getWorkspaces() {
		final Response resp = geoserverService.getWorkspaces();
		resp.bufferEntity();
		return JSONObject.fromObject(resp.readEntity(String.class));
	}

	public boolean createWorkspace(
			final String workspace ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		multiPart.field(
				"workspace",
				workspace);

		final Response resp = geoserverService.createWorkspace(multiPart);
		return resp.getStatus() == Status.CREATED.getStatusCode();
	}

	public boolean deleteWorkspace(
			final String workspace ) {
		final Response resp = geoserverService.deleteWorkspace(workspace);
		return resp.getStatus() == Status.OK.getStatusCode();
	}

	public JSONObject getStyles() {
		final Response resp = geoserverService.getStyles();
		resp.bufferEntity();
		return JSONObject.fromObject(resp.readEntity(String.class));
	}

	public InputStream getStyle(
			final String styleName ) {
		return (InputStream) geoserverService.getStyle(
				styleName).getEntity();
	}

	public boolean publishStyle(
			final File[] styleFiles )
			throws FileNotFoundException {
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		for (final File styleFile : styleFiles) {
			multiPart.bodyPart(new FileDataBodyPart(
					"file",
					styleFile));
		}

		final Response resp = geoserverService.publishStyle(multiPart);
		return resp.getStatus() == Status.OK.getStatusCode();
	}

	public boolean deleteStyle(
			final String styleName ) {
		final Response resp = geoserverService.deleteStyle(styleName);
		return resp.getStatus() == Status.OK.getStatusCode();
	}

	public JSONObject getDatastores() {
		return getDatastores("");
	}

	public JSONObject getDatastores(
			final String workspace ) {
		final Response resp = geoserverService.getDatastores(workspace);
		return JSONObject.fromObject(resp.readEntity(String.class));
	}

	public JSONObject getDatastore(
			final String datastoreName ) {
		return getDatastore(
				datastoreName,
				"");
	}

	public JSONObject getDatastore(
			final String datastoreName,
			final String workspace ) {
		return JSONObject.fromObject(geoserverService.getDatastore(
				datastoreName,
				workspace).readEntity(
				String.class));
	}

	public boolean publishDatastore(
			final String zookeeperUrl,
			final String username,
			final String password,
			final String instance,
			final String namespace ) {
		return publishDatastore(
				zookeeperUrl,
				username,
				password,
				instance,
				namespace,
				null,
				null,
				null,
				null);
	}

	public boolean publishDatastore(
			final String zookeeperUrl,
			final String username,
			final String password,
			final String instance,
			final String namespace,
			final String lockMgmt,
			final String authMgmtProvider,
			final String authDataUrl,
			final String workspace ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		multiPart.field(
				"zookeeperUrl",
				zookeeperUrl);

		multiPart.field(
				"username",
				username);

		multiPart.field(
				"password",
				password);

		multiPart.field(
				"instance",
				instance);

		multiPart.field(
				"namespace",
				namespace);

		if (lockMgmt != null) {
			multiPart.field(
					"lockMgmt",
					lockMgmt);
		}

		if (authMgmtProvider != null) {
			multiPart.field(
					"authMgmtPrvdr",
					authMgmtProvider);

			if (authDataUrl != null) {
				multiPart.field(
						"authDataUrl",
						authDataUrl);
			}
		}

		if (workspace != null) {
			multiPart.field(
					"workspace",
					workspace);
		}

		final Response resp = geoserverService.publishDatastore(multiPart);
		return resp.getStatus() == Status.OK.getStatusCode();
	}

	public boolean deleteDatastore(
			final String datastore ) {
		final Response resp = geoserverService.deleteDatastore(
				datastore,
				"");
		return resp.getStatus() == Status.OK.getStatusCode();
	}

	public boolean deleteDatastore(
			final String datastore,
			final String workspace ) {
		final Response resp = geoserverService.deleteDatastore(
				datastore,
				workspace);
		return resp.getStatus() == Status.OK.getStatusCode();
	}

	public JSONObject getLayers() {
		final Response resp = geoserverService.getLayers();
		return JSONObject.fromObject(resp.readEntity(String.class));
	}

	public JSONObject getLayer(
			final String layerName ) {
		return JSONObject.fromObject(geoserverService.getLayer(
				layerName).readEntity(
				String.class));
	}

	public boolean publishLayer(
			final String datastore,
			final String defaultStyle,
			final String featureTypeName ) {
		return publishLayer(
				datastore,
				defaultStyle,
				featureTypeName,
				null);
	}

	public boolean publishLayer(
			final String datastore,
			final String defaultStyle,
			final String featureTypeName,
			final String workspace ) {
		final FormDataMultiPart multiPart = new FormDataMultiPart();

		multiPart.field(
				"datastore",
				datastore);

		multiPart.field(
				"defaultStyle",
				defaultStyle);

		if (workspace != null) {
			multiPart.field(
					"workspace",
					workspace);
		}

		final String json = createFeatureTypeJson(featureTypeName);

		multiPart.field(
				"featureType",
				json);

		final Response resp = geoserverService.publishLayer(multiPart);
		return resp.getStatus() == Status.OK.getStatusCode();
	}

	public boolean deleteLayer(
			final String layerName ) {
		final Response resp = geoserverService.deleteLayer(layerName);
		return resp.getStatus() == Status.OK.getStatusCode();
	}

	private String createFeatureTypeJson(
			final String featureTypeName ) {

		final JSONObject featTypeJson = new JSONObject();

		featTypeJson.put(
				"name",
				featureTypeName);

		final JSONObject jsonObj = new JSONObject();
		jsonObj.put(
				"featureType",
				featTypeJson);

		return jsonObj.toString();
	}
}
