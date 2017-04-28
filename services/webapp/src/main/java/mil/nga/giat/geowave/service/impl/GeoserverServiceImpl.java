package mil.nga.giat.geowave.service.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletConfig;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveGTDataStoreFactory;
import mil.nga.giat.geowave.adapter.vector.plugin.GeoWavePluginConfig;
import mil.nga.giat.geowave.core.cli.operations.config.security.utils.SecurityUtils;
import mil.nga.giat.geowave.core.cli.utils.URLUtils;
import mil.nga.giat.geowave.service.GeoserverService;
import mil.nga.giat.geowave.service.ServiceUtils;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;

@Produces(MediaType.APPLICATION_JSON)
@Path("/geoserver")
public class GeoserverServiceImpl implements
		GeoserverService
{
	private final static Logger log = LoggerFactory.getLogger(GeoserverServiceImpl.class);
	private final static int defaultIndentation = 2;

	private String geoserverUrl;
	private final String geoserverUser;
	private String geoserverPass;
	private final String defaultWorkspace;

	public GeoserverServiceImpl(
			@Context
			final ServletConfig servletConfig ) {
		Properties props = null;
		String confPropFilename = servletConfig.getInitParameter("config.properties");
		try (InputStream is = servletConfig.getServletContext().getResourceAsStream(
				confPropFilename)) {
			props = ServiceUtils.loadProperties(is);
		}
		catch (IOException e) {
			log.error(
					e.getLocalizedMessage(),
					e);
		}

		geoserverUrl = ServiceUtils.getProperty(
				props,
				"geoserver.url");

		try {
			geoserverUrl = URLUtils.getUrl(geoserverUrl);
		}
		catch (MalformedURLException | URISyntaxException e) {
			log.error(
					"An error occurred validating url [" + e.getLocalizedMessage() + "]",
					e);
		}

		geoserverUser = ServiceUtils.getProperty(
				props,
				"geoserver.username");

		geoserverPass = ServiceUtils.getProperty(
				props,
				"geoserver.password");

		try {
			File resourceFile = SecurityUtils.getFormattedTokenKeyFileForConfig(new File(
					confPropFilename));
			geoserverPass = SecurityUtils.decryptHexEncodedValue(
					geoserverPass,
					resourceFile.getAbsolutePath());
		}
		catch (Exception e) {
			log.error(
					"An error occurred decrypting password: " + e.getLocalizedMessage(),
					e);
		}

		defaultWorkspace = ServiceUtils.getProperty(
				props,
				"geoserver.workspace");
	}

	@Override
	@GET
	@Path("/workspaces")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getWorkspaces() {

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		final Response resp = target.path(
				"rest/workspaces.json").request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {

			resp.bufferEntity();

			// get the workspace names
			final JSONArray workspaceArray = getArrayEntryNames(
					JSONObject.fromObject(resp.readEntity(String.class)),
					"workspaces",
					"workspace");

			final JSONObject workspacesObj = new JSONObject();
			workspacesObj.put(
					"workspaces",
					workspaceArray);

			return Response.ok(
					workspacesObj.toString(defaultIndentation)).build();
		}

		return resp;
	}

	private JSONArray getArrayEntryNames(
			JSONObject jsonObj,
			final String firstKey,
			final String secondKey ) {
		// get the top level object/array
		if (jsonObj.get(firstKey) instanceof JSONObject) {
			jsonObj = jsonObj.getJSONObject(firstKey);
		}
		else if (jsonObj.get(firstKey) instanceof JSONArray) {
			final JSONArray tempArray = jsonObj.getJSONArray(firstKey);
			if (tempArray.size() > 0) {
				jsonObj = tempArray.getJSONObject(0);
			}
		}

		// get the sub level object/array
		final JSONArray entryArray = new JSONArray();
		if (jsonObj.get(secondKey) instanceof JSONObject) {
			final JSONObject entry = new JSONObject();
			entry.put(
					"name",
					jsonObj.getJSONObject(
							secondKey).getString(
							"name"));
			entryArray.add(entry);
		}
		else if (jsonObj.get(secondKey) instanceof JSONArray) {
			final JSONArray entries = jsonObj.getJSONArray(secondKey);
			for (int i = 0; i < entries.size(); i++) {
				final JSONObject entry = new JSONObject();
				entry.put(
						"name",
						entries.getJSONObject(
								i).getString(
								"name"));
				entryArray.add(entry);
			}
		}
		return entryArray;
	}

	@Override
	@POST
	@Path("/workspaces")
	@Produces(MediaType.APPLICATION_JSON)
	public Response createWorkspace(
			final FormDataMultiPart multiPart ) {

		final String workspace = multiPart.getField(
				"workspace").getValue();

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		return target.path(
				"rest/workspaces").request().post(
				Entity.entity(
						"{'workspace':{'name':'" + workspace + "'}}",
						MediaType.APPLICATION_JSON));
	}

	@Override
	@DELETE
	@Path("/workspaces/{workspace}")
	public Response deleteWorkspace(
			@PathParam("workspace")
			final String workspace ) {

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		return target.path(
				"rest/workspaces/" + workspace).queryParam(
				"recurse",
				"true").request().delete();
	}

	@Override
	@GET
	@Path("/styles")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getStyles() {

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		final Response resp = target.path(
				"rest/styles.json").request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {

			resp.bufferEntity();

			// get the style names
			final JSONArray styleArray = getArrayEntryNames(
					JSONObject.fromObject(resp.readEntity(String.class)),
					"styles",
					"style");

			final JSONObject stylesObj = new JSONObject();
			stylesObj.put(
					"styles",
					styleArray);

			return Response.ok(
					stylesObj.toString(defaultIndentation)).build();
		}

		return resp;
	}

	@Override
	@GET
	@Path("/styles/{styleName}")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public Response getStyle(
			@PathParam("styleName")
			final String styleName ) {

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		final Response resp = target.path(
				"rest/styles/" + styleName + ".sld").request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			final InputStream inStream = (InputStream) resp.getEntity();

			return Response.ok(
					inStream,
					MediaType.APPLICATION_XML).header(
					"Content-Disposition",
					"attachment; filename=\"" + styleName + ".sld\"").build();
		}

		return resp;
	}

	@Override
	@POST
	@Path("/styles")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response publishStyle(
			final FormDataMultiPart multiPart ) {

		final Collection<FormDataBodyPart> fileFields = multiPart.getFields("file");
		if (fileFields == null) {
			return Response.noContent().build();
		}

		// read the list of files & upload to geoserver services
		for (final FormDataBodyPart field : fileFields) {
			final String filename = field.getFormDataContentDisposition().getFileName();
			if (filename.endsWith(".sld") || filename.endsWith(".xml")) {
				final String styleName = filename.substring(
						0,
						filename.length() - 4);
				final InputStream inStream = field.getValueAs(InputStream.class);

				final Client client = ClientBuilder.newClient().register(
						HttpAuthenticationFeature.basic(
								geoserverUser,
								geoserverPass));
				final WebTarget target = client.target(geoserverUrl);

				// create a new geoserver style
				target.path(
						"rest/styles").request().post(
						Entity.entity(
								"{'style':{'name':'" + styleName + "','filename':'" + styleName + ".sld'}}",
								MediaType.APPLICATION_JSON));

				// upload the style to geoserver
				final Response resp = target.path(
						"rest/styles/" + styleName).request().put(
						Entity.entity(
								inStream,
								"application/vnd.ogc.sld+xml"));

				if (resp.getStatus() != Status.OK.getStatusCode()) {
					return resp;
				}
			}
		}

		return Response.ok().build();
	}

	@Override
	@DELETE
	@Path("/styles/{styleName}")
	public Response deleteStyle(
			@PathParam("styleName")
			final String styleName ) {

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		return target.path(
				"rest/styles/" + styleName).request().delete();
	}

	@Override
	@GET
	@Path("/datastores")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getDatastores(
			@DefaultValue("") @QueryParam("workspace") String customWorkspace ) {

		customWorkspace = (customWorkspace.equals("")) ? defaultWorkspace : customWorkspace;

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		final Response resp = target.path(
				"rest/workspaces/" + customWorkspace + "/datastores.json").request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {

			resp.bufferEntity();

			// get the datastore names
			final JSONArray datastoreArray = getArrayEntryNames(
					JSONObject.fromObject(resp.readEntity(String.class)),
					"dataStores",
					"dataStore");

			final JSONArray datastores = new JSONArray();
			for (int i = 0; i < datastoreArray.size(); i++) {
				final String name = datastoreArray.getJSONObject(
						i).getString(
						"name");

				final JSONObject dsObj = JSONObject.fromObject(
						getDatastore(
								name,
								customWorkspace).getEntity()).getJSONObject(
						"dataStore");

				// only process the GeoWave datastores
				if ((dsObj != null) && dsObj.containsKey("type") && dsObj.getString(
						"type").startsWith(
						"GeoWave Datastore")) {

					final JSONObject datastore = new JSONObject();

					datastore.put(
							"name",
							name);

					JSONArray entryArray = null;
					if (dsObj.get("connectionParameters") instanceof JSONObject) {
						entryArray = dsObj.getJSONObject(
								"connectionParameters").getJSONArray(
								"entry");
					}
					else if (dsObj.get("connectionParameters") instanceof JSONArray) {
						entryArray = dsObj.getJSONArray(
								"connectionParameters").getJSONObject(
								0).getJSONArray(
								"entry");
					}

					if (entryArray == null) {
						log
								.error("entry Array was null; didn't find a valid connectionParameters datastore object of type JSONObject or JSONArray");
					}
					else {
						// report connection params for each data store
						for (int j = 0; j < entryArray.size(); j++) {
							final JSONObject entry = entryArray.getJSONObject(j);
							final String key = entry.getString("@key");
							final String value = entry.getString("$");

							datastore.put(
									key,
									value);
						}
					}
					datastores.add(datastore);
				}
			}

			final JSONObject dsObj = new JSONObject();
			dsObj.put(
					"dataStores",
					datastores);

			return Response.ok(
					dsObj.toString(defaultIndentation)).build();
		}

		return resp;
	}

	@Override
	@GET
	@Path("/datastores/{datastoreName}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getDatastore(
			@PathParam("datastoreName")
			final String datastoreName,
			@DefaultValue("") @QueryParam("workspace") String customWorkspace ) {

		customWorkspace = (customWorkspace.equals("")) ? defaultWorkspace : customWorkspace;

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		final Response resp = target.path(
				"rest/workspaces/" + customWorkspace + "/datastores/" + datastoreName + ".json").request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			JSONObject datastore = null;
			try {
				datastore = JSONObject.fromObject(IOUtils.toString((InputStream) resp.getEntity()));
			}
			catch (final IOException e) {
				log.error(
						"Unable to parse datastore.",
						e);
			}

			if (datastore != null) {
				return Response.ok(
						datastore.toString(defaultIndentation)).build();
			}
		}

		return resp;
	}

	@Override
	@POST
	@Path("/datastores")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response publishDatastore(
			final FormDataMultiPart multiPart ) {
		final Map<String, List<FormDataBodyPart>> fieldMap = multiPart.getFields();

		String lockMgmt = "memory";
		String authMgmtPrvdr = "empty";
		String authDataUrl = "";
		String customWorkspace = defaultWorkspace;
		String queryIndexStrategy = "Best Match";
		String geowaveStoreType = "memory";
		String name = "geowave";
		final Map<String, String> geowaveStoreConfig = new HashMap<String, String>();
		for (final Entry<String, List<FormDataBodyPart>> e : fieldMap.entrySet()) {
			if ((e.getValue() != null) && !e.getValue().isEmpty()) {
				if (e.getKey().equals(
						"lockMgmt")) {
					lockMgmt = e.getValue().get(
							0).getValue();
				}
				else if (e.getKey().equals(
						"queryIndexStrategy")) {
					queryIndexStrategy = e.getValue().get(
							0).getValue();
				}
				else if (e.getKey().equals(
						"authMgmtPrvdr")) {
					authMgmtPrvdr = e.getValue().get(
							0).getValue();
				}
				else if (e.getKey().equals(
						"authDataUrl")) {
					authDataUrl = e.getValue().get(
							0).getValue();
				}
				else if (e.getKey().equals(
						"workspace")) {
					customWorkspace = e.getValue().get(
							0).getValue();
				}
				else if (e.getKey().equals(
						"name")) {
					name = e.getValue().get(
							0).getValue();
				}
				else if (e.getKey().equals(
						"geowaveStoreType")) {
					geowaveStoreType = e.getValue().get(
							0).getValue();
				}
				else {
					geowaveStoreConfig.put(
							e.getKey(),
							e.getValue().get(
									0).getValue());
				}
			}
		}

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));

		final WebTarget target = client.target(geoserverUrl);

		final String dataStoreJson = createDatastoreJson(
				geowaveStoreType,
				geowaveStoreConfig,
				name,
				lockMgmt,
				authMgmtPrvdr,
				authDataUrl,
				queryIndexStrategy,
				true);

		// create a new geoserver style
		final Response resp = target.path(
				"rest/workspaces/" + customWorkspace + "/datastores").request().post(
				Entity.entity(
						dataStoreJson,
						MediaType.APPLICATION_JSON));

		if (resp.getStatus() == Status.CREATED.getStatusCode()) {
			return Response.ok().build();
		}

		return resp;
	}

	@Override
	@DELETE
	@Path("/datastores/{datastoreName}")
	public Response deleteDatastore(
			@PathParam("datastoreName")
			final String datastoreName,
			@DefaultValue("") @QueryParam("workspace") String customWorkspace ) {

		customWorkspace = (customWorkspace.equals("")) ? defaultWorkspace : customWorkspace;

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		return target.path(
				"rest/workspaces/" + customWorkspace + "/datastores/" + datastoreName).queryParam(
				"recurse",
				"true").request().delete();
	}

	private String createDatastoreJson(
			final String geowaveStoreType,
			final Map<String, String> geowaveStoreConfig,
			final String name,
			final String lockMgmt,
			final String authMgmtProvider,
			final String authDataUrl,
			final String queryIndexStrategy,
			final boolean enabled ) {
		final JSONObject dataStore = new JSONObject();
		dataStore.put(
				"name",
				name);
		dataStore.put(
				"type",
				GeoWaveGTDataStoreFactory.DISPLAY_NAME_PREFIX + geowaveStoreType);
		dataStore.put(
				"enabled",
				Boolean.toString(enabled));

		final JSONObject connParams = new JSONObject();
		for (final Entry<String, String> e : geowaveStoreConfig.entrySet()) {
			connParams.put(
					e.getKey(),
					e.getValue());
		}
		connParams.put(
				"Lock Management",
				lockMgmt);

		connParams.put(
				GeoWavePluginConfig.QUERY_INDEX_STRATEGY_KEY,
				queryIndexStrategy);

		connParams.put(
				"Authorization Management Provider",
				authMgmtProvider);
		if (!authMgmtProvider.equals("empty")) {
			connParams.put(
					"Authorization Data URL",
					authDataUrl);
		}

		dataStore.put(
				"connectionParameters",
				connParams);

		final JSONObject jsonObj = new JSONObject();
		jsonObj.put(
				"dataStore",
				dataStore);

		return jsonObj.toString();
	}

	@Override
	@GET
	@Path("/layers")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getLayers() {

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		final Response resp = target.path(
				"rest/layers.json").request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {

			resp.bufferEntity();

			// get the datastore names
			final JSONArray layerArray = getArrayEntryNames(
					JSONObject.fromObject(resp.readEntity(String.class)),
					"layers",
					"layer");

			final Map<String, List<String>> namespaceLayersMap = new HashMap<String, List<String>>();
			final Pattern p = Pattern.compile("workspaces/(.*?)/datastores/(.*?)/");
			for (int i = 0; i < layerArray.size(); i++) {
				final String name = layerArray.getJSONObject(
						i).getString(
						"name");

				final String layer = (String) getLayer(
						name).getEntity();

				// get the workspace and name for each datastore
				String workspace = null;
				String datastoreName = null;

				final Matcher m = p.matcher(layer);

				if (m.find()) {
					workspace = m.group(1);
					datastoreName = m.group(2);
				}

				if ((datastoreName != null) && (workspace != null)) {

					final JSONObject datastore = JSONObject.fromObject(
							getDatastore(
									datastoreName,
									workspace).getEntity()).getJSONObject(
							"dataStore");

					// only process GeoWave layers
					if ((datastore != null) && datastore.containsKey("type") && datastore.getString(
							"type").startsWith(
							"GeoWave Datastore")) {

						JSONArray entryArray = null;
						if (datastore.get("connectionParameters") instanceof JSONObject) {
							entryArray = datastore.getJSONObject(
									"connectionParameters").getJSONArray(
									"entry");
						}
						else if (datastore.get("connectionParameters") instanceof JSONArray) {
							entryArray = datastore.getJSONArray(
									"connectionParameters").getJSONObject(
									0).getJSONArray(
									"entry");
						}

						if (entryArray == null) {
							log
									.error("entry Array is null - didn't find a connectionParameters datastore object that was a JSONObject or JSONArray");
						}
						else {
							// group layers by namespace
							for (int j = 0; j < entryArray.size(); j++) {
								final JSONObject entry = entryArray.getJSONObject(j);
								final String key = entry.getString("@key");
								final String value = entry.getString("$");

								if (key.startsWith(GeoWavePluginConfig.GEOWAVE_NAMESPACE_KEY)) {
									if (namespaceLayersMap.containsKey(value)) {
										namespaceLayersMap.get(
												value).add(
												name);
									}
									else {
										final ArrayList<String> layers = new ArrayList<String>();
										layers.add(name);
										namespaceLayersMap.put(
												value,
												layers);
									}
									break;
								}
							}
						}
					}
				}
			}

			// create the json object with layers sorted by namespace
			final JSONArray layersArray = new JSONArray();
			for (final Map.Entry<String, List<String>> kvp : namespaceLayersMap.entrySet()) {
				final JSONArray layers = new JSONArray();

				for (int i = 0; i < kvp.getValue().size(); i++) {
					final JSONObject layerObj = new JSONObject();
					layerObj.put(
							"name",
							kvp.getValue().get(
									i));
					layers.add(layerObj);
				}

				final JSONObject layersObj = new JSONObject();
				layersObj.put(
						"namespace",
						kvp.getKey());
				layersObj.put(
						"layers",
						layers);

				layersArray.add(layersObj);
			}

			final JSONObject layersObj = new JSONObject();
			layersObj.put(
					"layers",
					layersArray);

			return Response.ok(
					layersObj.toString(defaultIndentation)).build();
		}

		return resp;
	}

	@Override
	@GET
	@Path("/layers/{layerName}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getLayer(
			@PathParam("layerName")
			final String layerName ) {

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		final Response resp = target.path(
				"rest/layers/" + layerName + ".json").request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			JSONObject layer = null;
			try {
				layer = JSONObject.fromObject(IOUtils.toString((InputStream) resp.getEntity()));
			}
			catch (final IOException e) {
				log.error(
						"Unable to parse layer.",
						e);
			}

			if (layer != null) {
				return Response.ok(
						layer.toString(defaultIndentation)).build();
			}
		}

		return resp;
	}

	@Override
	@POST
	@Path("/layers")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response publishLayer(
			final FormDataMultiPart multiPart ) {

		final String datastore = multiPart.getField(
				"datastore").getValue();

		final String defaultStyle = multiPart.getField(
				"defaultStyle").getValue();

		final String customWorkspace = (multiPart.getField("workspace") != null) ? multiPart.getField(
				"workspace").getValue() : defaultWorkspace;

		String jsonString;
		try {
			jsonString = IOUtils.toString(multiPart.getField(
					"featureType").getValueAs(
					InputStream.class));
		}
		catch (final IOException e) {
			throw new WebApplicationException(
					Response.status(
							Status.BAD_REQUEST).entity(
							"Layer Creation Failed - Unable to parse featureType").build());
		}

		final String layerName = JSONObject.fromObject(
				jsonString).getJSONObject(
				"featureType").getString(
				"name");

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		Response resp = target.path(
				"rest/workspaces/" + customWorkspace + "/datastores/" + datastore + "/featuretypes").request().post(
				Entity.entity(
						jsonString,
						MediaType.APPLICATION_JSON));

		if (resp.getStatus() != Status.CREATED.getStatusCode()) {
			return resp;
		}

		resp = target.path(
				"rest/layers/" + layerName).request().put(
				Entity.entity(
						"{'layer':{'defaultStyle':{'name':'" + defaultStyle + "'}}}",
						MediaType.APPLICATION_JSON));

		return resp;
	}

	@Override
	@DELETE
	@Path("/layers/{layer}")
	public Response deleteLayer(
			@PathParam("layer")
			final String layerName ) {

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		return target.path(
				"rest/layers/" + layerName).request().delete();
	}
}