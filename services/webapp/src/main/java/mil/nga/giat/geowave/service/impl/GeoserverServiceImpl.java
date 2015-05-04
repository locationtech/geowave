package mil.nga.giat.geowave.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
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

import mil.nga.giat.geowave.service.GeoserverService;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;

@Produces(MediaType.APPLICATION_JSON)
@Path("/geoserver")
public class GeoserverServiceImpl implements
		GeoserverService
{
	private final static Logger log = Logger.getLogger(GeoserverServiceImpl.class);
	private final static int defaultIndentation = 2;

	private final String geoserverUrl;
	private final String geoserverUser;
	private final String geoserverPass;
	private final String defaultWorkspace;

	public GeoserverServiceImpl(
			@Context
			final ServletConfig servletConfig ) {
		final Properties props = ServiceUtils.loadProperties(servletConfig.getServletContext().getResourceAsStream(
				servletConfig.getInitParameter("config.properties")));

		geoserverUrl = ServiceUtils.getProperty(
				props,
				"geoserver.url");

		geoserverUser = ServiceUtils.getProperty(
				props,
				"geoserver.username");

		geoserverPass = ServiceUtils.getProperty(
				props,
				"geoserver.password");

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
				"geoserver/rest/workspaces.json").request().get();

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
				"geoserver/rest/workspaces").request().post(
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
				"geoserver/rest/workspaces/" + workspace).queryParam(
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
				"geoserver/rest/styles.json").request().get();

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
				"geoserver/rest/styles/" + styleName + ".sld").request().get();

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

		Collection<FormDataBodyPart> fileFields = multiPart.getFields("file");
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
						"geoserver/rest/styles").request().post(
						Entity.entity(
								"{'style':{'name':'" + styleName + "','filename':'" + styleName + ".sld'}}",
								MediaType.APPLICATION_JSON));

				// upload the style to geoserver
				final Response resp = target.path(
						"geoserver/rest/styles/" + styleName).request().put(
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
				"geoserver/rest/styles/" + styleName).request().delete();
	}

	@Override
	@GET
	@Path("/datastores")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getDatastores(
			@DefaultValue("")
			@QueryParam("workspace")
			String customWorkspace ) {

		customWorkspace = (customWorkspace.equals("")) ? defaultWorkspace : customWorkspace;

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		final Response resp = target.path(
				"geoserver/rest/workspaces/" + customWorkspace + "/datastores.json").request().get();

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
						"type").equals(
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
						log.error("entry Array was null; didn't find a valid connectionParameters datastore object of type JSONObject or JSONArray");
					}
					else {
						// report zookeeper servers, instance name and namespace
						// for
						// each datastore
						for (int j = 0; j < entryArray.size(); j++) {
							final JSONObject entry = entryArray.getJSONObject(j);
							final String key = entry.getString("@key");
							final String value = entry.getString("$");

							if (key.equals("ZookeeperServers")) {
								datastore.put(
										"ZookeeperServers",
										value);
							}
							else if (key.equals("InstanceName")) {
								datastore.put(
										"InstanceName",
										value);
							}
							else if (key.equals("Namespace")) {
								datastore.put(
										"Namespace",
										value);
							}
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
			@DefaultValue("")
			@QueryParam("workspace")
			String customWorkspace ) {

		customWorkspace = (customWorkspace.equals("")) ? defaultWorkspace : customWorkspace;

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		final Response resp = target.path(
				"geoserver/rest/workspaces/" + customWorkspace + "/datastores/" + datastoreName + ".json").request().get();

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

		final String zookeeperUrl = multiPart.getField(
				"zookeeperUrl").getValue();

		final String username = multiPart.getField(
				"username").getValue();

		final String password = multiPart.getField(
				"password").getValue();

		final String instance = multiPart.getField(
				"instance").getValue();

		final String namespace = multiPart.getField(
				"namespace").getValue();

		final String lockMgmt = (multiPart.getField("lockMgmt") != null) ? multiPart.getField(
				"lockMgmt").getValue() : "memory";

		final String authMgmtPrvdr = (multiPart.getField("authMgmtPrvdr") != null) ? multiPart.getField(
				"authMgmtPrvdr").getValue() : "empty";

		final String authDataUrl = (multiPart.getField("authDataUrl") != null) ? multiPart.getField(
				"authDataUrl").getValue() : "";

		final String customWorkspace = (multiPart.getField("workspace") != null) ? multiPart.getField(
				"workspace").getValue() : defaultWorkspace;

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));

		final WebTarget target = client.target(geoserverUrl);

		final String dataStoreJson = createDatastoreJson(
				zookeeperUrl,
				username,
				password,
				instance,
				namespace,
				lockMgmt,
				authMgmtPrvdr,
				authDataUrl,
				true);

		// create a new geoserver style
		final Response resp = target.path(
				"geoserver/rest/workspaces/" + customWorkspace + "/datastores").request().post(
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
			@DefaultValue("")
			@QueryParam("workspace")
			String customWorkspace ) {

		customWorkspace = (customWorkspace.equals("")) ? defaultWorkspace : customWorkspace;

		final Client client = ClientBuilder.newClient().register(
				HttpAuthenticationFeature.basic(
						geoserverUser,
						geoserverPass));
		final WebTarget target = client.target(geoserverUrl);

		return target.path(
				"geoserver/rest/workspaces/" + customWorkspace + "/datastores/" + datastoreName).queryParam(
				"recurse",
				"true").request().delete();
	}

	private String createDatastoreJson(
			final String zookeeperUrl,
			final String username,
			final String password,
			final String instance,
			final String namespace,
			final String lockMgmt,
			final String authMgmtProvider,
			final String authDataUrl,
			final boolean enabled ) {

		final JSONObject dataStore = new JSONObject();
		dataStore.put(
				"name",
				namespace);
		dataStore.put(
				"type",
				"GeoWave Datastore");
		dataStore.put(
				"enabled",
				Boolean.toString(enabled));

		final JSONObject connParams = new JSONObject();
		connParams.put(
				"ZookeeperServers",
				zookeeperUrl);
		connParams.put(
				"UserName",
				username);
		connParams.put(
				"Password",
				password);
		connParams.put(
				"InstanceName",
				instance);
		connParams.put(
				"Namespace",
				namespace);
		connParams.put(
				"Lock Management",
				lockMgmt);
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
				"geoserver/rest/layers.json").request().get();

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
							"type").equals(
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
							log.error("entry Array is null - didn't find a connectionParameters datastore object that was a JSONObject or JSONArray");
						}
						else {
							// group layers by namespace
							for (int j = 0; j < entryArray.size(); j++) {
								final JSONObject entry = entryArray.getJSONObject(j);
								final String key = entry.getString("@key");
								final String value = entry.getString("$");

								if (key.equals("Namespace")) {
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
				"geoserver/rest/layers/" + layerName + ".json").request().get();

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
				"geoserver/rest/workspaces/" + customWorkspace + "/datastores/" + datastore + "/featuretypes").request().post(
				Entity.entity(
						jsonString,
						MediaType.APPLICATION_JSON));

		if (resp.getStatus() != Status.CREATED.getStatusCode()) {
			return resp;
		}

		resp = target.path(
				"geoserver/rest/layers/" + layerName).request().put(
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
				"geoserver/rest/layers/" + layerName).request().delete();
	}
}