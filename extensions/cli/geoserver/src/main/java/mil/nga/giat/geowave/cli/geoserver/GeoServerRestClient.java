package mil.nga.giat.geowave.cli.geoserver;

import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.cli.geoserver.GeoServerAddLayerCommand.AddOption;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.beust.jcommander.ParameterException;

public class GeoServerRestClient
{
	private final static Logger logger = Logger.getLogger(GeoServerRestClient.class);
	private final static int defaultIndentation = 2;

	private class DataAdapterInfo
	{
		String adapterId;
		Boolean isRaster;
	}

	private GeoServerConfig config;
	private WebTarget webTarget = null;

	public GeoServerRestClient(
			GeoServerConfig config ) {
		this.config = config;
		logger.setLevel(Level.DEBUG);
	}

	public GeoServerConfig getConfig() {
		return config;
	}

	private WebTarget getWebTarget() {
		if (webTarget == null) {
			final Client client = ClientBuilder.newClient().register(
					HttpAuthenticationFeature.basic(
							config.getUser(),
							config.getPass()));

			webTarget = client.target(config.getUrl());
		}

		return webTarget;
	}

	// Convenience - add layer(s) for the given store by type
	public Response addLayer(
			String workspaceName,
			final String storeName,
			final String adapterId ) {
		// retrieve the adapter info list for the store
		ArrayList<DataAdapterInfo> adapterInfoList = getStoreAdapterInfo(
				storeName,
				adapterId);

		logger.debug("Finished retrieving adapter list");

		if (adapterInfoList.size() > 1 && adapterId == null) {
			logger.debug("addlayer doesn't know how to deal with multiple adapters");

			String descr = "Please use -a, or choose one of these with -id:";
			JSONObject jsonObj = getJsonFromAdapters(
					adapterInfoList,
					descr);

			logger.debug(jsonObj);

			return Response.ok(
					jsonObj.toString(defaultIndentation)).build();
		}

		// verify the workspace exists
		if (!workspaceExists(workspaceName)) {
			logger.debug("addlayer needs to create the " + workspaceName + " workspace");

			Response addWsResponse = addWorkspace(workspaceName);
			if (addWsResponse.getStatus() != Status.CREATED.getStatusCode()) {
				return addWsResponse;
			}
		}

		String cvgStoreName = storeName + GeoServerConfig.DEFAULT_CS;
		String dataStoreName = storeName + GeoServerConfig.DEFAULT_DS;

		// iterate through data adapters
		for (DataAdapterInfo dataAdapterInfo : adapterInfoList) {
			// handle coverage stores & coverages
			if (dataAdapterInfo.isRaster) {
				// verify coverage store exists
				Response getCsResponse = getCoverageStore(
						workspaceName,
						cvgStoreName);
				if (getCsResponse.getStatus() == Status.NOT_FOUND.getStatusCode()) {
					Response addCsResponse = addCoverageStore(
							workspaceName,
							cvgStoreName,
							storeName);

					if (addCsResponse.getStatus() != Status.CREATED.getStatusCode()) {
						return addCsResponse;
					}
				}
				else if (getCsResponse.getStatus() != Status.OK.getStatusCode()) {
					return getCsResponse;
				}

				// See if the coverage already exists
				Response getCvResponse = getCoverage(
						workspaceName,
						cvgStoreName,
						dataAdapterInfo.adapterId);
				if (getCvResponse.getStatus() == Status.OK.getStatusCode()) {
					logger.debug(dataAdapterInfo.adapterId + " layer already exists");
					continue;
				}

				// We have a coverage store. Add the layer per the adapter ID
				Response addCvResponse = addCoverage(
						workspaceName,
						cvgStoreName,
						dataAdapterInfo.adapterId);
				if (addCvResponse.getStatus() != Status.CREATED.getStatusCode()) {
					return addCvResponse;
				}
			}
			// handle datastores and feature layers
			else {
				// verify datastore exists
				Response getDsResponse = getDatastore(
						workspaceName,
						dataStoreName);
				if (getDsResponse.getStatus() == Status.NOT_FOUND.getStatusCode()) {
					Response addDsResponse = addDatastore(
							workspaceName,
							dataStoreName,
							storeName);
					if (addDsResponse.getStatus() != Status.CREATED.getStatusCode()) {
						return addDsResponse;
					}
				}
				else if (getDsResponse.getStatus() != Status.OK.getStatusCode()) {
					return getDsResponse;
				}
				
				logger.debug("Checking for existing feature layer: " + dataAdapterInfo.adapterId);

				// See if the feature layer already exists
				Response getFlResponse = getFeatureLayer(
						dataAdapterInfo.adapterId);
				if (getFlResponse.getStatus() == Status.OK.getStatusCode()) {
					logger.debug(dataAdapterInfo.adapterId + " layer already exists");
					continue;
				}

				logger.debug("Get feature layer: " + dataAdapterInfo.adapterId + " returned " + getFlResponse.getStatus());
				
				// We have a datastore. Add the layer per the adapter ID
				Response addFlResponse = addFeatureLayer(
						workspaceName,
						dataStoreName,
						dataAdapterInfo.adapterId);
				if (addFlResponse.getStatus() != Status.CREATED.getStatusCode()) {
					return addFlResponse;
				}
			}
		}

		// Report back to the caller the adapter IDs and the types that were used to create the layers
		JSONObject jsonObj = getJsonFromAdapters(
				adapterInfoList,
				"Successfully added:");

		return Response.ok(
				jsonObj.toString(defaultIndentation)).build();
	}

	private JSONObject getJsonFromAdapters(
			ArrayList<DataAdapterInfo> adapterInfoList,
			String description ) {
		StringBuffer buf = new StringBuffer();

		// If we made it this far, let's just iterate through the adapter IDs and build the JSON response data
		buf.append("{'description':'" + description + "', " + "'layers':[");

		for (int i = 0; i < adapterInfoList.size(); i++) {
			DataAdapterInfo info = adapterInfoList.get(i);

			buf.append("{'id':'" + info.adapterId + "',");
			buf.append("'type':'" + (info.isRaster ? "raster" : "vector") + "'}");

			if (i < adapterInfoList.size() - 1) {
				buf.append(",");
			}
		}

		buf.append("]}");

		return JSONObject.fromObject(buf.toString());
	}

	// Workspaces
	public boolean workspaceExists(
			String workspace ) {
		if (workspace == null) {
			workspace = config.getWorkspace();
		}

		Response getWsResponse = getWorkspaces();
		if (getWsResponse.getStatus() == Status.OK.getStatusCode()) {
			JSONObject jsonResponse = JSONObject.fromObject(getWsResponse.getEntity());

			final JSONArray workspaces = jsonResponse.getJSONArray("workspaces");

			for (int i = 0; i < workspaces.size(); i++) {
				String wsName = workspaces.getJSONObject(
						i).getString(
						"name");

				if (wsName.equals(workspace)) {
					return true;
				}
			}
		}
		else {
			logger.error("Error retieving GeoServer workspace list");
		}

		return false;
	}

	public Response getWorkspaces() {
		final Response resp = getWebTarget().path(
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

	public Response addWorkspace(
			final String workspace ) {
		return getWebTarget().path(
				"geoserver/rest/workspaces").request().post(
				Entity.entity(
						"{'workspace':{'name':'" + workspace + "'}}",
						MediaType.APPLICATION_JSON));
	}

	public Response deleteWorkspace(
			final String workspace ) {
		return getWebTarget().path(
				"geoserver/rest/workspaces/" + workspace).queryParam(
				"recurse",
				"true").request().delete();
	}

	// Datastores
	public Response getDatastore(
			final String workspaceName,
			String datastoreName ) {
		final Response resp = getWebTarget().path(
				"geoserver/rest/workspaces/" + workspaceName + "/datastores/" + datastoreName + ".json").request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			resp.bufferEntity();

			JSONObject datastore = JSONObject.fromObject(resp.readEntity(String.class));

			if (datastore != null) {
				return Response.ok(
						datastore.toString(defaultIndentation)).build();
			}
		}

		return resp;
	}

	public Response getDatastores(
			String workspaceName ) {
		final Response resp = getWebTarget().path(
				"geoserver/rest/workspaces/" + workspaceName + "/datastores.json").request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			resp.bufferEntity();

			// get the datastore names
			final JSONArray datastoreArray = getArrayEntryNames(
					JSONObject.fromObject(resp.readEntity(String.class)),
					"dataStores",
					"dataStore");

			final JSONObject dsObj = new JSONObject();
			dsObj.put(
					"dataStores",
					datastoreArray);

			return Response.ok(
					dsObj.toString(defaultIndentation)).build();
		}

		return resp;
	}

	public Response addDatastore(
			String workspaceName,
			String datastoreName,
			String gwStoreName ) {
		DataStorePluginOptions inputStoreOptions = getStorePlugin(gwStoreName);

		if (datastoreName == null || datastoreName.isEmpty()) {
			datastoreName = gwStoreName + GeoServerConfig.DEFAULT_DS;
		}

		String lockMgmt = "memory";
		String authMgmtPrvdr = "empty";
		String authDataUrl = "";
		String queryIndexStrategy = "Best Match";

		final String dataStoreJson = createDatastoreJson(
				"accumulo",
				inputStoreOptions.getFactoryOptionsAsMap(),
				datastoreName,
				lockMgmt,
				authMgmtPrvdr,
				authDataUrl,
				queryIndexStrategy,
				true);

		// create a new geoserver style
		return getWebTarget().path(
				"geoserver/rest/workspaces/" + workspaceName + "/datastores").request().post(
				Entity.entity(
						dataStoreJson,
						MediaType.APPLICATION_JSON));
	}

	public Response deleteDatastore(
			String workspaceName,
			String datastoreName ) {
		return getWebTarget().path(
				"geoserver/rest/workspaces/" + workspaceName + "/datastores/" + datastoreName).queryParam(
				"recurse",
				"true").request().delete();
	}

	// Feature (vector) Layers
	public Response getFeatureLayer(
			final String layerName ) {
		final Response resp = getWebTarget().path(
				"geoserver/rest/layers/" + layerName + ".json").request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			JSONObject layer = JSONObject.fromObject(resp.readEntity(String.class));

			if (layer != null) {
				return Response.ok(
						layer.toString(defaultIndentation)).build();
			}
		}

		return resp;
	}

	/**
	 * Get list of layers from geoserver
	 * 
	 * @param workspaceName
	 *            : if null, don't filter on workspace
	 * @param datastoreName
	 *            : if null, don't filter on datastore
	 * @param geowaveOnly
	 *            : if true, only return geowave layers
	 * @return
	 */
	public Response getFeatureLayers(
			String workspaceName,
			String datastoreName,
			boolean geowaveOnly ) {
		boolean wsFilter = (workspaceName != null && !workspaceName.isEmpty());
		boolean dsFilter = (datastoreName != null && !datastoreName.isEmpty());

		final Response resp = getWebTarget().path(
				"geoserver/rest/layers.json").request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			resp.bufferEntity();

			// get the datastore names
			final JSONArray layerArray = getArrayEntryNames(
					JSONObject.fromObject(resp.readEntity(String.class)),
					"layers",
					"layer");

			// holder for simple layer info (when geowaveOnly = false)
			final JSONArray layerInfoArray = new JSONArray();

			final Map<String, List<String>> namespaceLayersMap = new HashMap<String, List<String>>();
			final Pattern p = Pattern.compile("workspaces/(.*?)/datastores/(.*?)/");
			for (int i = 0; i < layerArray.size(); i++) {
				boolean include = !geowaveOnly && !wsFilter && !dsFilter; // no filtering of any kind

				if (include) { // just grab it...
					layerInfoArray.add(layerArray.getJSONObject(i));
					continue; // and move on
				}

				// at this point, we are filtering somehow. get some more info
				// about the layer
				final String name = layerArray.getJSONObject(
						i).getString(
						"name");

				final String layer = (String) getFeatureLayer(
						name).getEntity();

				// get the workspace and name for each datastore
				String ws = null;
				String ds = null;

				final Matcher m = p.matcher(layer);

				if (m.find()) {
					ws = m.group(1);
					ds = m.group(2);
				}

				// filter on datastore?
				if (!dsFilter || (ds != null && ds.equals(datastoreName))) {

					// filter on workspace?
					if (!wsFilter || (ws != null && ws.equals(workspaceName))) {
						final JSONObject datastore = JSONObject.fromObject(
								getDatastore(
										ds,
										ws).getEntity()).getJSONObject(
								"dataStore");

						// only process GeoWave layers
						if (geowaveOnly) {
							if (datastore != null && datastore.containsKey("type") && datastore.getString(
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
									logger.error("entry Array is null - didn't find a connectionParameters datastore object that was a JSONObject or JSONArray");
								}
								else {
									// group layers by namespace
									for (int j = 0; j < entryArray.size(); j++) {
										final JSONObject entry = entryArray.getJSONObject(j);
										final String key = entry.getString("@key");
										final String value = entry.getString("$");

										if (key.startsWith("gwNamespace")) {
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
						else { // just get all the layers from this store
							layerInfoArray.add(layerArray.getJSONObject(i));
						}
					}
				}
			}

			// Handle geowaveOnly response
			if (geowaveOnly) {
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
			else {
				final JSONObject layersObj = new JSONObject();
				layersObj.put(
						"layers",
						layerInfoArray);

				return Response.ok(
						layersObj.toString(defaultIndentation)).build();
			}
		}

		return resp;
	}

	public Response addFeatureLayer(
			final String workspaceName,
			final String datastoreName,
			final String layerName ) {
		return getWebTarget().path(
				"geoserver/rest/workspaces/" + workspaceName + "/datastores/" + datastoreName + "/featuretypes").request().post(
				Entity.entity(
						"{'featureType':{'name':'" + layerName + "'}}",
						MediaType.APPLICATION_JSON));
	}

	public Response deleteFeatureLayer(
			final String layerName ) {
		return getWebTarget().path(
				"geoserver/rest/layers/" + layerName).request().delete();
	}

	// Coverage Stores
	public Response getCoverageStore(
			final String workspaceName,
			String coverageName ) {
		final Response resp = getWebTarget().path(
				"geoserver/rest/workspaces/" + workspaceName + "/coveragestores/" + coverageName + ".json").request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			resp.bufferEntity();

			JSONObject cvgstore = JSONObject.fromObject(resp.readEntity(String.class));

			if (cvgstore != null) {
				return Response.ok(
						cvgstore.toString(defaultIndentation)).build();
			}
		}

		return resp;
	}

	public Response getCoverageStores(
			String workspaceName ) {
		final Response resp = getWebTarget().path(
				"geoserver/rest/workspaces/" + workspaceName + "/coveragestores.json").request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			resp.bufferEntity();

			// get the datastore names
			final JSONArray coveragesArray = getArrayEntryNames(
					JSONObject.fromObject(resp.readEntity(String.class)),
					"coverageStores",
					"coverageStore");

			final JSONObject dsObj = new JSONObject();
			dsObj.put(
					"coverageStores",
					coveragesArray);

			return Response.ok(
					dsObj.toString(defaultIndentation)).build();
		}

		return resp;
	}

	public Response addCoverageStore(
			String workspaceName,
			String cvgStoreName,
			String gwStoreName ) {
		DataStorePluginOptions inputStoreOptions = getStorePlugin(gwStoreName);

		if (cvgStoreName == null || cvgStoreName.isEmpty()) {
			cvgStoreName = gwStoreName + GeoServerConfig.DEFAULT_CS;
		}

		// Get the store's accumulo config
		Map<String, String> storeConfigMap = inputStoreOptions.getFactoryOptionsAsMap();

		// Add in geoserver coverage store info
		storeConfigMap.put(
				GeoServerConfig.GEOSERVER_WORKSPACE,
				workspaceName);

		storeConfigMap.put(
				"gwNamespace",
				gwStoreName);

		storeConfigMap.put(
				GeoServerConfig.GEOSERVER_CS,
				cvgStoreName);

		final String cvgStoreXml = createCoverageXml(storeConfigMap);

		System.out.println("Add coverage store - xml params:\n" + cvgStoreXml);

		// create a new geoserver style
		return getWebTarget().path(
				"geoserver/rest/workspaces/" + workspaceName + "/coveragestores").request().post(
				Entity.entity(
						cvgStoreXml,
						MediaType.APPLICATION_XML));
	}

	public Response deleteCoverageStore(
			String workspaceName,
			String cvgstoreName ) {
		return getWebTarget().path(
				"geoserver/rest/workspaces/" + workspaceName + "/coveragestores/" + cvgstoreName).queryParam(
				"recurse",
				"true").request().delete();
	}

	// Coverages (raster layers)
	public Response getCoverages(
			String workspaceName,
			String cvsstoreName ) {
		final Response resp = getWebTarget().path(
				"geoserver/rest/workspaces/" + workspaceName + "/coveragestores/" + cvsstoreName + "/coverages.json").request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			resp.bufferEntity();

			// get the datastore names
			final JSONArray coveragesArray = getArrayEntryNames(
					JSONObject.fromObject(resp.readEntity(String.class)),
					"coverages",
					"coverage");

			final JSONObject dsObj = new JSONObject();
			dsObj.put(
					"coverages",
					coveragesArray);

			return Response.ok(
					dsObj.toString(defaultIndentation)).build();
		}

		return resp;
	}

	public Response getCoverage(
			final String workspaceName,
			String cvgStoreName,
			String coverageName ) {
		final Response resp = getWebTarget().path(
				"geoserver/rest/workspaces/" + workspaceName + "/coveragestores/" + cvgStoreName + "/coverages/" + coverageName + ".json").request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			resp.bufferEntity();

			JSONObject cvg = JSONObject.fromObject(resp.readEntity(String.class));

			if (cvg != null) {
				return Response.ok(
						cvg.toString(defaultIndentation)).build();
			}
		}

		return resp;
	}

	public Response addCoverage(
			final String workspaceName,
			final String cvgStoreName,
			final String coverageName ) {
		String jsonString = "{'coverage':" + "{'name':'" + coverageName + "'," + "'nativeCoverageName':'" + coverageName + "'}}";
		logger.debug("Posting JSON: " + jsonString + " to " + workspaceName + "/" + cvgStoreName);

		return getWebTarget().path(
				"geoserver/rest/workspaces/" + workspaceName + "/coveragestores/" + cvgStoreName + "/coverages").request().post(
				Entity.entity(
						jsonString,
						MediaType.APPLICATION_JSON));
	}

	public Response deleteCoverage(
			String workspaceName,
			String cvgstoreName,
			String coverageName ) {
		return getWebTarget().path(
				"geoserver/rest/workspaces/" + workspaceName + "/coveragestores/" + cvgstoreName + "/coverages/" + coverageName).queryParam(
				"recurse",
				"true").request().delete();
	}

	// Internal methods
	protected String createFeatureTypeJson(
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

	protected JSONArray getArrayEntryNames(
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
				if (tempArray.get(0) instanceof JSONObject) {
					jsonObj = tempArray.getJSONObject(0);
				}
				else {
					// empty list!
					return new JSONArray();
				}
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

	protected String createDatastoreJson(
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
				GeoServerConfig.DISPLAY_NAME_PREFIX + geowaveStoreType);
		dataStore.put(
				"enabled",
				Boolean.toString(enabled));

		final JSONObject connParams = new JSONObject();

		if (geowaveStoreConfig != null) {
			for (final Entry<String, String> e : geowaveStoreConfig.entrySet()) {
				connParams.put(
						e.getKey(),
						e.getValue());
			}
		}
		connParams.put(
				"Lock Management",
				lockMgmt);

		connParams.put(
				GeoServerConfig.QUERY_INDEX_STRATEGY_KEY,
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

	private String createCoverageXml(
			Map<String, String> geowaveStoreConfig ) {
		String coverageXml = null;

		String workspace = geowaveStoreConfig.get(GeoServerConfig.GEOSERVER_WORKSPACE);
		String cvgstoreName = geowaveStoreConfig.get(GeoServerConfig.GEOSERVER_CS);

		try {
			// create the post XML
			Document xmlDoc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();

			Element rootEl = xmlDoc.createElement("coverageStore");
			xmlDoc.appendChild(rootEl);

			Element nameEl = xmlDoc.createElement("name");
			nameEl.appendChild(xmlDoc.createTextNode(cvgstoreName));
			rootEl.appendChild(nameEl);

			Element wsEl = xmlDoc.createElement("workspace");
			wsEl.appendChild(xmlDoc.createTextNode(workspace));
			rootEl.appendChild(wsEl);

			Element typeEl = xmlDoc.createElement("type");
			typeEl.appendChild(xmlDoc.createTextNode("GeoWaveRasterFormat"));
			rootEl.appendChild(typeEl);

			Element enabledEl = xmlDoc.createElement("enabled");
			enabledEl.appendChild(xmlDoc.createTextNode("true"));
			rootEl.appendChild(enabledEl);

			Element configEl = xmlDoc.createElement("configure");
			configEl.appendChild(xmlDoc.createTextNode("all"));
			rootEl.appendChild(configEl);

			// Method using custom URL & handler:
			String storeConfigUrl = createParamUrl(geowaveStoreConfig);

			Element urlEl = xmlDoc.createElement("url");
			urlEl.appendChild(xmlDoc.createTextNode(storeConfigUrl));
			rootEl.appendChild(urlEl);

			/*
			 * // Retrieve store config String user = geowaveStoreConfig.get("user"); String pass =
			 * geowaveStoreConfig.get("password"); String zookeeper = geowaveStoreConfig.get("zookeeper"); String
			 * instance = geowaveStoreConfig.get("instance");
			 * 
			 * // Write the temp XML file for the store config writeConfigXml( storeConfigPath, user, pass, zookeeper,
			 * instance, cvgstoreName);
			 */

			// use a transformer to create the xml string for the rest call
			Transformer xformer = TransformerFactory.newInstance().newTransformer();
			DOMSource source = new DOMSource(
					xmlDoc);
			StreamResult result = new StreamResult(
					new StringWriter());

			xformer.transform(
					source,
					result);

			coverageXml = result.getWriter().toString();
		}
		catch (Exception e) {
			e.printStackTrace();
		}

		return coverageXml;
	}

	private String createParamUrl(
			Map<String, String> geowaveStoreConfig ) {
		// Retrieve store config
		String user = geowaveStoreConfig.get("user");
		String pass = geowaveStoreConfig.get("password");
		String zookeeper = geowaveStoreConfig.get("zookeeper");
		String instance = geowaveStoreConfig.get("instance");
		String gwNamespace = geowaveStoreConfig.get("gwNamespace");

		// Create the custom geowave url w/ params
		StringBuffer buf = new StringBuffer();
		buf.append("user=");
		buf.append(user);
		buf.append(";password=");
		buf.append(pass);
		buf.append(";zookeeper=");
		buf.append(zookeeper);
		buf.append(";instance=");
		buf.append(instance);
		buf.append(";gwNamespace=");
		buf.append(gwNamespace);

		return buf.toString();
	}

	public DataStorePluginOptions getStorePlugin(
			String storeName ) {
		StoreLoader inputStoreLoader = new StoreLoader(
				storeName);
		if (!inputStoreLoader.loadFromConfig(config.getPropFile())) {
			throw new ParameterException(
					"Cannot find store name: " + inputStoreLoader.getStoreName());
		}

		return inputStoreLoader.getDataStorePlugin();
	}

	public ArrayList<String> getStoreAdapters(
			String storeName,
			String adapterId ) {
		ArrayList<DataAdapterInfo> adapterInfoList = getStoreAdapterInfo(
				storeName,
				adapterId);

		ArrayList<String> adapterIdList = new ArrayList<String>();

		for (DataAdapterInfo info : adapterInfoList) {
			adapterIdList.add(info.adapterId);
		}

		return adapterIdList;
	}

	private ArrayList<DataAdapterInfo> getStoreAdapterInfo(
			String storeName,
			String adapterId ) {
		DataStorePluginOptions dsPlugin = getStorePlugin(storeName);

		AdapterStore adapterStore = dsPlugin.createAdapterStore();

		ArrayList<DataAdapterInfo> adapterInfoList = new ArrayList<DataAdapterInfo>();

		logger.debug("Adapter list for " + storeName + " with adapterId = " + adapterId + ": ");

		try (final CloseableIterator<DataAdapter<?>> it = adapterStore.getAdapters()) {
			while (it.hasNext()) {
				final DataAdapter<?> adapter = it.next();

				DataAdapterInfo info = getAdapterInfo(
						adapterId,
						adapter);

				if (info != null) {
					adapterInfoList.add(info);
					logger.debug("> '" + info.adapterId + "' adapter passed filter");
				}
			}

		}
		catch (final IOException e) {
			System.err.println("unable to close adapter iterator while looking up coverage names");
		}

		logger.debug("getStoreAdapterInfo(" + storeName + ") got " + adapterInfoList.size() + " ids");

		return adapterInfoList;
	}

	private DataAdapterInfo getAdapterInfo(
			String adapterId,
			DataAdapter adapter ) {
		logger.debug("getAdapterInfo for id = " + adapterId);
		
		DataAdapterInfo info = new DataAdapterInfo();
		info.adapterId = adapter.getAdapterId().getString();
		info.isRaster = false;

		if (adapter instanceof RasterDataAdapter) {
			info.isRaster = true;
		}
		
		logger.debug("> Adapter ID: " + info.adapterId);
		logger.debug("> Adapter Type: " + adapter.getClass().getSimpleName());

		if (adapterId == null || adapterId.equals(AddOption.ALL.name())) {
			logger.debug("id is null or all");
			return info;
		}

		if (adapterId.equals(adapter.getAdapterId().getString())) {
			logger.debug("id matches adapter id");
			return info;
		}

		if (adapterId.equals(AddOption.RASTER.name()) && adapter instanceof RasterDataAdapter) {
			logger.debug("id is all-raster and adapter is raster type");
			return info;
		}

		if (adapterId.equals(AddOption.VECTOR.name()) && adapter instanceof GeotoolsFeatureDataAdapter) {
			logger.debug("id is all-vector and adapter is vector type");
			return info;
		}

		logger.debug("No match!");
		
		return null;
	}

	private void writeConfigXml(
			String storeConfigPath,
			String user,
			String pass,
			String zookeeper,
			String instance,
			String cvgstoreName ) {
		try {
			// create the post XML
			Document xmlDoc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();

			Element configEl = xmlDoc.createElement("config");
			xmlDoc.appendChild(configEl);

			Element userEl = xmlDoc.createElement("user");
			userEl.appendChild(xmlDoc.createTextNode(user));
			configEl.appendChild(userEl);

			Element passEl = xmlDoc.createElement("password");
			passEl.appendChild(xmlDoc.createTextNode(pass));
			configEl.appendChild(passEl);

			Element zkEl = xmlDoc.createElement("zookeeper");
			zkEl.appendChild(xmlDoc.createTextNode(zookeeper));
			configEl.appendChild(zkEl);

			Element instEl = xmlDoc.createElement("instance");
			instEl.appendChild(xmlDoc.createTextNode(instance));
			configEl.appendChild(instEl);

			Element gwnsEl = xmlDoc.createElement("gwNamespace");
			gwnsEl.appendChild(xmlDoc.createTextNode(cvgstoreName));
			configEl.appendChild(gwnsEl);

			Transformer xformer = TransformerFactory.newInstance().newTransformer();
			DOMSource source = new DOMSource(
					xmlDoc);

			String xmlFile = storeConfigPath + "/gwraster.xml";
			FileWriter xmlWriter = new FileWriter(
					xmlFile);

			StreamResult result = new StreamResult(
					xmlWriter);

			xformer.transform(
					source,
					result);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	// Example use of geoserver rest client
	public static void main(
			final String[] args ) {
		// create the client
		GeoServerConfig config = new GeoServerConfig();
		GeoServerRestClient geoserverClient = new GeoServerRestClient(
				config);

		// test getWorkspaces
		// Response getWorkspacesResponse = geoserverClient.getWorkspaces();
		//
		// if (getWorkspacesResponse.getStatus() == Status.OK.getStatusCode()) {
		// System.out.println("\nList of GeoServer workspaces:");
		//
		// JSONObject jsonResponse =
		// JSONObject.fromObject(getWorkspacesResponse.getEntity());
		//
		// final JSONArray workspaces = jsonResponse.getJSONArray("workspaces");
		// for (int i = 0; i < workspaces.size(); i++) {
		// String wsName = workspaces.getJSONObject(
		// i).getString(
		// "name");
		// System.out.println("  > " + wsName);
		// }
		//
		// System.out.println("---\n");
		// }
		// else {
		// System.err.println("Error getting GeoServer workspace list; code = "
		// + getWorkspacesResponse.getStatus());
		// }
		//
		// // test addWorkspace
		// Response addWorkspaceResponse =
		// geoserverClient.addWorkspace("delete-me-ws");
		// if (addWorkspaceResponse.getStatus() ==
		// Status.CREATED.getStatusCode()) {
		// System.out.println("Add workspace 'delete-me-ws' to GeoServer: OK");
		// }
		// else {
		// System.err.println("Error adding workspace 'delete-me-ws' to GeoServer; code = "
		// + addWorkspaceResponse.getStatus());
		// }

		// test coverage store list
		Response listCoveragesResponse = geoserverClient.getCoverageStores("geowave");

		if (listCoveragesResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("\nGeoServer coverage stores list for 'geowave':");

			JSONObject jsonResponse = JSONObject.fromObject(listCoveragesResponse.getEntity());
			JSONArray datastores = jsonResponse.getJSONArray("coverageStores");
			System.out.println(datastores.toString(2));
		}
		else {
			System.err.println("Error getting GeoServer coverage stores list for 'geowave'; code = " + listCoveragesResponse.getStatus());
		}

		// test get coverage store
		Response getCvgStoreResponse = geoserverClient.getCoverageStore(
				"geowave",
				"sfdem");

		if (getCvgStoreResponse.getStatus() == Status.OK.getStatusCode()) {
			System.out.println("\nGeoServer coverage store info for 'geowave/sfdem':");

			JSONObject jsonResponse = JSONObject.fromObject(getCvgStoreResponse.getEntity());
			JSONObject datastore = jsonResponse.getJSONObject("coverageStore");
			System.out.println(datastore.toString(2));
		}
		else {
			System.err.println("Error getting GeoServer coverage store info for 'geowave/sfdem'; code = " + getCvgStoreResponse.getStatus());
		}

		// test add store
		// HashMap<String, String> geowaveStoreConfig = new HashMap<String,
		// String>();
		// geowaveStoreConfig.put(
		// "user",
		// "root");
		// geowaveStoreConfig.put(
		// "password",
		// "password");
		// geowaveStoreConfig.put(
		// "gwNamespace",
		// "ne_50m_admin_0_countries");
		// geowaveStoreConfig.put(
		// "zookeeper",
		// "localhost:2181");
		// geowaveStoreConfig.put(
		// "instance",
		// "geowave");
		//
		// Response addStoreResponse = geoserverClient.addDatastore(
		// "delete-me-ws",
		// "delete-me-ds",
		// "accumulo",
		// geowaveStoreConfig);
		//
		// if (addStoreResponse.getStatus() == Status.OK.getStatusCode() ||
		// addStoreResponse.getStatus() == Status.CREATED.getStatusCode()) {
		// System.out.println("Add store 'delete-me-ds' to workspace 'delete-me-ws' on GeoServer: OK");
		// }
		// else {
		// System.err.println("Error adding store 'delete-me-ds' to workspace 'delete-me-ws' on GeoServer; code = "
		// + addStoreResponse.getStatus());
		// }
		//
		// // test getLayer
		// Response getLayerResponse = geoserverClient.getLayer("states");
		//
		// if (getLayerResponse.getStatus() == Status.OK.getStatusCode()) {
		// System.out.println("\nGeoServer layer info for 'states':");
		//
		// JSONObject jsonResponse =
		// JSONObject.fromObject(getLayerResponse.getEntity());
		// System.out.println(jsonResponse.toString(2));
		// }
		// else {
		// System.err.println("Error getting GeoServer layer info for 'states'; code = "
		// + getLayerResponse.getStatus());
		// }

		// test list layers
		// Response listLayersResponse = geoserverClient.getLayers(
		// "topp",
		// null,
		// false);
		// if (listLayersResponse.getStatus() == Status.OK.getStatusCode()) {
		// System.out.println("\nGeoServer layer list:");
		// JSONObject listObj =
		// JSONObject.fromObject(listLayersResponse.getEntity());
		// System.out.println(listObj.toString(2));
		// }
		// else {
		// System.err.println("Error getting GeoServer layer list; code = " +
		// listLayersResponse.getStatus());
		// }

		// test add layer
		// Response addLayerResponse = geoserverClient.addLayer(
		// "delete-me-ws",
		// "delete-me-ds",
		// "polygon",
		// "ne_50m_admin_0_countries");
		//
		// if (addLayerResponse.getStatus() == Status.OK.getStatusCode()) {
		// System.out.println("\nGeoServer layer add response for 'ne_50m_admin_0_countries':");
		//
		// JSONObject jsonResponse = JSONObject.fromObject(addLayerResponse.getEntity());
		// System.out.println(jsonResponse.toString(2));
		// }
		// else {
		// System.err.println("Error adding GeoServer layer 'ne_50m_admin_0_countries'; code = " +
		// addLayerResponse.getStatus());
		// }

		// test delete layer
		// Response deleteLayerResponse =
		// geoserverClient.deleteLayer("ne_50m_admin_0_countries");
		// if (deleteLayerResponse.getStatus() == Status.OK.getStatusCode()) {
		// System.out.println("\nGeoServer layer delete response for 'ne_50m_admin_0_countries':");
		//
		// JSONObject jsonResponse =
		// JSONObject.fromObject(deleteLayerResponse.getEntity());
		// System.out.println(jsonResponse.toString(2));
		// }
		// else {
		// System.err.println("Error deleting GeoServer layer 'ne_50m_admin_0_countries'; code = "
		// + deleteLayerResponse.getStatus());
		// }

		// test delete store
		// Response deleteStoreResponse = geoserverClient.deleteDatastore(
		// "DeleteMe",
		// "kamteststore2");
		//
		// if (deleteStoreResponse.getStatus() == Status.OK.getStatusCode() ||
		// addStoreResponse.getStatus() == Status.CREATED.getStatusCode()) {
		// System.out.println("Delete store 'kamstoretest2' from workspace 'DeleteMe' on GeoServer: OK");
		// }
		// else {
		// System.err.println("Error deleting store 'kamstoretest2' from workspace 'DeleteMe' on GeoServer; code = "
		// + deleteStoreResponse.getStatus());
		// }

		// test deleteWorkspace
		// Response deleteWorkspaceResponse =
		// geoserverClient.deleteWorkspace("DeleteMe");
		// if (deleteWorkspaceResponse.getStatus() == Status.OK.getStatusCode())
		// {
		// System.out.println("Delete workspace 'DeleteMe' from GeoServer: OK");
		// }
		// else {
		// System.err.println("Error deleting workspace 'DeleteMe' from GeoServer; code = "
		// + deleteWorkspaceResponse.getStatus());
		// }
	}
}
