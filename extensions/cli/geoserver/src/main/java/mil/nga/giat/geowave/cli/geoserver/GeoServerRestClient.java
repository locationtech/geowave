package mil.nga.giat.geowave.cli.geoserver;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.PathParam;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
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

	static private class DataAdapterInfo
	{
		String adapterId;
		Boolean isRaster;
	}

	private GeoServerConfig config;
	private WebTarget webTarget = null;

	public GeoServerRestClient(
			GeoServerConfig config ) {
		this.config = config;
	}

	/**
	 * 
	 * @return
	 */
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

	/**
	 * Convenience - add layer(s) for the given store to geoserver
	 * 
	 * @param workspaceName
	 * @param storeName
	 * @param adapterId
	 * @param defaultStyle
	 * @return
	 */
	public Response addLayer(
			String workspaceName,
			final String storeName,
			final String adapterId,
			final String defaultStyle ) {
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
							storeName,
							null,
							null,
							null);

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
				Response getFlResponse = getFeatureLayer(dataAdapterInfo.adapterId);
				if (getFlResponse.getStatus() == Status.OK.getStatusCode()) {
					logger.debug(dataAdapterInfo.adapterId + " layer already exists");
					continue;
				}

				logger.debug("Get feature layer: " + dataAdapterInfo.adapterId + " returned "
						+ getFlResponse.getStatus());

				// We have a datastore. Add the layer per the adapter ID
				Response addFlResponse = addFeatureLayer(
						workspaceName,
						dataStoreName,
						dataAdapterInfo.adapterId,
						defaultStyle);
				if (addFlResponse.getStatus() != Status.CREATED.getStatusCode()) {
					return addFlResponse;
				}
			}
		}

		// Report back to the caller the adapter IDs and the types that were
		// used to create the layers
		JSONObject jsonObj = getJsonFromAdapters(
				adapterInfoList,
				"Successfully added:");

		return Response.ok(
				jsonObj.toString(defaultIndentation)).build();
	}

	/**
	 * Get JSON object(s) from adapter list
	 * 
	 * @param adapterInfoList
	 * @param description
	 * @return JSONObject
	 */
	private JSONObject getJsonFromAdapters(
			ArrayList<DataAdapterInfo> adapterInfoList,
			String description ) {
		StringBuffer buf = new StringBuffer();

		// If we made it this far, let's just iterate through the adapter IDs
		// and build the JSON response data
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

	/**
	 * Check if workspace exists
	 * 
	 * @param workspace
	 * @return true if workspace exists, false if not
	 */
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

	/**
	 * Get list of workspaces from geoserver
	 * 
	 * @return
	 */
	public Response getWorkspaces() {
		final Response resp = getWebTarget().path(
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

	/**
	 * Add workspace to geoserver
	 * 
	 * @param workspace
	 * @return
	 */
	public Response addWorkspace(
			final String workspace ) {
		return getWebTarget().path(
				"rest/workspaces").request().post(
				Entity.entity(
						"{'workspace':{'name':'" + workspace + "'}}",
						MediaType.APPLICATION_JSON));
	}

	/**
	 * Delete workspace from geoserver
	 * 
	 * @param workspace
	 * @return
	 */
	public Response deleteWorkspace(
			final String workspace ) {
		return getWebTarget().path(
				"rest/workspaces/" + workspace).queryParam(
				"recurse",
				"true").request().delete();
	}

	/**
	 * Get the string version of a datastore JSONObject from geoserver
	 * 
	 * @param workspaceName
	 * @param datastoreName
	 * @return
	 */
	public Response getDatastore(
			final String workspaceName,
			String datastoreName ) {
		final Response resp = getWebTarget().path(
				"rest/workspaces/" + workspaceName + "/datastores/" + datastoreName + ".json").request().get();

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

	/**
	 * Get list of Datastore names from geoserver
	 * 
	 * @param workspaceName
	 * @return
	 */
	public Response getDatastores(
			String workspaceName ) {
		final Response resp = getWebTarget().path(
				"rest/workspaces/" + workspaceName + "/datastores.json").request().get();

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

	/**
	 * Add a geowave datastore to geoserver
	 * 
	 * @param workspaceName
	 * @param datastoreName
	 * @param gwStoreName
	 * @return
	 */
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
				"rest/workspaces/" + workspaceName + "/datastores").request().post(
				Entity.entity(
						dataStoreJson,
						MediaType.APPLICATION_JSON));
	}

	/**
	 * Delete a geowave datastore from geoserver
	 * 
	 * @param workspaceName
	 * @param datastoreName
	 * @return
	 */
	public Response deleteDatastore(
			String workspaceName,
			String datastoreName ) {
		return getWebTarget().path(
				"rest/workspaces/" + workspaceName + "/datastores/" + datastoreName).queryParam(
				"recurse",
				"true").request().delete();
	}

	/**
	 * Get a layer from geoserver
	 * 
	 * @param layerName
	 * @return
	 */
	public Response getFeatureLayer(
			final String layerName ) {
		final Response resp = getWebTarget().path(
				"rest/layers/" + layerName + ".json").request().get();

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
				"rest/layers.json").request().get();

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
				boolean include = !geowaveOnly && !wsFilter && !dsFilter; // no
																			// filtering
																			// of
																			// any
																			// kind

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
									logger
											.error("entry Array is null - didn't find a connectionParameters datastore object that was a JSONObject or JSONArray");
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

	/**
	 * Add feature layer to geoserver
	 * 
	 * @param workspaceName
	 * @param datastoreName
	 * @param layerName
	 * @param defaultStyle
	 * @return
	 */
	public Response addFeatureLayer(
			final String workspaceName,
			final String datastoreName,
			final String layerName,
			final String defaultStyle ) {
		if (defaultStyle != null) {
			getWebTarget().path(
					"rest/layers/" + layerName + ".json").request().put(
					Entity.entity(
							"{'layer':{'defaultStyle':{'name':'" + defaultStyle + "'}}}",
							MediaType.APPLICATION_JSON));
		}

		return getWebTarget().path(
				"rest/workspaces/" + workspaceName + "/datastores/" + datastoreName + "/featuretypes").request().post(
				Entity.entity(
						"{'featureType':{'name':'" + layerName + "'}}",
						MediaType.APPLICATION_JSON));
	}

	/**
	 * Delete a feature layer from geoserver
	 * 
	 * @param layerName
	 * @return
	 */
	public Response deleteFeatureLayer(
			final String layerName ) {
		return getWebTarget().path(
				"rest/layers/" + layerName).request().delete();
	}

	/**
	 * Change the default style of a layer
	 * 
	 * @param layerName
	 * @param styleName
	 * @return
	 */
	public Response setLayerStyle(
			final String layerName,
			final String styleName ) {

		return getWebTarget().path(
				"rest/layers/" + layerName + ".json").request().put(
				Entity.entity(
						"{'layer':{'defaultStyle':{'name':'" + styleName + "'}}}",
						MediaType.APPLICATION_JSON));
	}

	/**
	 * Get a geoserver style
	 * 
	 * @param styleName
	 * @return
	 */
	public Response getStyle(
			@PathParam("styleName")
			final String styleName ) {

		final Response resp = getWebTarget().path(
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

	/**
	 * Get a list of geoserver styles
	 * 
	 * @return
	 */
	public Response getStyles() {
		final Response resp = getWebTarget().path(
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

	/**
	 * Add a style to geoserver
	 * 
	 * @param styleName
	 * @param fileInStream
	 * @return
	 */
	public Response addStyle(
			final String styleName,
			final InputStream fileInStream ) {

		getWebTarget().path(
				"rest/styles").request().post(
				Entity.entity(
						"{'style':{'name':'" + styleName + "','filename':'" + styleName + ".sld'}}",
						MediaType.APPLICATION_JSON));

		return getWebTarget().path(
				"rest/styles/" + styleName).request().put(
				Entity.entity(
						fileInStream,
						"application/vnd.ogc.sld+xml"));
	}

	/**
	 * Delete a style from geoserver
	 * 
	 * @param styleName
	 * @return
	 */
	public Response deleteStyle(
			final String styleName ) {

		return getWebTarget().path(
				"rest/styles/" + styleName).request().delete();
	}

	/**
	 * Get coverage store from geoserver
	 * 
	 * @param workspaceName
	 * @param coverageName
	 * @return
	 */
	public Response getCoverageStore(
			final String workspaceName,
			String coverageName ) {
		final Response resp = getWebTarget().path(
				"rest/workspaces/" + workspaceName + "/coveragestores/" + coverageName + ".json").request().get();

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

	/**
	 * Get a list of coverage stores from geoserver
	 * 
	 * @param workspaceName
	 * @return
	 */
	public Response getCoverageStores(
			String workspaceName ) {
		final Response resp = getWebTarget().path(
				"rest/workspaces/" + workspaceName + "/coveragestores.json").request().get();

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

	/**
	 * Add coverage store to geoserver
	 * 
	 * @param workspaceName
	 * @param cvgStoreName
	 * @param gwStoreName
	 * @param equalizeHistogramOverride
	 * @param interpolationOverride
	 * @param scaleTo8Bit
	 * @return
	 */
	public Response addCoverageStore(
			String workspaceName,
			String cvgStoreName,
			String gwStoreName,
			Boolean equalizeHistogramOverride,
			String interpolationOverride,
			Boolean scaleTo8Bit ) {
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
				inputStoreOptions.getGeowaveNamespace());

		storeConfigMap.put(
				GeoServerConfig.GEOSERVER_CS,
				cvgStoreName);

		final String cvgStoreXml = createCoverageXml(
				storeConfigMap,
				equalizeHistogramOverride,
				interpolationOverride,
				scaleTo8Bit);

		System.out.println("Add coverage store - xml params:\n" + cvgStoreXml);

		// create a new geoserver style
		return getWebTarget().path(
				"rest/workspaces/" + workspaceName + "/coveragestores").request().post(
				Entity.entity(
						cvgStoreXml,
						MediaType.APPLICATION_XML));
	}

	/**
	 * Delete coverage store form geoserver
	 * 
	 * @param workspaceName
	 * @param cvgstoreName
	 * @return
	 */
	public Response deleteCoverageStore(
			String workspaceName,
			String cvgstoreName ) {
		return getWebTarget().path(
				"rest/workspaces/" + workspaceName + "/coveragestores/" + cvgstoreName).queryParam(
				"recurse",
				"true").request().delete();
	}

	/**
	 * Get a list of coverages (raster layers) from geoserver
	 * 
	 * @param workspaceName
	 * @param cvsstoreName
	 * @return
	 */
	public Response getCoverages(
			String workspaceName,
			String cvsstoreName ) {
		final Response resp = getWebTarget()
				.path(
						"rest/workspaces/" + workspaceName + "/coveragestores/" + cvsstoreName + "/coverages.json")
				.request()
				.get();

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

	/**
	 * Get coverage from geoserver
	 * 
	 * @param workspaceName
	 * @param cvgStoreName
	 * @param coverageName
	 * @return
	 */
	public Response getCoverage(
			final String workspaceName,
			String cvgStoreName,
			String coverageName ) {
		final Response resp = getWebTarget()
				.path(
						"rest/workspaces/" + workspaceName + "/coveragestores/" + cvgStoreName + "/coverages/"
								+ coverageName + ".json")
				.request()
				.get();

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

	/**
	 * Add coverage to geoserver
	 * 
	 * @param workspaceName
	 * @param cvgStoreName
	 * @param coverageName
	 * @return
	 */
	public Response addCoverage(
			final String workspaceName,
			final String cvgStoreName,
			final String coverageName ) {
		String jsonString = "{'coverage':" + "{'name':'" + coverageName + "'," + "'nativeCoverageName':'"
				+ coverageName + "'}}";
		logger.debug("Posting JSON: " + jsonString + " to " + workspaceName + "/" + cvgStoreName);

		return getWebTarget().path(
				"rest/workspaces/" + workspaceName + "/coveragestores/" + cvgStoreName + "/coverages").request().post(
				Entity.entity(
						jsonString,
						MediaType.APPLICATION_JSON));
	}

	/**
	 * Delete coverage from geoserver
	 * 
	 * @param workspaceName
	 * @param cvgstoreName
	 * @param coverageName
	 * @return
	 */
	public Response deleteCoverage(
			String workspaceName,
			String cvgstoreName,
			String coverageName ) {
		return getWebTarget()
				.path(
						"rest/workspaces/" + workspaceName + "/coveragestores/" + cvgstoreName + "/coverages/"
								+ coverageName)
				.queryParam(
						"recurse",
						"true")
				.request()
				.delete();
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
			Map<String, String> geowaveStoreConfig,
			Boolean equalizeHistogramOverride,
			String interpolationOverride,
			Boolean scaleTo8Bit ) {
		String coverageXml = null;

		String workspace = geowaveStoreConfig.get(GeoServerConfig.GEOSERVER_WORKSPACE);
		String cvgstoreName = geowaveStoreConfig.get(GeoServerConfig.GEOSERVER_CS);

		try {
			// create the post XML
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

			factory.setFeature(
					"http://xml.org/sax/features/external-general-entities",
					false);
			factory.setFeature(
					"http://xml.org/sax/features/external-parameter-entities",
					false);

			Document xmlDoc = factory.newDocumentBuilder().newDocument();

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
			String storeConfigUrl = createParamUrl(
					geowaveStoreConfig,
					equalizeHistogramOverride,
					interpolationOverride,
					scaleTo8Bit);

			Element urlEl = xmlDoc.createElement("url");
			urlEl.appendChild(xmlDoc.createTextNode(storeConfigUrl));
			rootEl.appendChild(urlEl);

			// use a transformer to create the xml string for the rest call
			TransformerFactory xformerFactory = TransformerFactory.newInstance();

			xformerFactory.setFeature(
					"http://xml.org/sax/features/external-general-entities",
					false);
			xformerFactory.setFeature(
					"http://xml.org/sax/features/external-parameter-entities",
					false);
			xformerFactory.setAttribute(
					XMLConstants.ACCESS_EXTERNAL_DTD,
					"");
			xformerFactory.setAttribute(
					XMLConstants.ACCESS_EXTERNAL_STYLESHEET,
					"");

			Transformer xformer = xformerFactory.newTransformer();

			DOMSource source = new DOMSource(
					xmlDoc);
			StreamResult result = new StreamResult(
					new StringWriter());

			xformer.transform(
					source,
					result);

			coverageXml = result.getWriter().toString();
		}
		catch (TransformerException e) {
			e.printStackTrace();
		}
		catch (ParserConfigurationException e) {
			e.printStackTrace();
		}

		return coverageXml;
	}

	private String createParamUrl(
			Map<String, String> geowaveStoreConfig,
			Boolean equalizeHistogramOverride,
			String interpolationOverride,
			Boolean scaleTo8Bit ) {
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
		if (equalizeHistogramOverride != null) {
			buf.append(";equalizeHistogramOverride=");
			buf.append(equalizeHistogramOverride);
		}
		if (interpolationOverride != null) {
			buf.append(";interpolationOverride=");
			buf.append(interpolationOverride);
		}
		if (scaleTo8Bit != null) {
			buf.append(";scaleTo8Bit=");
			buf.append(scaleTo8Bit);
		}

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
}
