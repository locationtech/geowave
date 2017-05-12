package mil.nga.giat.geowave.cli.geoserver;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.glassfish.jersey.SslConfigurator;

import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.beust.jcommander.ParameterException;

import static mil.nga.giat.geowave.cli.geoserver.constants.GeoServerConstants.*;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterDataAdapter;
import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.cli.geoserver.GeoServerAddLayerCommand.AddOption;
import mil.nga.giat.geowave.core.cli.operations.config.security.crypto.BaseEncryption;
import mil.nga.giat.geowave.core.cli.operations.config.security.utils.SecurityUtils;
import mil.nga.giat.geowave.core.cli.utils.FileUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class GeoServerRestClient
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoServerRestClient.class);
	private final static int defaultIndentation = 2;

	static private class DataAdapterInfo
	{
		String adapterId;
		Boolean isRaster;
	}

	private final GeoServerConfig config;
	private WebTarget webTarget = null;

	public GeoServerRestClient(
			final GeoServerConfig config ) {
		this.config = config;
		org.apache.log4j.Logger.getRootLogger().setLevel(
				org.apache.log4j.Level.DEBUG);
	}

	public GeoServerRestClient(
			final GeoServerConfig config,
			WebTarget webTarget ) {
		this.config = config;
		this.webTarget = webTarget;
		org.apache.log4j.Logger.getRootLogger().setLevel(
				org.apache.log4j.Level.DEBUG);
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
			String url = getConfig().getUrl();
			if (url != null) {
				url = url.trim();
				Client client = null;
				if (url.toLowerCase().startsWith(
						"http://")) {
					client = ClientBuilder.newClient();
				}
				else if (url.toLowerCase().startsWith(
						"https://")) {
					SslConfigurator sslConfig = SslConfigurator.newInstance();
					if (getConfig().getGsConfigProperties() != null) {
						loadSSLConfigurations(
								sslConfig,
								getConfig().getGsConfigProperties());
					}
					SSLContext sslContext = sslConfig.createSSLContext();

					HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
					client = ClientBuilder.newBuilder().sslContext(
							sslContext).build();
				}
				if (client != null) {
					client.register(HttpAuthenticationFeature.basic(
							getConfig().getUser(),
							getConfig().getPass()));
					webTarget = client.target(url);
				}
			}
		}

		return webTarget;
	}

	/**
	 * If connecting to GeoServer over HTTPS (HTTP+SSL), we need to specify the
	 * SSL properties. The SSL properties are set from a properties file. Since
	 * the properties will be different, based on one GeoServer deployment
	 * compared to another, this gives the ability to specify any of the fields.
	 * If the key is in provided properties file, it will be loaded into the
	 * GeoServer SSL configuration.
	 * 
	 * @param sslConfig
	 *            SSL Configuration object for use when instantiating an HTTPS
	 *            connection to GeoServer
	 * @param gsConfigProperties
	 *            Properties object with applicable GeoServer connection
	 *            properties
	 */
	private void loadSSLConfigurations(
			SslConfigurator sslConfig,
			Properties gsConfigProperties ) {
		if (gsConfigProperties != null && sslConfig != null) {
			// default to TLS for geoserver ssl security protocol
			sslConfig.securityProtocol(getPropertyValue(
					gsConfigProperties,
					GEOSERVER_SSL_SECURITY_PROTOCOL,
					"TLS"));

			// check truststore property settings
			if (gsConfigProperties.containsKey(GEOSERVER_SSL_TRUSTSTORE_FILE)) {
				// resolve file path - either relative or absolute - then get
				// the canonical path
				File trustStoreFile = new File(
						getPropertyValue(
								gsConfigProperties,
								GEOSERVER_SSL_TRUSTSTORE_FILE));
				if (trustStoreFile != null) {
					try {
						sslConfig.trustStoreFile(trustStoreFile.getCanonicalPath());
					}
					catch (IOException e) {
						LOGGER.error(
								"An error occurred loading the truststore at the specified path [" + getPropertyValue(
										gsConfigProperties,
										GEOSERVER_SSL_TRUSTSTORE_FILE) + "]:" + e.getLocalizedMessage(),
								e);
					}
				}
			}
			if (gsConfigProperties.containsKey(GEOSERVER_SSL_TRUSTSTORE_PASS)) {
				sslConfig.trustStorePassword(getPropertyValue(
						gsConfigProperties,
						GEOSERVER_SSL_TRUSTSTORE_PASS));
			}
			if (gsConfigProperties.containsKey(GEOSERVER_SSL_TRUSTSTORE_TYPE)) {
				sslConfig.trustStoreType(getPropertyValue(
						gsConfigProperties,
						GEOSERVER_SSL_TRUSTSTORE_TYPE));
			}
			if (gsConfigProperties.containsKey(GEOSERVER_SSL_TRUSTSTORE_PROVIDER)) {
				sslConfig.trustStoreProvider(getPropertyValue(
						gsConfigProperties,
						GEOSERVER_SSL_TRUSTSTORE_PROVIDER));
			}
			if (gsConfigProperties.containsKey(GEOSERVER_SSL_TRUSTMGR_ALG)) {
				sslConfig.trustManagerFactoryAlgorithm(getPropertyValue(
						gsConfigProperties,
						GEOSERVER_SSL_TRUSTMGR_ALG));
			}
			if (gsConfigProperties.containsKey(GEOSERVER_SSL_TRUSTMGR_PROVIDER)) {
				sslConfig.trustManagerFactoryProvider(getPropertyValue(
						gsConfigProperties,
						GEOSERVER_SSL_TRUSTMGR_PROVIDER));
			}

			// check keystore property settings
			if (gsConfigProperties.containsKey(GEOSERVER_SSL_KEYSTORE_FILE)) {
				// resolve file path - either relative or absolute - then get
				// the canonical path
				File keyStoreFile = new File(
						FileUtils.formatFilePath(getPropertyValue(
								gsConfigProperties,
								GEOSERVER_SSL_KEYSTORE_FILE)));
				if (keyStoreFile != null) {
					try {
						sslConfig.keyStoreFile(keyStoreFile.getCanonicalPath());
					}
					catch (IOException e) {
						LOGGER.error(
								"An error occurred loading the keystore at the specified path [" + getPropertyValue(
										gsConfigProperties,
										GEOSERVER_SSL_KEYSTORE_FILE) + "]:" + e.getLocalizedMessage(),
								e);
					}
				}
			}
			if (gsConfigProperties.containsKey(GEOSERVER_SSL_KEYSTORE_PASS)) {
				sslConfig.keyStorePassword(getPropertyValue(
						gsConfigProperties,
						GEOSERVER_SSL_KEYSTORE_PASS));
			}
			if (gsConfigProperties.containsKey(GEOSERVER_SSL_KEY_PASS)) {
				sslConfig.keyPassword(getPropertyValue(
						gsConfigProperties,
						GEOSERVER_SSL_KEY_PASS));
			}
			if (gsConfigProperties.containsKey(GEOSERVER_SSL_KEYSTORE_PROVIDER)) {
				sslConfig.keyStoreProvider(getPropertyValue(
						gsConfigProperties,
						GEOSERVER_SSL_KEYSTORE_PROVIDER));
			}
			if (gsConfigProperties.containsKey(GEOSERVER_SSL_KEYSTORE_TYPE)) {
				sslConfig.keyStoreType(getPropertyValue(
						gsConfigProperties,
						GEOSERVER_SSL_KEYSTORE_TYPE));
			}
			if (gsConfigProperties.containsKey(GEOSERVER_SSL_KEYMGR_ALG)) {
				sslConfig.keyManagerFactoryAlgorithm(getPropertyValue(
						gsConfigProperties,
						GEOSERVER_SSL_KEYMGR_ALG));
			}
			if (gsConfigProperties.containsKey(GEOSERVER_SSL_KEYMGR_PROVIDER)) {
				sslConfig.keyManagerFactoryProvider(getPropertyValue(
						gsConfigProperties,
						GEOSERVER_SSL_KEYMGR_PROVIDER));
			}
		}
	}

	private String getPropertyValue(
			final Properties configProps,
			final String configKey ) {
		return getPropertyValue(
				configProps,
				configKey,
				null);
	}

	private String getPropertyValue(
			final Properties configProps,
			final String configKey,
			final String defaultValue ) {
		String configValue = defaultValue;
		if (configProps != null) {
			configValue = configProps.getProperty(
					configKey,
					defaultValue);
			if (BaseEncryption.isProperlyWrapped(configValue)) {
				try {
					final File resourceTokenFile = SecurityUtils.getFormattedTokenKeyFileForConfig(getConfig()
							.getPropFile());
					// if password in config props is encrypted, need to decrypt
					// it
					configValue = SecurityUtils.decryptHexEncodedValue(
							configValue,
							resourceTokenFile.getCanonicalPath());
					return configValue;
				}
				catch (Exception e) {
					LOGGER.error(
							"An error occurred decrypting password: " + e.getLocalizedMessage(),
							e);
					return configValue;
				}
			}
		}
		return configValue;
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
			final String workspaceName,
			final String storeName,
			final String adapterId,
			final String defaultStyle ) {
		// retrieve the adapter info list for the store
		final ArrayList<DataAdapterInfo> adapterInfoList = getStoreAdapterInfo(
				storeName,
				adapterId);

		LOGGER.debug("Finished retrieving adapter list");

		if ((adapterInfoList.size() > 1) && (adapterId == null)) {
			LOGGER.debug("addlayer doesn't know how to deal with multiple adapters");

			final String descr = "Please use -a, or choose one of these with -id:";
			final JSONObject jsonObj = getJsonFromAdapters(
					adapterInfoList,
					descr);

			LOGGER.debug(jsonObj.toString());

			return Response.ok(
					jsonObj.toString(defaultIndentation)).build();
		}

		// verify the workspace exists
		if (!workspaceExists(workspaceName)) {
			LOGGER.debug("addlayer needs to create the " + workspaceName + " workspace");

			final Response addWsResponse = addWorkspace(workspaceName);
			if (addWsResponse.getStatus() != Status.CREATED.getStatusCode()) {
				return addWsResponse;
			}
		}

		final String cvgStoreName = storeName + GeoServerConfig.DEFAULT_CS;
		final String dataStoreName = storeName + GeoServerConfig.DEFAULT_DS;

		// iterate through data adapters
		for (final DataAdapterInfo dataAdapterInfo : adapterInfoList) {
			// handle coverage stores & coverages
			if (dataAdapterInfo.isRaster) {
				// verify coverage store exists
				final Response getCsResponse = getCoverageStore(
						workspaceName,
						cvgStoreName);
				if (getCsResponse.getStatus() == Status.NOT_FOUND.getStatusCode()) {
					final Response addCsResponse = addCoverageStore(
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
				final Response getCvResponse = getCoverage(
						workspaceName,
						cvgStoreName,
						dataAdapterInfo.adapterId);
				if (getCvResponse.getStatus() == Status.OK.getStatusCode()) {
					LOGGER.debug(dataAdapterInfo.adapterId + " layer already exists");
					continue;
				}

				// We have a coverage store. Add the layer per the adapter ID
				final Response addCvResponse = addCoverage(
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
				final Response getDsResponse = getDatastore(
						workspaceName,
						dataStoreName);
				if (getDsResponse.getStatus() == Status.NOT_FOUND.getStatusCode()) {
					final Response addDsResponse = addDatastore(
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

				LOGGER.debug("Checking for existing feature layer: " + dataAdapterInfo.adapterId);

				// See if the feature layer already exists
				final Response getFlResponse = getFeatureLayer(dataAdapterInfo.adapterId);
				if (getFlResponse.getStatus() == Status.OK.getStatusCode()) {
					LOGGER.debug(dataAdapterInfo.adapterId + " layer already exists");
					continue;
				}

				LOGGER.debug("Get feature layer: " + dataAdapterInfo.adapterId + " returned "
						+ getFlResponse.getStatus());

				// We have a datastore. Add the layer per the adapter ID
				final Response addFlResponse = addFeatureLayer(
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
		final JSONObject jsonObj = getJsonFromAdapters(
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
			final ArrayList<DataAdapterInfo> adapterInfoList,
			final String description ) {
		final StringBuffer buf = new StringBuffer();

		// If we made it this far, let's just iterate through the adapter IDs
		// and build the JSON response data
		buf.append("{'description':'" + description + "', " + "'layers':[");

		for (int i = 0; i < adapterInfoList.size(); i++) {
			final DataAdapterInfo info = adapterInfoList.get(i);

			buf.append("{'id':'" + info.adapterId + "',");
			buf.append("'type':'" + (info.isRaster ? "raster" : "vector") + "'}");

			if (i < (adapterInfoList.size() - 1)) {
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

		final Response getWsResponse = getWorkspaces();
		if (getWsResponse.getStatus() == Status.OK.getStatusCode()) {
			final JSONObject jsonResponse = JSONObject.fromObject(getWsResponse.getEntity());

			final JSONArray workspaces = jsonResponse.getJSONArray("workspaces");

			for (int i = 0; i < workspaces.size(); i++) {
				final String wsName = workspaces.getJSONObject(
						i).getString(
						"name");

				if (wsName.equals(workspace)) {
					return true;
				}
			}
		}
		else {
			LOGGER.error("Error retrieving GeoServer workspace list");
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
			final String datastoreName ) {
		final Response resp = getWebTarget().path(
				"rest/workspaces/" + workspaceName + "/datastores/" + datastoreName + ".json").request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			resp.bufferEntity();

			final JSONObject datastore = JSONObject.fromObject(resp.readEntity(String.class));

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
			final String workspaceName ) {
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
			final String workspaceName,
			String datastoreName,
			final String gwStoreName ) {
		final DataStorePluginOptions inputStoreOptions = getStorePlugin(gwStoreName);

		if ((datastoreName == null) || datastoreName.isEmpty()) {
			datastoreName = gwStoreName + GeoServerConfig.DEFAULT_DS;
		}

		final String lockMgmt = "memory";
		final String authMgmtPrvdr = "empty";
		final String authDataUrl = "";
		final String queryIndexStrategy = "Best Match";

		final String dataStoreJson = createDatastoreJson(
				inputStoreOptions.getType(),
				inputStoreOptions.getOptionsAsMap(),
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
			final String workspaceName,
			final String datastoreName ) {
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
			final JSONObject layer = JSONObject.fromObject(resp.readEntity(String.class));

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
			final String workspaceName,
			final String datastoreName,
			final boolean geowaveOnly ) {
		final boolean wsFilter = ((workspaceName != null) && !workspaceName.isEmpty());
		final boolean dsFilter = ((datastoreName != null) && !datastoreName.isEmpty());

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
				final boolean include = !geowaveOnly && !wsFilter && !dsFilter; // no
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
				if (!dsFilter || ((ds != null) && ds.equals(datastoreName))) {

					// filter on workspace?
					if (!wsFilter || ((ws != null) && ws.equals(workspaceName))) {
						final JSONObject datastore = JSONObject.fromObject(
								getDatastore(
										ds,
										ws).getEntity()).getJSONObject(
								"dataStore");

						// only process GeoWave layers
						if (geowaveOnly) {
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
									LOGGER
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
			final String coverageName ) {
		final Response resp = getWebTarget().path(
				"rest/workspaces/" + workspaceName + "/coveragestores/" + coverageName + ".json").request().get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			resp.bufferEntity();

			final JSONObject cvgstore = JSONObject.fromObject(resp.readEntity(String.class));

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
			final String workspaceName ) {
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
			final String workspaceName,
			String cvgStoreName,
			final String gwStoreName,
			final Boolean equalizeHistogramOverride,
			final String interpolationOverride,
			final Boolean scaleTo8Bit ) {
		final DataStorePluginOptions inputStoreOptions = getStorePlugin(gwStoreName);

		if ((cvgStoreName == null) || cvgStoreName.isEmpty()) {
			cvgStoreName = gwStoreName + GeoServerConfig.DEFAULT_CS;
		}

		// Get the store's db config
		final Map<String, String> storeConfigMap = inputStoreOptions.getOptionsAsMap();

		// Add in geoserver coverage store info
		storeConfigMap.put(
				GEOSERVER_WORKSPACE,
				workspaceName);

		storeConfigMap.put(
				"gwNamespace",
				inputStoreOptions.getGeowaveNamespace());

		storeConfigMap.put(
				GEOSERVER_CS,
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
			final String workspaceName,
			final String cvgstoreName ) {
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
			final String workspaceName,
			final String cvsstoreName ) {
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
			final String cvgStoreName,
			final String coverageName ) {
		final Response resp = getWebTarget()
				.path(
						"rest/workspaces/" + workspaceName + "/coveragestores/" + cvgStoreName + "/coverages/"
								+ coverageName + ".json")
				.request()
				.get();

		if (resp.getStatus() == Status.OK.getStatusCode()) {
			resp.bufferEntity();

			final JSONObject cvg = JSONObject.fromObject(resp.readEntity(String.class));

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
		final String jsonString = "{'coverage':" + "{'name':'" + coverageName + "'," + "'nativeCoverageName':'"
				+ coverageName + "'}}";
		LOGGER.debug("Posting JSON: " + jsonString + " to " + workspaceName + "/" + cvgStoreName);

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
			final String workspaceName,
			final String cvgstoreName,
			final String coverageName ) {
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
			final Map<String, String> geowaveStoreConfig,
			final Boolean equalizeHistogramOverride,
			final String interpolationOverride,
			final Boolean scaleTo8Bit ) {
		String coverageXml = null;

		final String workspace = geowaveStoreConfig.get(GEOSERVER_WORKSPACE);
		final String cvgstoreName = geowaveStoreConfig.get(GEOSERVER_CS);

		StreamResult result = null;
		try {
			// create the post XML
			final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

			factory.setFeature(
					"http://xml.org/sax/features/external-general-entities",
					false);
			factory.setFeature(
					"http://xml.org/sax/features/external-parameter-entities",
					false);

			final Document xmlDoc = factory.newDocumentBuilder().newDocument();

			final Element rootEl = xmlDoc.createElement("coverageStore");
			xmlDoc.appendChild(rootEl);

			final Element nameEl = xmlDoc.createElement("name");
			nameEl.appendChild(xmlDoc.createTextNode(cvgstoreName));
			rootEl.appendChild(nameEl);

			final Element wsEl = xmlDoc.createElement("workspace");
			wsEl.appendChild(xmlDoc.createTextNode(workspace));
			rootEl.appendChild(wsEl);

			final Element typeEl = xmlDoc.createElement("type");
			typeEl.appendChild(xmlDoc.createTextNode("GeoWaveRasterFormat"));
			rootEl.appendChild(typeEl);

			final Element enabledEl = xmlDoc.createElement("enabled");
			enabledEl.appendChild(xmlDoc.createTextNode("true"));
			rootEl.appendChild(enabledEl);

			final Element configEl = xmlDoc.createElement("configure");
			configEl.appendChild(xmlDoc.createTextNode("all"));
			rootEl.appendChild(configEl);

			// Method using custom URL & handler:
			final String storeConfigUrl = createParamUrl(
					geowaveStoreConfig,
					equalizeHistogramOverride,
					interpolationOverride,
					scaleTo8Bit);

			final Element urlEl = xmlDoc.createElement("url");
			urlEl.appendChild(xmlDoc.createTextNode(storeConfigUrl));
			rootEl.appendChild(urlEl);

			// use a transformer to create the xml string for the rest call
			final TransformerFactory xformerFactory = TransformerFactory.newInstance();

			// HP Fortify "XML External Entity Injection" false positive
			// The following modifications to xformerFactory are the
			// fortify-recommended procedure to secure a TransformerFactory
			// but the report still flags this instance
			xformerFactory.setFeature(
					XMLConstants.FEATURE_SECURE_PROCESSING,
					true);

			final Transformer xformer = xformerFactory.newTransformer();

			final DOMSource source = new DOMSource(
					xmlDoc);
			result = new StreamResult(
					new StringWriter());

			xformer.transform(
					source,
					result);

			coverageXml = result.getWriter().toString();
		}
		catch (final TransformerException e) {
			LOGGER.error(
					"Unable to create transformer",
					e);
		}
		catch (final ParserConfigurationException e1) {
			LOGGER.error(
					"Unable to create DocumentBuilderFactory",
					e1);
		}
		finally {
			if ((result != null) && (result.getWriter() != null)) {
				try {
					result.getWriter().close();
				}
				catch (final IOException e) {
					LOGGER.error(
							e.getLocalizedMessage(),
							e);
				}
			}
		}

		return coverageXml;
	}

	private String createParamUrl(
			final Map<String, String> geowaveStoreConfig,
			final Boolean equalizeHistogramOverride,
			final String interpolationOverride,
			final Boolean scaleTo8Bit ) {
		// Retrieve store config
		final String user = geowaveStoreConfig.get("user");
		final String pass = geowaveStoreConfig.get("password");
		final String zookeeper = geowaveStoreConfig.get("zookeeper");
		final String instance = geowaveStoreConfig.get("instance");
		final String gwNamespace = geowaveStoreConfig.get("gwNamespace");

		// Create the custom geowave url w/ params
		final StringBuffer buf = new StringBuffer();
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
			final String storeName ) {
		final StoreLoader inputStoreLoader = new StoreLoader(
				storeName);
		if (!inputStoreLoader.loadFromConfig(config.getPropFile())) {
			throw new ParameterException(
					"Cannot find store name: " + inputStoreLoader.getStoreName());
		}

		return inputStoreLoader.getDataStorePlugin();
	}

	public ArrayList<String> getStoreAdapters(
			final String storeName,
			final String adapterId ) {
		final ArrayList<DataAdapterInfo> adapterInfoList = getStoreAdapterInfo(
				storeName,
				adapterId);

		final ArrayList<String> adapterIdList = new ArrayList<String>();

		for (final DataAdapterInfo info : adapterInfoList) {
			adapterIdList.add(info.adapterId);
		}

		return adapterIdList;
	}

	private ArrayList<DataAdapterInfo> getStoreAdapterInfo(
			final String storeName,
			final String adapterId ) {
		final DataStorePluginOptions dsPlugin = getStorePlugin(storeName);

		final AdapterStore adapterStore = dsPlugin.createAdapterStore();

		final ArrayList<DataAdapterInfo> adapterInfoList = new ArrayList<DataAdapterInfo>();

		LOGGER.debug("Adapter list for " + storeName + " with adapterId = " + adapterId + ": ");

		try (final CloseableIterator<DataAdapter<?>> it = adapterStore.getAdapters()) {
			while (it.hasNext()) {
				final DataAdapter<?> adapter = it.next();

				final DataAdapterInfo info = getAdapterInfo(
						adapterId,
						adapter);

				if (info != null) {
					adapterInfoList.add(info);
					LOGGER.debug("> '" + info.adapterId + "' adapter passed filter");
				}
			}

		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to close adapter iterator while looking up coverage names",
					e);
		}

		LOGGER.debug("getStoreAdapterInfo(" + storeName + ") got " + adapterInfoList.size() + " ids");

		return adapterInfoList;
	}

	private DataAdapterInfo getAdapterInfo(
			final String adapterId,
			final DataAdapter adapter ) {
		LOGGER.debug("getAdapterInfo for id = " + adapterId);

		final DataAdapterInfo info = new DataAdapterInfo();
		info.adapterId = adapter.getAdapterId().getString();
		info.isRaster = false;

		if (adapter instanceof RasterDataAdapter) {
			info.isRaster = true;
		}

		LOGGER.debug("> Adapter ID: " + info.adapterId);
		LOGGER.debug("> Adapter Type: " + adapter.getClass().getSimpleName());

		if ((adapterId == null) || adapterId.equals(AddOption.ALL.name())) {
			LOGGER.debug("id is null or all");
			return info;
		}

		if (adapterId.equals(adapter.getAdapterId().getString())) {
			LOGGER.debug("id matches adapter id");
			return info;
		}

		if (adapterId.equals(AddOption.RASTER.name()) && (adapter instanceof RasterDataAdapter)) {
			LOGGER.debug("id is all-raster and adapter is raster type");
			return info;
		}

		if (adapterId.equals(AddOption.VECTOR.name()) && (adapter instanceof GeotoolsFeatureDataAdapter)) {
			LOGGER.debug("id is all-vector and adapter is vector type");
			return info;
		}

		LOGGER.debug("No match!");

		return null;
	}
}
