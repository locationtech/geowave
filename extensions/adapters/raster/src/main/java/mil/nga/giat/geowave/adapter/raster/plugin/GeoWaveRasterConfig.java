package mil.nga.giat.geowave.adapter.raster.plugin;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.media.jai.Interpolation;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreFactorySpi;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStoreFactorySpi;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStoreFactorySpi;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.IndexStoreFactorySpi;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class GeoWaveRasterConfig
{

	static private final Map<String, GeoWaveRasterConfig> CONFIG_CACHE = new Hashtable<String, GeoWaveRasterConfig>();

	protected static enum ConfigParameter {
		NAMESPACE(
				"namespace"),
		// the following two are optional parameters that will override the
		// behavior of tile mosaicing that is already set within each adapter
		INTERPOLATION(
				"interpolationOverride"),
		EQUALIZE_HISTOGRAM(
				"equalizeHistogramOverride");
		private String configName;

		private ConfigParameter(
				final String configName ) {
			this.configName = configName;
		}

		public String getConfigName() {
			return configName;
		}
	}

	private String geowaveNamespace;
	private Map<String, Object> storeConfigObj;
	private DataStoreFactorySpi dataStoreFactory;
	private IndexStoreFactorySpi indexStoreFactory;
	private AdapterStoreFactorySpi adapterStoreFactory;
	private DataStatisticsStoreFactorySpi dataStatisticsStoreFactory;
	private DataStore dataStore;
	private IndexStore indexStore;
	private AdapterStore adapterStore;
	private DataStatisticsStore dataStatisticsStore;

	private Boolean equalizeHistogramOverride = null;

	private Integer interpolationOverride = null;

	protected GeoWaveRasterConfig() {}

	public static GeoWaveRasterConfig createConfig(
			final Map<String, String> dataStoreConfig,
			final String geowaveNamespace ) {
		return createConfig(
				dataStoreConfig,
				geowaveNamespace,
				null,
				null);
	}

	public static GeoWaveRasterConfig createConfig(
			final Map<String, String> dataStoreConfig,
			final String geowaveNamespace,
			final Boolean equalizeHistogramOverride,
			final Integer interpolationOverride ) {
		final GeoWaveRasterConfig result = new GeoWaveRasterConfig();
		result.equalizeHistogramOverride = equalizeHistogramOverride;
		result.interpolationOverride = interpolationOverride;
		synchronized (result) {
			result.geowaveNamespace = geowaveNamespace;
			result.storeConfigObj = ConfigUtils.valuesFromStrings(dataStoreConfig);
			result.dataStoreFactory = GeoWaveStoreFinder.findDataStoreFactory(result.storeConfigObj);
			result.indexStoreFactory = GeoWaveStoreFinder.findIndexStoreFactory(result.storeConfigObj);
			result.adapterStoreFactory = GeoWaveStoreFinder.findAdapterStoreFactory(result.storeConfigObj);
			result.dataStatisticsStoreFactory = GeoWaveStoreFinder.findDataStatisticsStoreFactory(result.storeConfigObj);
		}
		return result;
	}

	public static GeoWaveRasterConfig readFromConfigParams(
			final String configParams )
			throws Exception {
		GeoWaveRasterConfig result = CONFIG_CACHE.get(configParams);

		if (result != null) {
			return result;
		}
		result = new GeoWaveRasterConfig();
		CONFIG_CACHE.put(
				configParams,
				result);
		final Map<String, String> params = StringUtils.parseParams(configParams);

		parseParamsIntoRasterConfig(
				result,
				params);

		return result;
	}

	public static GeoWaveRasterConfig readFromURL(
			final URL xmlURL )
			throws Exception {
		GeoWaveRasterConfig result = CONFIG_CACHE.get(xmlURL.toString());

		if (result != null) {
			return result;
		}

		result = new GeoWaveRasterConfig();

		CONFIG_CACHE.put(
				xmlURL.toString(),
				result);

		final Map<String, String> params = getParamsFromURL(xmlURL);
		parseParamsIntoRasterConfig(
				result,
				params);

		return result;
	}

	private static Map<String, String> getParamsFromURL(
			final URL xmlURL )
			throws IOException,
			ParserConfigurationException,
			SAXException {
		final InputStream in = xmlURL.openStream();
		final InputSource input = new InputSource(
				xmlURL.toString());

		final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		dbf.setIgnoringElementContentWhitespace(true);
		dbf.setIgnoringComments(true);

		final DocumentBuilder db = dbf.newDocumentBuilder();

		// db.setEntityResolver(new ConfigEntityResolver(xmlURL));
		final Document dom = db.parse(input);
		in.close();

		final NodeList children = dom.getChildNodes();
		final Map<String, String> configParams = new HashMap<String, String>();
		for (int i = 0; i < children.getLength(); i++) {
			final Node child = children.item(i);
			boolean isConfigParameter = false;
			for (final ConfigParameter p : ConfigParameter.values()) {
				if (child.getNodeName().equalsIgnoreCase(
						p.getConfigName())) {
					isConfigParameter = true;
					break;
				}
			}
			if (!isConfigParameter) {
				configParams.put(
						child.getNodeName(),
						child.getNodeValue());
			}
		}
		return configParams;
	}

	private static void parseParamsIntoRasterConfig(
			final GeoWaveRasterConfig result,
			final Map<String, String> params ) {
		final Map<String, String> storeParams = new HashMap<String, String>(
				params);
		// isolate just the dynamic store params
		for (final ConfigParameter param : ConfigParameter.values()) {
			storeParams.remove(param.getConfigName());
		}
		// findbugs complaint requires this synchronization
		synchronized (result) {
			result.geowaveNamespace = params.get(ConfigParameter.NAMESPACE.getConfigName());
			result.storeConfigObj = ConfigUtils.valuesFromStrings(storeParams);
			result.dataStoreFactory = GeoWaveStoreFinder.findDataStoreFactory(result.storeConfigObj);
			result.indexStoreFactory = GeoWaveStoreFinder.findIndexStoreFactory(result.storeConfigObj);
			result.adapterStoreFactory = GeoWaveStoreFinder.findAdapterStoreFactory(result.storeConfigObj);
			result.dataStatisticsStoreFactory = GeoWaveStoreFinder.findDataStatisticsStoreFactory(result.storeConfigObj);
		}
		final String equalizeHistogram = params.get(ConfigParameter.EQUALIZE_HISTOGRAM.getConfigName());
		if (equalizeHistogram != null) {
			if (equalizeHistogram.trim().toLowerCase().equals(
					"true")) {
				result.equalizeHistogramOverride = true;
			}
			else {
				result.equalizeHistogramOverride = false;
			}
		}
		if (params.containsKey(ConfigParameter.INTERPOLATION.getConfigName())) {
			result.interpolationOverride = Integer.parseInt(params.get(ConfigParameter.INTERPOLATION.getConfigName()));
		}
	}

	public synchronized DataStore getDataStore() {
		if (dataStore == null) {
			dataStore = dataStoreFactory.createStore(
					storeConfigObj,
					geowaveNamespace);
		}
		return dataStore;
	}

	public synchronized AdapterStore getAdapterStore() {
		if (adapterStore == null) {
			adapterStore = adapterStoreFactory.createStore(
					storeConfigObj,
					geowaveNamespace);
		}
		return adapterStore;
	}

	public synchronized IndexStore getIndexStore() {
		if (indexStore == null) {
			indexStore = indexStoreFactory.createStore(
					storeConfigObj,
					geowaveNamespace);
		}
		return indexStore;
	}

	public synchronized DataStatisticsStore getDataStatisticsStore() {
		if (dataStatisticsStore == null) {
			dataStatisticsStore = dataStatisticsStoreFactory.createStore(
					storeConfigObj,
					geowaveNamespace);
		}
		return dataStatisticsStore;
	}

	public boolean isInterpolationOverrideSet() {
		return (interpolationOverride != null);
	}

	public Interpolation getInterpolationOverride() {
		if (!isInterpolationOverrideSet()) {
			throw new IllegalStateException(
					"Interpolation Override is not set for this config");
		}

		return Interpolation.getInstance(interpolationOverride);
	}

	public boolean isEqualizeHistogramOverrideSet() {
		return (equalizeHistogramOverride != null);
	}

	public boolean isEqualizeHistogramOverride() {
		if (!isEqualizeHistogramOverrideSet()) {
			throw new IllegalStateException(
					"Equalize Histogram is not set for this config");
		}
		return equalizeHistogramOverride;
	}
}
