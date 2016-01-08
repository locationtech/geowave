package mil.nga.giat.geowave.adapter.raster.plugin;

import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.media.jai.Interpolation;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

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

	private String xmlUrl;
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

	public static GeoWaveRasterConfig readFrom(
			final URL xmlURL )
			throws Exception {
		GeoWaveRasterConfig result = CONFIG_CACHE.get(xmlURL.toString());

		if (result != null) {
			return result;
		}

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

		result = new GeoWaveRasterConfig();

		result.xmlUrl = xmlURL.toString();
		final NodeList children = dom.getChildNodes();
		final Map<String, String> storeConfig = new HashMap<String, String>();
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
				storeConfig.put(
						child.getNodeName(),
						child.getNodeValue());
			}
		}
		// findbugs complaint requires this synchronization
		synchronized (result) {
			result.geowaveNamespace = readValueString(
					dom,
					ConfigParameter.NAMESPACE.getConfigName());
			result.storeConfigObj = ConfigUtils.valuesFromStrings(storeConfig);
			result.dataStoreFactory = GeoWaveStoreFinder.findDataStoreFactory(result.storeConfigObj);
			result.indexStoreFactory = GeoWaveStoreFinder.findIndexStoreFactory(result.storeConfigObj);
			result.adapterStoreFactory = GeoWaveStoreFinder.findAdapterStoreFactory(result.storeConfigObj);
			result.dataStatisticsStoreFactory = GeoWaveStoreFinder.findDataStatisticsStoreFactory(result.storeConfigObj);
		}
		final String equalizeHistogram = readValueString(
				dom,
				ConfigParameter.EQUALIZE_HISTOGRAM.getConfigName());
		if (equalizeHistogram != null) {
			if (equalizeHistogram.trim().toLowerCase().equals(
					"true")) {
				result.equalizeHistogramOverride = true;
			}
			else {
				result.equalizeHistogramOverride = false;
			}
		}
		result.interpolationOverride = readValueInteger(
				dom,
				ConfigParameter.INTERPOLATION.getConfigName());
		CONFIG_CACHE.put(
				xmlURL.toString(),
				result);

		return result;
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

	public String getXmlUrl() {
		return xmlUrl;
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

	static private String readValueString(
			final Document dom,
			final String elemName ) {
		final Node n = getNodeByName(
				dom,
				elemName);

		if (n == null) {
			return null;
		}

		return n.getTextContent();
	}

	static private Integer readValueInteger(
			final Document dom,
			final String elemName ) {
		final Node n = getNodeByName(
				dom,
				elemName);

		if (n == null) {
			return null;
		}

		return Integer.valueOf(n.getTextContent());
	}

	static private Node getNodeByName(
			final Document dom,
			final String elemName ) {
		final NodeList list = dom.getElementsByTagName(elemName);
		final Node n = list.item(0);

		return n;
	}
}
