package mil.nga.giat.geowave.adapter.raster.plugin;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import javax.media.jai.Interpolation;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import mil.nga.giat.geowave.adapter.auth.AuthorizationFactorySPI;
import mil.nga.giat.geowave.adapter.auth.EmptyAuthorizationFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.index.IndexStore;

public class GeoWaveRasterConfig
{
	static private final Logger LOGGER = LoggerFactory.getLogger(GeoWaveRasterConfig.class);
	static private final Map<String, GeoWaveRasterConfig> CONFIG_CACHE = new Hashtable<String, GeoWaveRasterConfig>();

	protected static enum ConfigParameter {
		// the following two are optional parameters that will override the
		// behavior of tile mosaicing that is already set within each adapter
		INTERPOLATION(
				"interpolationOverride"),
		SCALE_TO_8BIT(
				"scaleTo8Bit"),
		EQUALIZE_HISTOGRAM(
				"equalizeHistogramOverride"),
		AUTHORIZATION_PROVIDER(
				"authorizationProvider"),
		AUTHORIZATION_URL(
				"authorizationUrl");
		private String configName;

		private ConfigParameter(
				final String configName ) {
			this.configName = configName;
		}

		public String getConfigName() {
			return configName;
		}
	}

	private Map<String, String> storeConfigObj;
	private StoreFactoryFamilySpi factoryFamily;
	private DataStore dataStore;
	private IndexStore indexStore;
	private AdapterStore adapterStore;
	private DataStatisticsStore dataStatisticsStore;
	private AdapterIndexMappingStore adapterIndexMappingStore;
	private AuthorizationFactorySPI authorizationFactory;
	private URL authorizationURL;

	private Boolean equalizeHistogramOverride = null;

	private Boolean scaleTo8Bit = null;

	private Integer interpolationOverride = null;

	protected GeoWaveRasterConfig() {}

	public static GeoWaveRasterConfig createConfig(
			final Map<String, String> dataStoreConfig,
			final String geowaveNamespace ) {
		return createConfig(
				dataStoreConfig,
				geowaveNamespace,
				null,
				null,
				null,
				null,
				null);
	}

	public static GeoWaveRasterConfig createConfig(
			final Map<String, String> dataStoreConfig,
			final String geowaveNamespace,
			final Boolean equalizeHistogramOverride,
			final Boolean scaleTo8Bit,
			final Integer interpolationOverride,
			final String authorizationProvider,
			final URL authorizationURL ) {
		final GeoWaveRasterConfig result = new GeoWaveRasterConfig();
		result.equalizeHistogramOverride = equalizeHistogramOverride;
		result.interpolationOverride = interpolationOverride;
		result.scaleTo8Bit = scaleTo8Bit;
		synchronized (result) {
			result.storeConfigObj = dataStoreConfig;
			result.factoryFamily = GeoWaveStoreFinder.findStoreFamily(result.storeConfigObj);
		}
		result.authorizationFactory = getAuthorizationFactory(authorizationProvider);
		result.authorizationURL = authorizationURL;
		return result;
	}

	public static AuthorizationFactorySPI getAuthorizationFactory(
			final String authProviderName ) {
		if (authProviderName != null) {
			final Iterator<AuthorizationFactorySPI> authIt = getAuthorizationFactoryList();
			while (authIt.hasNext()) {
				AuthorizationFactorySPI authFactory = authIt.next();
				if (authProviderName.equals(authFactory.toString())) return authFactory;
			}
		}
		return new EmptyAuthorizationFactory();
	}

	private static Iterator<AuthorizationFactorySPI> getAuthorizationFactoryList() {
		final ServiceLoader<AuthorizationFactorySPI> ldr = ServiceLoader.load(AuthorizationFactorySPI.class);
		return ldr.iterator();
	}

	public static URL getAuthorizationURL(
			final String authorizationURL ) {
		if (authorizationURL != null) {
			try {
				return new URL(
						authorizationURL.toString());
			}
			catch (MalformedURLException e) {
				LOGGER.warn(
						"Accumulo Plugin: malformed Authorization Service URL " + authorizationURL,
						e);
			}
		}
		return null;
	}

	public static GeoWaveRasterConfig readFromConfigParams(
			final String configParams )
			throws NullPointerException {
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
			throws IOException,
			ParserConfigurationException,
			SAXException {
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

		dbf.setFeature(
				XMLConstants.FEATURE_SECURE_PROCESSING,
				true);

		final DocumentBuilder db = dbf.newDocumentBuilder();

		// db.setEntityResolver(new ConfigEntityResolver(xmlURL));
		final Document dom = db.parse(input);
		in.close();

		final NodeList children = dom.getChildNodes().item(
				0).getChildNodes();
		final Map<String, String> configParams = new HashMap<String, String>();
		for (int i = 0; i < children.getLength(); i++) {
			final Node child = children.item(i);
			configParams.put(
					child.getNodeName(),
					child.getTextContent());
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
			result.storeConfigObj = storeParams;
			result.factoryFamily = GeoWaveStoreFinder.findStoreFamily(result.storeConfigObj);
		}
		final String equalizeHistogram = params.get(ConfigParameter.EQUALIZE_HISTOGRAM.getConfigName());
		if (equalizeHistogram != null) {
			if (equalizeHistogram.trim().toLowerCase(
					Locale.ENGLISH).equals(
					"true")) {
				result.equalizeHistogramOverride = true;
			}
			else {
				result.equalizeHistogramOverride = false;
			}
		}
		final String scaleTo8Bit = params.get(ConfigParameter.SCALE_TO_8BIT.getConfigName());
		if (scaleTo8Bit != null) {
			if (scaleTo8Bit.trim().toLowerCase(
					Locale.ENGLISH).equals(
					"true")) {
				result.scaleTo8Bit = true;
			}
			else {
				result.scaleTo8Bit = false;
			}
		}
		if (params.containsKey(ConfigParameter.INTERPOLATION.getConfigName())) {
			result.interpolationOverride = Integer.parseInt(params.get(ConfigParameter.INTERPOLATION.getConfigName()));
		}

		result.authorizationFactory = getAuthorizationFactory(params.get(ConfigParameter.AUTHORIZATION_PROVIDER
				.getConfigName()));

		result.authorizationURL = getAuthorizationURL(params.get(ConfigParameter.AUTHORIZATION_URL.getConfigName()));
	}

	protected AuthorizationFactorySPI getAuthorizationFactory() {
		return authorizationFactory;
	}

	protected URL getAuthorizationURL() {
		return authorizationURL;
	}

	public synchronized DataStore getDataStore() {
		if (dataStore == null) {
			dataStore = factoryFamily.getDataStoreFactory().createStore(
					ConfigUtils.populateOptionsFromList(
							factoryFamily.getDataStoreFactory().createOptionsInstance(),
							storeConfigObj));
		}
		return dataStore;
	}

	public synchronized AdapterStore getAdapterStore() {
		if (adapterStore == null) {
			adapterStore = factoryFamily.getAdapterStoreFactory().createStore(
					ConfigUtils.populateOptionsFromList(
							factoryFamily.getAdapterStoreFactory().createOptionsInstance(),
							storeConfigObj));
		}
		return adapterStore;
	}

	public synchronized IndexStore getIndexStore() {
		if (indexStore == null) {
			indexStore = factoryFamily.getIndexStoreFactory().createStore(
					ConfigUtils.populateOptionsFromList(
							factoryFamily.getIndexStoreFactory().createOptionsInstance(),
							storeConfigObj));
		}
		return indexStore;
	}

	public synchronized DataStatisticsStore getDataStatisticsStore() {
		if (dataStatisticsStore == null) {
			dataStatisticsStore = factoryFamily.getDataStatisticsStoreFactory().createStore(
					ConfigUtils.populateOptionsFromList(
							factoryFamily.getDataStatisticsStoreFactory().createOptionsInstance(),
							storeConfigObj));
		}
		return dataStatisticsStore;
	}

	public synchronized AdapterIndexMappingStore getAdapterIndexMappingStore() {
		if (adapterIndexMappingStore == null) {
			adapterIndexMappingStore = factoryFamily.getAdapterIndexMappingStoreFactory().createStore(
					ConfigUtils.populateOptionsFromList(
							factoryFamily.getDataStatisticsStoreFactory().createOptionsInstance(),
							storeConfigObj));
		}
		return adapterIndexMappingStore;
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

	public boolean isScaleTo8BitSet() {
		return (scaleTo8Bit != null);
	}

	public boolean isScaleTo8Bit() {
		if (!isScaleTo8BitSet()) {
			throw new IllegalStateException(
					"Scale To 8-bit is not set for this config");
		}
		return scaleTo8Bit;
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
