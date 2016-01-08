package mil.nga.giat.geowave.adapter.vector.plugin;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;
import org.geotools.data.DataAccessFactory.Param;
import org.geotools.data.Parameter;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import mil.nga.giat.geowave.adapter.vector.auth.AuthorizationFactorySPI;
import mil.nga.giat.geowave.adapter.vector.auth.EmptyAuthorizationFactory;
import mil.nga.giat.geowave.adapter.vector.index.ChooseBestMatchIndexQueryStrategy;
import mil.nga.giat.geowave.adapter.vector.index.ChooseHeuristicMatchIndexQueryStrategy;
import mil.nga.giat.geowave.adapter.vector.index.IndexQueryStrategySPI;
import mil.nga.giat.geowave.adapter.vector.plugin.lock.LockingManagementFactory;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.config.AbstractConfigOption;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.config.PasswordConfigOption;
import mil.nga.giat.geowave.core.store.filter.GenericTypeResolver;
import mil.nga.giat.geowave.core.store.index.IndexStore;

/**
 * This class encapsulates the parameterized configuration that can be provided
 * per GeoWave data store within GeoTools. For GeoServer this configuration can
 * be provided within the data store definition workflow.
 * 
 */
public class GeoWavePluginConfig
{
	private final static Logger LOGGER = Logger.getLogger(GeoWavePluginConfig.class);

	public static final String GEOWAVE_NAMESPACE_KEY = "gwNamespace";
	// name matches the workspace parameter provided to the factory
	protected static final String FEATURE_NAMESPACE_KEY = "namespace";
	protected static final String LOCK_MGT_KEY = "Lock Management";
	protected static final String AUTH_MGT_KEY = "Authorization Management Provider";
	protected static final String AUTH_URL_KEY = "Authorization Data URL";
	protected static final String TRANSACTION_BUFFER_SIZE = "Transaction Buffer Size";
	public static final String QUERY_INDEX_STRATEGY_KEY = "Query Index Strategy";

	private static final Param GEOWAVE_NAMESPACE = new Param(
			GEOWAVE_NAMESPACE_KEY,
			String.class,
			"The table namespace associated with this data store",
			true);
	private static final Param TRANSACTION_BUFFER_SIZE_PARAM = new Param(
			TRANSACTION_BUFFER_SIZE,
			Integer.class,
			"Number of buffered feature insertions before flushing to the datastore.",
			false);

	/*
	 * private static final Param FEATURE_NAMESPACE = new Param(
	 * FEATURE_NAMESPACE_KEY, String.class,
	 * "The overriding namespace for all feature types maintained within this data store"
	 * , false);
	 */

	private static final Param LOCK_MGT = new Param(
			LOCK_MGT_KEY,
			String.class,
			"WFS-T Locking Support.",
			true,
			null,
			getLockMgtOptions());

	private static final Param AUTH_MGT = new Param(
			AUTH_MGT_KEY,
			String.class,
			"The provider to obtain authorization given a user.",
			true,
			null,
			getAuthSPIOptions());

	private static final Param AUTH_URL = new Param(
			AUTH_URL_KEY,
			String.class,
			"The providers data URL.",
			false);

	private static final Param QUERY_INDEX_STRATEGY = new Param(
			QUERY_INDEX_STRATEGY_KEY,
			String.class,
			"Strategy to choose an index during query processing.",
			true,
			null,
			getIndexQueryStrategyOptions());

	private final AdapterStore adapterStore;
	private final DataStore dataStore;
	private final IndexStore indexStore;
	private final DataStatisticsStore dataStatisticsStore;
	private final String name;
	private final URI featureNameSpaceURI;
	private final LockingManagementFactory lockingManagementFactory;
	private final AuthorizationFactorySPI authorizationFactory;
	private final URL authorizationURL;
	private final Integer transactionBufferSize;
	private final IndexQueryStrategySPI indexQueryStrategy;

	private static Map<String, List<Param>> paramMap = new HashMap<String, List<Param>>();

	public static List<Param> getAuthPluginParams() {
		final List<Param> accumuloParams = new ArrayList<Param>();
		accumuloParams.add(AUTH_MGT);
		accumuloParams.add(AUTH_URL);
		return accumuloParams;
	}

	public synchronized static List<Param> getPluginParams(
			final StoreFactoryFamilySpi storeFactoryFamily ) {
		List<Param> params = paramMap.get(storeFactoryFamily.getName());
		if (params == null) {
			final AbstractConfigOption<?>[] configOptions = GeoWaveStoreFinder.getAllOptions(storeFactoryFamily);
			params = new ArrayList<Param>(
					Lists.transform(
							Lists.newArrayList(configOptions),
							new GeoWaveConfigOptionToGeoToolsConfigOption()));
			params.add(GEOWAVE_NAMESPACE);
			params.add(LOCK_MGT);
			params.add(AUTH_MGT);
			params.add(AUTH_URL);
			params.add(TRANSACTION_BUFFER_SIZE_PARAM);
			params.add(QUERY_INDEX_STRATEGY);
			paramMap.put(
					storeFactoryFamily.getName(),
					params);
		}
		return params;
	}

	public GeoWavePluginConfig(
			final StoreFactoryFamilySpi storeFactoryFamily,
			final Map<String, Serializable> params )
			throws GeoWavePluginException {

		Serializable param = params.get(GEOWAVE_NAMESPACE_KEY);
		if (param == null) {
			throw new GeoWavePluginException(
					"GeoWave Plugin: Missing namespace param");
		}
		final String namespace = param.toString();
		name = storeFactoryFamily.getName() + "_" + namespace;
		final Map<String, String> paramStrs = new HashMap<String, String>();
		// first converts serializable objects to String to avoid any issue if
		// there's a difference how geotools is converting objects to how
		// geowave intends to convert objects
		for (final Entry<String, Serializable> e : params.entrySet()) {
			paramStrs.put(
					e.getKey(),
					e.getValue() == null ? null : e.getValue().toString());
		}

		param = params.get(FEATURE_NAMESPACE_KEY);
		URI namespaceURI = null;
		if (param != null) {
			try {
				namespaceURI = param instanceof String ? new URI(
						param.toString()) : (URI) param;
			}
			catch (final URISyntaxException e) {
				LOGGER.error("Malformed Feature Namespace URI : " + param);
			}
		}
		featureNameSpaceURI = namespaceURI;

		param = params.get(TRANSACTION_BUFFER_SIZE);
		Integer bufferSizeFromParam = 10000;
		if (param != null) {
			try {
				bufferSizeFromParam = param instanceof Integer ? (Integer) param : Integer.parseInt(param.toString());
			}
			catch (final Exception e) {
				LOGGER.error("Malformed buffer size : " + param);
			}
		}
		transactionBufferSize = bufferSizeFromParam;

		param = params.get(LOCK_MGT_KEY);

		final Iterator<LockingManagementFactory> it = getLockManagementFactoryList();
		LockingManagementFactory factory = null;
		while (it.hasNext()) {
			factory = it.next();
			if ((param == null) || param.toString().equals(
					factory.toString())) {
				break;
			}
		}

		adapterStore = storeFactoryFamily.getAdapterStoreFactory().createStore(
				ConfigUtils.valuesFromStrings(
						paramStrs,
						storeFactoryFamily.getAdapterStoreFactory().getOptions()),
				namespace);

		dataStore = storeFactoryFamily.getDataStoreFactory().createStore(
				ConfigUtils.valuesFromStrings(
						paramStrs,
						storeFactoryFamily.getDataStoreFactory().getOptions()),
				namespace);

		dataStatisticsStore = storeFactoryFamily.getDataStatisticsStoreFactory().createStore(
				ConfigUtils.valuesFromStrings(
						paramStrs,
						storeFactoryFamily.getDataStatisticsStoreFactory().getOptions()),
				namespace);

		indexStore = storeFactoryFamily.getIndexStoreFactory().createStore(
				ConfigUtils.valuesFromStrings(
						paramStrs,
						storeFactoryFamily.getIndexStoreFactory().getOptions()),
				namespace);
		lockingManagementFactory = factory;

		authorizationFactory = getAuthorizationFactory(params);
		authorizationURL = getAuthorizationURL(params);
		indexQueryStrategy = getIndexQueryStrategy(params);
	}

	public String getName() {
		return name;
	}

	public static AuthorizationFactorySPI getAuthorizationFactory(
			final Map<String, Serializable> params )

			throws GeoWavePluginException {
		final Serializable param = params.get(AUTH_MGT_KEY);
		final Iterator<AuthorizationFactorySPI> authIt = getAuthorizationFactoryList();
		AuthorizationFactorySPI authFactory = new EmptyAuthorizationFactory();
		while (authIt.hasNext()) {
			authFactory = authIt.next();
			if ((param == null) || param.toString().equals(
					authFactory.toString())) {
				break;
			}
		}
		return authFactory;
	}

	public IndexQueryStrategySPI getIndexQueryStrategy() {
		return indexQueryStrategy;
	}

	public AdapterStore getAdapterStore() {
		return adapterStore;
	}

	public DataStore getDataStore() {
		return dataStore;
	}

	public IndexStore getIndexStore() {
		return indexStore;
	}

	public DataStatisticsStore getDataStatisticsStore() {
		return dataStatisticsStore;
	}

	public static IndexQueryStrategySPI getIndexQueryStrategy(
			final Map<String, Serializable> params )
			throws GeoWavePluginException {
		final Serializable param = params.get(QUERY_INDEX_STRATEGY_KEY);
		if (param != null) {
			final Iterator<IndexQueryStrategySPI> it = getInxexQueryStrategyList();
			while (it.hasNext()) {
				IndexQueryStrategySPI spi = it.next();
				if (spi.toString().equals(
						param.toString())) return spi;
			}

		}
		return new ChooseHeuristicMatchIndexQueryStrategy();
	}

	public static URL getAuthorizationURL(
			final Map<String, Serializable> params )
			throws GeoWavePluginException {

		final Serializable param = params.get(AUTH_URL_KEY);
		if (param == null) {
			return null;

		}
		else {
			try {
				return new URL(
						param.toString());
			}
			catch (final MalformedURLException e) {

				throw new GeoWavePluginException(
						"Accumulo Plugin: malformed Authorization Service URL " + param.toString());
			}
		}
	}

	protected AuthorizationFactorySPI getAuthorizationFactory() {
		return authorizationFactory;
	}

	protected URL getAuthorizationURL() {
		return authorizationURL;
	}

	public LockingManagementFactory getLockingManagementFactory() {
		return lockingManagementFactory;
	}

	public URI getFeatureNamespace() {
		return featureNameSpaceURI;
	}

	public Integer getTransactionBufferSize() {
		return transactionBufferSize;
	}

	private static Map<String, List<String>> getLockMgtOptions() {
		final List<String> options = new ArrayList<String>();
		final Iterator<LockingManagementFactory> it = getLockManagementFactoryList();
		while (it.hasNext()) {
			options.add(it.next().toString());
		}
		final Map<String, List<String>> map = new HashMap<String, List<String>>();
		map.put(
				Parameter.OPTIONS,
				options);
		return map;
	}

	private static Map<String, List<String>> getIndexQueryStrategyOptions() {
		final List<String> options = new ArrayList<String>();

		final Iterator<IndexQueryStrategySPI> it = getInxexQueryStrategyList();
		while (it.hasNext()) {
			options.add(it.next().toString());
		}
		final Map<String, List<String>> map = new HashMap<String, List<String>>();
		map.put(
				Parameter.OPTIONS,
				options);
		return map;
	}

	private static Map<String, List<String>> getAuthSPIOptions() {
		final List<String> options = new ArrayList<String>();
		final Iterator<AuthorizationFactorySPI> it = getAuthorizationFactoryList();
		while (it.hasNext()) {
			options.add(it.next().toString());
		}
		final Map<String, List<String>> map = new HashMap<String, List<String>>();
		map.put(
				Parameter.OPTIONS,
				options);
		return map;
	}

	private static Iterator<LockingManagementFactory> getLockManagementFactoryList() {
		final ServiceLoader<LockingManagementFactory> ldr = ServiceLoader.load(LockingManagementFactory.class);
		return ldr.iterator();
	}

	private static Iterator<AuthorizationFactorySPI> getAuthorizationFactoryList() {
		final ServiceLoader<AuthorizationFactorySPI> ldr = ServiceLoader.load(AuthorizationFactorySPI.class);
		return ldr.iterator();
	}

	private static Iterator<IndexQueryStrategySPI> getInxexQueryStrategyList() {
		final ServiceLoader<IndexQueryStrategySPI> ldr = ServiceLoader.load(IndexQueryStrategySPI.class);
		return ldr.iterator();
	}

	public static void applyOptions(
			final Options allOptions ) {
		final Option zookeeperUrl = new Option(
				"z",
				"zookeepers",
				true,
				"A comma-separated list of zookeeper servers that an Accumulo instance is using");
		allOptions.addOption(zookeeperUrl);
		final Option instanceId = new Option(
				"i",
				"instance-id",
				true,
				"The Accumulo instance ID");
		allOptions.addOption(instanceId);
		final Option user = new Option(
				"u",
				"user",
				true,
				"A valid Accumulo user ID");
		allOptions.addOption(user);
		final Option password = new Option(
				"p",
				"password",
				true,
				"The password for the user");
		allOptions.addOption(password);

		final Option namespace = new Option(
				"n",
				"namespace",
				true,
				"The table namespace (optional; default is no namespace)");
		allOptions.addOption(namespace);
	}

	private static class GeoWaveConfigOptionToGeoToolsConfigOption implements
			Function<AbstractConfigOption<?>, Param>
	{

		@Override
		public Param apply(
				final AbstractConfigOption<?> input ) {
			if (input instanceof PasswordConfigOption) {
				return new Param(
						input.getName(),
						String.class,
						input.getDescription(),
						!input.isOptional(),
						Collections.singletonMap(
								Parameter.IS_PASSWORD,
								Boolean.TRUE));
			}
			return new Param(
					input.getName(),
					GenericTypeResolver.resolveTypeArgument(
							input.getClass(),
							AbstractConfigOption.class),
					input.getDescription(),
					!input.isOptional());
		}
	}

}
