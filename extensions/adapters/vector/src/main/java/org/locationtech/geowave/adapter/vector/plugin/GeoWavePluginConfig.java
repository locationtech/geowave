/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.plugin;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.geotools.data.DataAccessFactory.Param;
import org.geotools.data.Parameter;
import org.locationtech.geowave.adapter.auth.AuthorizationFactorySPI;
import org.locationtech.geowave.adapter.auth.EmptyAuthorizationFactory;
import org.locationtech.geowave.adapter.vector.index.ChooseHeuristicMatchIndexQueryStrategy;
import org.locationtech.geowave.adapter.vector.index.IndexQueryStrategySPI;
import org.locationtech.geowave.adapter.vector.plugin.lock.LockingManagementFactory;
import org.locationtech.geowave.core.index.SPIServiceRegistry;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.GeoWaveStoreFinder;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.config.ConfigOption;
import org.locationtech.geowave.core.store.config.ConfigUtils;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class encapsulates the parameterized configuration that can be provided per GeoWave data
 * store within GeoTools. For GeoServer this configuration can be provided within the data store
 * definition workflow.
 */
public class GeoWavePluginConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWavePluginConfig.class);

  public static final String GEOWAVE_NAMESPACE_KEY = StoreFactoryOptions.GEOWAVE_NAMESPACE_OPTION;
  // name matches the workspace parameter provided to the factory
  protected static final String FEATURE_NAMESPACE_KEY = "namespace";
  protected static final String LOCK_MGT_KEY = "Lock Management";
  protected static final String AUTH_MGT_KEY = "Authorization Management Provider";
  protected static final String AUTH_URL_KEY = "Authorization Data URL";
  protected static final String TRANSACTION_BUFFER_SIZE = "Transaction Buffer Size";
  public static final String QUERY_INDEX_STRATEGY_KEY = "Query Index Strategy";
  public static final String DEFAULT_QUERY_INDEX_STRATEGY =
      ChooseHeuristicMatchIndexQueryStrategy.NAME;

  private static final Param GEOWAVE_NAMESPACE =
      new Param(
          GEOWAVE_NAMESPACE_KEY,
          String.class,
          "The table namespace associated with this data store",
          false);
  private static final Param TRANSACTION_BUFFER_SIZE_PARAM =
      new Param(
          TRANSACTION_BUFFER_SIZE,
          Integer.class,
          "Number of buffered feature insertions before flushing to the datastore when writing using WFS-T (advanced option, for basic usage leave as default).",
          false);

  private static final Param FEATURE_NAMESPACE =
      new Param(
          FEATURE_NAMESPACE_KEY,
          String.class,
          "The overriding namespace for all feature types maintained within this data store",
          false);

  private static final Param LOCK_MGT =
      new Param(
          LOCK_MGT_KEY,
          String.class,
          "WFS-T Locking Support (advanced option, for basic usage leave as default).",
          false,
          null,
          getLockMgtOptions());

  private static final Param AUTH_MGT =
      new Param(
          AUTH_MGT_KEY,
          String.class,
          "The provider to obtain authorization given a user (advanced option, for basic usage leave as default).",
          true,
          null,
          getAuthSPIOptions());

  private static final Param AUTH_URL =
      new Param(
          AUTH_URL_KEY,
          String.class,
          "The providers data URL (advanced option, for basic usage leave as default).",
          false);

  private static final Param QUERY_INDEX_STRATEGY =
      new Param(
          QUERY_INDEX_STRATEGY_KEY,
          String.class,
          "Strategy to choose an index during query processing (advanced option, for basic usage leave as default).",
          false,
          null,
          getIndexQueryStrategyOptions());

  private static final List<Param> BASE_GEOWAVE_PLUGIN_PARAMS =
      Arrays.asList(
          new Param[] {
              FEATURE_NAMESPACE,
              GEOWAVE_NAMESPACE,
              LOCK_MGT,
              AUTH_MGT,
              AUTH_URL,
              TRANSACTION_BUFFER_SIZE_PARAM,
              QUERY_INDEX_STRATEGY});
  public static final List<String> BASE_GEOWAVE_PLUGIN_PARAM_KEYS =
      Arrays.asList(
          BASE_GEOWAVE_PLUGIN_PARAMS.stream().map(p -> p.key).toArray(size -> new String[size]));

  private final PersistentAdapterStore adapterStore;
  private final InternalAdapterStore internalAdapterStore;
  private final DataStore dataStore;
  private final DataStoreOptions dataStoreOptions;
  private final IndexStore indexStore;
  private final DataStatisticsStore dataStatisticsStore;
  private final String name;
  private final URI featureNameSpaceURI;
  private final LockingManagementFactory lockingManagementFactory;
  private final AuthorizationFactorySPI authorizationFactory;
  private final URL authorizationURL;
  private final Integer transactionBufferSize;
  private final IndexQueryStrategySPI indexQueryStrategy;
  private final AdapterIndexMappingStore adapterIndexMappingStore;

  private static Map<String, List<Param>> paramMap = new HashMap<>();

  public static synchronized List<Param> getPluginParams(
      final StoreFactoryFamilySpi storeFactoryFamily) {
    List<Param> params = paramMap.get(storeFactoryFamily.getType());
    if (params == null) {
      final ConfigOption[] configOptions =
          GeoWaveStoreFinder.getAllOptions(storeFactoryFamily, false);
      params =
          Arrays.stream(configOptions).map(new GeoWaveConfigOptionToGeoToolsConfigOption()).collect(
              Collectors.toList());
      params.addAll(BASE_GEOWAVE_PLUGIN_PARAMS);
      paramMap.put(storeFactoryFamily.getType(), params);
    }
    return params;
  }

  public GeoWavePluginConfig(final DataStorePluginOptions params) throws GeoWavePluginException {
    this(
        params.getFactoryFamily(),
        // converting to Map<String,String> to Map<String,Serializable>
        params.getOptionsAsMap().entrySet().stream().collect(
            Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
  }

  public GeoWavePluginConfig(
      final StoreFactoryFamilySpi storeFactoryFamily,
      final Map<String, ?> params) throws GeoWavePluginException {

    Object param = params.get(GEOWAVE_NAMESPACE_KEY);
    name = storeFactoryFamily.getType() + (param == null ? "" : ("_" + param));
    final Map<String, String> paramStrs = new HashMap<>();
    // first converts serializable objects to String to avoid any issue if
    // there's a difference how geotools is converting objects to how
    // geowave intends to convert objects
    for (final Entry<String, ?> e : params.entrySet()) {
      paramStrs.put(e.getKey(), e.getValue() == null ? null : e.getValue().toString());
    }

    param = params.get(FEATURE_NAMESPACE_KEY);
    URI namespaceURI = null;
    if ((param != null) && !param.toString().trim().isEmpty()) {
      try {
        namespaceURI = param instanceof String ? new URI(param.toString()) : (URI) param;
      } catch (final URISyntaxException e) {
        LOGGER.error("Malformed Feature Namespace URI : " + param, e);
      }
    }
    featureNameSpaceURI = namespaceURI;
    param = params.get(TRANSACTION_BUFFER_SIZE);
    Integer bufferSizeFromParam = 10000;
    if ((param != null) && !param.toString().trim().isEmpty()) {
      try {
        bufferSizeFromParam =
            param instanceof Integer ? (Integer) param : Integer.parseInt(param.toString());
      } catch (final Exception e) {
        LOGGER.error("Malformed buffer size : " + param, e);
      }
    }
    transactionBufferSize = bufferSizeFromParam;

    param = params.get(LOCK_MGT_KEY);

    final Iterator<LockingManagementFactory> it = getLockManagementFactoryList();
    LockingManagementFactory factory = null;
    while (it.hasNext()) {
      factory = it.next();
      if ((param == null)
          || param.toString().trim().isEmpty()
          || param.toString().equals(factory.toString())) {
        break;
      }
    }
    final StoreFactoryOptions options =
        ConfigUtils.populateOptionsFromList(
            storeFactoryFamily.getAdapterStoreFactory().createOptionsInstance(),
            paramStrs);
    adapterStore = storeFactoryFamily.getAdapterStoreFactory().createStore(options);
    internalAdapterStore = storeFactoryFamily.getInternalAdapterStoreFactory().createStore(options);

    dataStore = storeFactoryFamily.getDataStoreFactory().createStore(options);

    dataStoreOptions = options.getStoreOptions();

    dataStatisticsStore = storeFactoryFamily.getDataStatisticsStoreFactory().createStore(options);

    indexStore = storeFactoryFamily.getIndexStoreFactory().createStore(options);
    adapterIndexMappingStore =
        storeFactoryFamily.getAdapterIndexMappingStoreFactory().createStore(options);
    lockingManagementFactory = factory;

    authorizationFactory = getAuthorizationFactory(params);
    authorizationURL = getAuthorizationURL(params);
    indexQueryStrategy = getIndexQueryStrategy(params);
  }

  public String getName() {
    return name;
  }

  public static AuthorizationFactorySPI getAuthorizationFactory(final Map<String, ?> params)
      throws GeoWavePluginException {
    final Object param = params.get(AUTH_MGT_KEY);
    final Iterator<AuthorizationFactorySPI> authIt = getAuthorizationFactoryList();
    AuthorizationFactorySPI authFactory = new EmptyAuthorizationFactory();
    while (authIt.hasNext()) {
      authFactory = authIt.next();
      if ((param == null)
          || param.toString().trim().isEmpty()
          || param.toString().equals(authFactory.toString())) {
        break;
      }
    }
    return authFactory;
  }

  public IndexQueryStrategySPI getIndexQueryStrategy() {
    return indexQueryStrategy;
  }

  public PersistentAdapterStore getAdapterStore() {
    return adapterStore;
  }

  public InternalAdapterStore getInternalAdapterStore() {
    return internalAdapterStore;
  }

  public DataStore getDataStore() {
    return dataStore;
  }

  public DataStoreOptions getDataStoreOptions() {
    return dataStoreOptions;
  }

  public AdapterIndexMappingStore getAdapterIndexMappingStore() {
    return adapterIndexMappingStore;
  }

  public IndexStore getIndexStore() {
    return indexStore;
  }

  public DataStatisticsStore getDataStatisticsStore() {
    return dataStatisticsStore;
  }

  public static IndexQueryStrategySPI getIndexQueryStrategy(final Map<String, ?> params)
      throws GeoWavePluginException {
    final Object param = params.get(QUERY_INDEX_STRATEGY_KEY);
    final String strategy =
        ((param == null) || param.toString().trim().isEmpty()) ? DEFAULT_QUERY_INDEX_STRATEGY
            : param.toString();
    final Iterator<IndexQueryStrategySPI> it = getInxexQueryStrategyList();
    while (it.hasNext()) {
      final IndexQueryStrategySPI spi = it.next();
      if (spi.toString().equals(strategy)) {
        return spi;
      }
    }
    // This would only get hit if the default query index strategy is removed from the spi registry.
    return null;
  }

  public static URL getAuthorizationURL(final Map<String, ?> params) throws GeoWavePluginException {
    final Object param = params.get(AUTH_URL_KEY);
    if ((param == null) || param.toString().trim().isEmpty()) {
      return null;
    } else {
      try {
        return new URL(param.toString());
      } catch (final MalformedURLException e) {

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
    final List<String> options = new ArrayList<>();
    final Iterator<LockingManagementFactory> it = getLockManagementFactoryList();
    while (it.hasNext()) {
      options.add(it.next().toString());
    }
    final Map<String, List<String>> map = new HashMap<>();
    map.put(Parameter.OPTIONS, options);
    return map;
  }

  static final List<String> BooleanOptions = Arrays.asList("true", "false");

  private static Map<String, List<String>> getIndexQueryStrategyOptions() {
    final List<String> options = new ArrayList<>();

    final Iterator<IndexQueryStrategySPI> it = getInxexQueryStrategyList();
    while (it.hasNext()) {
      options.add(it.next().toString());
    }
    final Map<String, List<String>> map = new HashMap<>();
    map.put(Parameter.OPTIONS, options);
    return map;
  }

  private static Map<String, List<String>> getAuthSPIOptions() {
    final List<String> options = new ArrayList<>();
    final Iterator<AuthorizationFactorySPI> it = getAuthorizationFactoryList();
    while (it.hasNext()) {
      options.add(it.next().toString());
    }
    final Map<String, List<String>> map = new HashMap<>();
    map.put(Parameter.OPTIONS, options);
    return map;
  }

  private static Iterator<LockingManagementFactory> getLockManagementFactoryList() {
    return new SPIServiceRegistry(GeoWavePluginConfig.class).load(LockingManagementFactory.class);
  }

  private static Iterator<AuthorizationFactorySPI> getAuthorizationFactoryList() {
    return new SPIServiceRegistry(GeoWavePluginConfig.class).load(AuthorizationFactorySPI.class);
  }

  private static Iterator<IndexQueryStrategySPI> getInxexQueryStrategyList() {
    return new SPIServiceRegistry(GeoWavePluginConfig.class).load(IndexQueryStrategySPI.class);
  }

  private static class GeoWaveConfigOptionToGeoToolsConfigOption implements
      Function<ConfigOption, Param> {

    @Override
    public Param apply(final ConfigOption input) {
      if (input.isPassword()) {
        return new Param(
            input.getName(),
            String.class,
            input.getDescription(),
            !input.isOptional(),
            "mypassword",
            Collections.singletonMap(Parameter.IS_PASSWORD, Boolean.TRUE));
      }
      if (input.getType().isPrimitive() && (input.getType() == boolean.class)) {
        return new Param(
            input.getName(),
            input.getType(),
            input.getDescription(),
            true,
            "true",
            Collections.singletonMap(Parameter.OPTIONS, BooleanOptions));
      }
      return new Param(
          input.getName(),
          input.usesStringConverter() ? String.class : input.getType(),
          input.getDescription(),
          !input.isOptional());
    }
  }
}
