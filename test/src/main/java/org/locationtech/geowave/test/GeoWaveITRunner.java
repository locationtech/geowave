/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.internal.runners.statements.RunAfters;
import org.junit.internal.runners.statements.RunBefores;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.Suite;
import org.junit.runners.model.FrameworkField;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.test.annotation.Environments;
import org.locationtech.geowave.test.annotation.Environments.Environment;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.annotation.GeoWaveTestStoreImpl;
import org.locationtech.geowave.test.annotation.NamespaceOverride;
import org.locationtech.geowave.test.annotation.OptionsOverride;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoWaveITRunner extends Suite {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveITRunner.class);
  public static final AtomicBoolean DEFER_CLEANUP = new AtomicBoolean(false);
  public static final Object MUTEX = new Object();

  public static final String STORE_TYPE_ENVIRONMENT_VARIABLE_NAME = "STORE_TYPE";
  public static final String STORE_TYPE_PROPERTY_NAME = "testStoreType";

  public static final String DATASTORE_OPTIONS_ENVIRONMENT_VARIABLE_NAME = "STORE_OPTIONS";
  public static final String DATASTORE_OPTIONS_PROPERTY_NAME = "testStoreOptions";

  @Override
  protected Statement withBeforeClasses(final Statement statement) {
    // add test environment setup
    try {
      final Method setupMethod = GeoWaveITRunner.class.getDeclaredMethod("setup");
      setupMethod.setAccessible(true);
      return super.withBeforeClasses(
          new RunBefores(
              statement,
              Collections.singletonList(new FrameworkMethod(setupMethod)),
              this));
    } catch (NoSuchMethodException | SecurityException e) {
      LOGGER.warn("Unable to find setup method", e);
    }

    return super.withBeforeClasses(statement);
  }

  @Override
  protected Statement withAfterClasses(final Statement statement) {
    // add test environment tear down
    try {
      final Statement newStatement = super.withAfterClasses(statement);
      final Method tearDownMethod = GeoWaveITRunner.class.getDeclaredMethod("tearDown");
      tearDownMethod.setAccessible(true);
      return new RunAfters(
          newStatement,
          Collections.singletonList(new FrameworkMethod(tearDownMethod)),
          this);
    } catch (NoSuchMethodException | SecurityException e) {
      LOGGER.warn("Unable to find tearDown method", e);
    }
    return super.withAfterClasses(statement);
  }

  private class TestClassRunnerForStoreTypes extends BlockJUnit4ClassRunner {
    private final Map<String, GeoWaveStoreType> fieldNameStoreTypePair;
    private final String nameSuffix;
    private final String[] profileOptions;

    private TestClassRunnerForStoreTypes(
        final Class<?> type,
        final Map<String, GeoWaveStoreType> fieldNameStoreTypePair,
        final String[] profileOptions) throws InitializationError {
      super(type);

      this.fieldNameStoreTypePair = fieldNameStoreTypePair;
      this.profileOptions = profileOptions;

      final StringBuilder nameBldr = new StringBuilder();
      for (final Entry<String, GeoWaveStoreType> e : fieldNameStoreTypePair.entrySet()) {
        nameBldr.append(" (").append(e.getKey()).append("=").append(e.getValue().toString()).append(
            ")");
      }
      if ((profileOptions != null) && (profileOptions.length > 0)) {
        nameBldr.append("; options=").append("\"" + String.join(",", profileOptions) + "\"");
      }
      nameSuffix = nameBldr.toString();
    }

    @Override
    public Object createTest() throws Exception {
      return createTestUsingFieldInjection();
    }

    private Object createTestUsingFieldInjection() throws IllegalAccessException, SecurityException,
        NoSuchFieldException, GeoWaveITException, InstantiationException {
      final Set<Pair<Field, GeoWaveTestStore>> fieldsAndStorePairs = new HashSet<>();
      if (typeIsAnnotated()) {
        final GeoWaveTestStore store =
            getTestClass().getJavaClass().getAnnotation(GeoWaveTestStore.class);
        for (final String fieldName : fieldNameStoreTypePair.keySet()) {
          final Field field = getTestClass().getJavaClass().getDeclaredField(fieldName);
          final GeoWaveTestStoreImpl storeWithOverrides = new GeoWaveTestStoreImpl(store);
          if (field.isAnnotationPresent(NamespaceOverride.class)) {
            storeWithOverrides.setNamespace(field.getAnnotation(NamespaceOverride.class).value());
          } else if (field.isAnnotationPresent(OptionsOverride.class)) {
            storeWithOverrides.setOptions(field.getAnnotation(OptionsOverride.class).value());
          }
          fieldsAndStorePairs.add(
              new ImmutablePair<Field, GeoWaveTestStore>(field, storeWithOverrides));
        }
      } else {
        final List<FrameworkField> annotatedFields = getStoreAnnotatedFields();
        if (annotatedFields.size() != fieldNameStoreTypePair.size()) {
          throw new GeoWaveITException(
              "Wrong number of stores and @GeoWaveTestStore fields."
                  + " @GeoWaveTestStore fields counted: "
                  + annotatedFields.size()
                  + ", available parameters: "
                  + fieldNameStoreTypePair.size()
                  + ".");
        }
        for (final FrameworkField field : annotatedFields) {
          fieldsAndStorePairs.add(
              new ImmutablePair<>(
                  field.getField(),
                  field.getField().getAnnotation(GeoWaveTestStore.class)));
        }
      }

      final Object testClassInstance = getTestClass().getJavaClass().newInstance();

      for (final Pair<Field, GeoWaveTestStore> field : fieldsAndStorePairs) {
        final GeoWaveStoreType type = fieldNameStoreTypePair.get(field.getLeft().getName());
        field.getLeft().setAccessible(true);
        final GeoWaveTestStore store = field.getRight();
        field.getLeft().set(
            testClassInstance,
            type.getTestEnvironment().getDataStoreOptions(store, profileOptions));
      }

      return testClassInstance;
    }

    @Override
    protected String getName() {
      return super.getName() + nameSuffix;
    }

    @Override
    protected String testName(final FrameworkMethod method) {
      return method.getName() + " - " + getName();
    }

    @Override
    protected void validateFields(final List<Throwable> errors) {
      super.validateFields(errors);
      if (typeIsAnnotated()) {
        if (fieldsAreAnnotated()) {
          errors.add(
              new GeoWaveITException(
                  "Only type or fields can be annotated with @GeoWaveTestStore, not both"));
        }
        try {
          getDataStoreOptionFieldsForTypeAnnotation();
        } catch (final Exception e) {
          errors.add(e);
        }
      } else if (fieldsAreAnnotated()) {
        final List<FrameworkField> annotatedFields = getStoreAnnotatedFields();
        for (final FrameworkField field : annotatedFields) {
          if (!field.getType().isAssignableFrom(DataStorePluginOptions.class)) {
            errors.add(
                new GeoWaveITException(
                    "'"
                        + field.getName()
                        + "' must be of type '"
                        + DataStorePluginOptions.class.getName()
                        + "'"));
          }
        }
      }
    }

    @Override
    protected Statement classBlock(final RunNotifier notifier) {
      return childrenInvoker(notifier);
    }

    @Override
    protected Annotation[] getRunnerAnnotations() {
      return new Annotation[0];
    }
  }

  private static final List<Runner> NO_RUNNERS = Collections.<Runner>emptyList();

  private final List<Runner> runners = new ArrayList<>();
  private final Set<GeoWaveStoreType> storeTypes = new HashSet<>();
  private final TestEnvironment[] testEnvs;

  /** Only called reflectively. Do not use programmatically. */
  public GeoWaveITRunner(final Class<?> klass)
      throws InitializationError, SecurityException, GeoWaveITException {
    super(klass, NO_RUNNERS);
    createRunnersForDataStores();
    testEnvs = getTestEnvironments();
  }

  @Override
  protected List<Runner> getChildren() {
    return runners;
  }

  private void createRunnersForDataStores()
      throws InitializationError, SecurityException, GeoWaveITException {
    List<GeoWaveStoreRunnerConfig> configs = new ArrayList<>();

    String storeTypeProp = System.getenv(STORE_TYPE_ENVIRONMENT_VARIABLE_NAME);
    if (!TestUtils.isSet(storeTypeProp)) {
      storeTypeProp = System.getProperty(STORE_TYPE_PROPERTY_NAME);
    }
    final GeoWaveStoreType storeType;
    final Set<String> dataStoreOptionFields = getDataStoreOptionFieldsForTypeAnnotation();
    // See if user specified a single store type
    if (TestUtils.isSet(storeTypeProp)) {
      storeType = GeoWaveStoreType.valueOf(storeTypeProp);
    } else { // No user override - just use RocksDB
      storeType = GeoWaveStoreType.ROCKSDB;
    }
    if (containsAnnotationForType(storeType)) {
      configs.add(new GeoWaveStoreRunnerConfig(storeType, dataStoreOptionFields));
      storeTypes.add(storeType);
    }

    // Get the set of profile options from the profile, if any
    final String[][] profileOptionSets = getProfileOptionSets();

    // Iterate through option sets to create runners
    for (final String[] profileOptions : profileOptionSets) {
      // Create a test runner for each store type / config
      for (final GeoWaveStoreRunnerConfig config : configs) {
        final TestClassRunnerForStoreTypes runner =
            new TestClassRunnerForStoreTypes(
                getTestClass().getJavaClass(),
                config.fieldNameStoreTypePair,
                profileOptions);
        runners.add(runner);
      }
    }
  }

  private String[][] getProfileOptionSets() {
    String optionsStr = System.getenv(DATASTORE_OPTIONS_ENVIRONMENT_VARIABLE_NAME);
    if (!TestUtils.isSet(optionsStr)) {
      optionsStr = System.getProperty(DATASTORE_OPTIONS_PROPERTY_NAME);
    }

    String[][] profileOptions = null;
    if (TestUtils.isSet(optionsStr)) {
      final String[] optionSets = optionsStr.split("!");
      profileOptions = new String[optionSets.length][];

      for (int i = 0; i < optionSets.length; i++) {
        profileOptions[i] = optionSets[i].split(",");
      }
    }

    if (profileOptions == null) {
      profileOptions = new String[1][];
    }
    return profileOptions;
  }

  private boolean containsAnnotationForType(final GeoWaveStoreType storeType) {
    if (typeIsAnnotated()) {
      final GeoWaveTestStore store =
          getTestClass().getJavaClass().getAnnotation(GeoWaveTestStore.class);
      for (final GeoWaveStoreType annotationType : store.value()) {
        if (annotationType == storeType) {
          return true;
        }
      }
    } else {
      for (final FrameworkField field : getTestClass().getAnnotatedFields(GeoWaveTestStore.class)) {
        for (final GeoWaveStoreType annotationType : field.getField().getAnnotation(
            GeoWaveTestStore.class).value()) {
          if (annotationType == storeType) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private Set<String> getDataStoreOptionFieldsForTypeAnnotation()
      throws SecurityException, GeoWaveITException {
    final Field[] fields = getTestClass().getJavaClass().getDeclaredFields();
    final Set<String> dataStoreOptionFields = new HashSet<>();
    for (final Field field : fields) {
      if (field.getType().isAssignableFrom(DataStorePluginOptions.class)) {
        dataStoreOptionFields.add(field.getName());
      }
    }
    if (dataStoreOptionFields.isEmpty()) {
      throw new GeoWaveITException(
          "Types annotated with GeoWaveTestStore must have at least one field of type DataStorePluginOptions");
    }
    return dataStoreOptionFields;
  }

  private static List<GeoWaveStoreRunnerConfig> addRunnerConfigsForField(
      final FrameworkField field,
      final List<GeoWaveStoreRunnerConfig> currentConfigs,
      final Set<GeoWaveStoreType> storeTypes) throws GeoWaveITException {
    final GeoWaveTestStore store = field.getField().getAnnotation(GeoWaveTestStore.class);
    final GeoWaveStoreType[] types = store.value();
    if ((types == null) || (types.length == 0)) {
      throw new GeoWaveITException(
          MessageFormat.format("{0} must have at least one GeoWaveStoreType", field.getName()));
    }
    final List<GeoWaveStoreRunnerConfig> newConfigs = new ArrayList<>();
    for (final GeoWaveStoreRunnerConfig config : currentConfigs) {
      for (final GeoWaveStoreType type : types) {
        newConfigs.add(new GeoWaveStoreRunnerConfig(config, field.getName(), type));

        storeTypes.add(type);
      }
    }
    return newConfigs;
  }

  private List<FrameworkField> getStoreAnnotatedFields() {
    return getTestClass().getAnnotatedFields(GeoWaveTestStore.class);
  }

  private List<FrameworkMethod> getTestEnvAnnotatedMethods() {
    return getTestClass().getAnnotatedMethods(Environments.class);
  }

  private TestEnvironment[] getTestEnvironments() throws NullPointerException {
    final Set<Environment> environments = new HashSet<>();
    final Environments es = getTestClass().getJavaClass().getAnnotation(Environments.class);
    if (es != null) {
      final Environment[] envs = es.value();
      for (final Environment env : envs) {
        environments.add(env);
      }
    }
    final List<FrameworkMethod> envMethods = getTestEnvAnnotatedMethods();

    for (final FrameworkMethod m : envMethods) {
      final Environment[] envs = m.getMethod().getAnnotation(Environments.class).value();
      for (final Environment env : envs) {
        environments.add(env);
      }
    }
    final TestEnvironment[] testEnvs = new TestEnvironment[environments.size() + storeTypes.size()];
    int i = 0;
    for (final GeoWaveStoreType t : storeTypes) {
      testEnvs[i++] = t.getTestEnvironment();
    }
    for (final Environment e : environments) {
      testEnvs[i++] = e.getTestEnvironment();
    }

    return processDependencies(testEnvs);
  }

  private TestEnvironment[] processDependencies(final TestEnvironment[] testEnvs) {
    final TestEnvironmentDependencyTree dependencyTree = new TestEnvironmentDependencyTree();
    for (final TestEnvironment e : testEnvs) {
      dependencyTree.processDependencies(e);
    }
    return dependencyTree.getOrderedTestEnvironments();
  }

  private boolean fieldsAreAnnotated() {
    return !getStoreAnnotatedFields().isEmpty();
  }

  private boolean typeIsAnnotated() {
    return getTestClass().getJavaClass().isAnnotationPresent(GeoWaveTestStore.class);
  }

  protected void setup() throws Exception {
    synchronized (MUTEX) {
      TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
      for (final TestEnvironment e : testEnvs) {
        e.setup();
      }
    }
  }

  protected void tearDown() throws Exception {
    synchronized (MUTEX) {
      if (!DEFER_CLEANUP.get()) {
        // Teardown in reverse
        final List<TestEnvironment> envs = Arrays.asList(testEnvs);
        final ListIterator<TestEnvironment> it = envs.listIterator(envs.size());
        while (it.hasPrevious()) {
          it.previous().tearDown();
        }
      }
    }
  }

  private static class GeoWaveStoreRunnerConfig {
    private final Map<String, GeoWaveStoreType> fieldNameStoreTypePair;

    public GeoWaveStoreRunnerConfig() {
      fieldNameStoreTypePair = new HashMap<>();
    }

    public GeoWaveStoreRunnerConfig(
        final GeoWaveStoreType storeType,
        final Set<String> fieldNames) {
      fieldNameStoreTypePair = new HashMap<>();
      for (final String fieldName : fieldNames) {
        fieldNameStoreTypePair.put(fieldName, storeType);
      }
    }

    public GeoWaveStoreRunnerConfig(
        final GeoWaveStoreRunnerConfig previousConfig,
        final String name,
        final GeoWaveStoreType type) {
      if ((previousConfig == null) || (previousConfig.fieldNameStoreTypePair == null)) {
        fieldNameStoreTypePair = new HashMap<>();
      } else {
        fieldNameStoreTypePair = new HashMap<>(previousConfig.fieldNameStoreTypePair);
      }
      fieldNameStoreTypePair.put(name, type);
    }
  }

  private static class GeoWaveITException extends Exception {

    /** */
    private static final long serialVersionUID = 1L;

    public GeoWaveITException(final String message) {
      super(message);
    }
  }

  private static class TestEnvironmentDependencyTree {
    // just keep a two-way mapping although I think we only need to traverse
    // in one direction
    Map<TestEnvironment, Set<TestEnvironment>> dependenciesMapping = new LinkedHashMap<>();
    Map<TestEnvironment, Set<TestEnvironment>> requirementsMapping = new LinkedHashMap<>();
    Set<TestEnvironment> independentEnvironments = new LinkedHashSet<>();
    Set<TestEnvironment> visitedEnvs = new LinkedHashSet<>();

    private TestEnvironmentDependencyTree() {}

    private void processDependencies(final TestEnvironment env) {
      if (!visitedEnvs.contains(env)) {
        visitedEnvs.add(env);
        if ((env.getDependentEnvironments() == null)
            || (env.getDependentEnvironments().length == 0)) {
          independentEnvironments.add(env);
        } else {

          for (final TestEnvironment requiredEnv : env.getDependentEnvironments()) {
            Set<TestEnvironment> dependentSet = dependenciesMapping.get(requiredEnv);
            if (dependentSet == null) {
              dependentSet = new HashSet<>();
              dependenciesMapping.put(requiredEnv, dependentSet);
            }
            dependentSet.add(env);
            Set<TestEnvironment> requiredSet = requirementsMapping.get(env);
            if (requiredSet == null) {
              requiredSet = new HashSet<>();
              requirementsMapping.put(env, requiredSet);
            }
            requiredSet.add(requiredEnv);
            processDependencies(requiredEnv);
          }
        }
      }
    }

    private TestEnvironment[] getOrderedTestEnvironments() {
      final TestEnvironment[] retVal = new TestEnvironment[visitedEnvs.size()];
      int i = 0;
      final Set<TestEnvironment> testsAddedToArray = new HashSet<>();
      for (final TestEnvironment e : independentEnvironments) {
        retVal[i++] = e;
        testsAddedToArray.add(e);
      }
      for (final TestEnvironment entry : requirementsMapping.keySet()) {
        traverseRequirements(entry, retVal, i++, testsAddedToArray);
      }
      return retVal;
    }

    private int traverseRequirements(
        final TestEnvironment env,
        final TestEnvironment[] currentOrderedArray,
        final int startIndex,
        final Set<TestEnvironment> testsAddedToArray) {
      int count = 0;
      final Set<TestEnvironment> requirements = requirementsMapping.get(env);
      for (final TestEnvironment req : requirements) {
        if (!testsAddedToArray.contains(req)) {
          count =
              traverseRequirements(req, currentOrderedArray, startIndex + count, testsAddedToArray);
        }
      }
      currentOrderedArray[startIndex + count++] = env;
      testsAddedToArray.add(env);
      return count;
    }
  }
}
