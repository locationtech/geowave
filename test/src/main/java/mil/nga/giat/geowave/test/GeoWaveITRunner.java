package mil.nga.giat.geowave.test;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.test.annotation.Environments;
import mil.nga.giat.geowave.test.annotation.Environments.Environment;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStoreImpl;
import mil.nga.giat.geowave.test.annotation.NamespaceOverride;
import mil.nga.giat.geowave.test.annotation.OptionsOverride;

public class GeoWaveITRunner extends
		Suite
{
	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveITRunner.class);
	public static final AtomicBoolean DEFER_CLEANUP = new AtomicBoolean(
			false);
	public static final Object MUTEX = new Object();

	public static final String STORE_TYPE_ENVIRONMENT_VARIABLE_NAME = "STORE_TYPE";
	public static final String STORE_TYPE_PROPERTY_NAME = "testStoreType";

	@Override
	protected Statement withBeforeClasses(
			final Statement statement ) {
		// add test environment setup
		try {
			final Method setupMethod = GeoWaveITRunner.class.getDeclaredMethod("setup");
			setupMethod.setAccessible(true);
			return super.withBeforeClasses(new RunBefores(
					statement,
					Collections.singletonList(new FrameworkMethod(
							setupMethod)),
					this));
		}
		catch (NoSuchMethodException | SecurityException e) {
			LOGGER.warn(
					"Unable to find setup method",
					e);
		}

		return super.withBeforeClasses(statement);
	}

	@Override
	protected Statement withAfterClasses(
			final Statement statement ) {
		// add test environment tear down
		try {
			final Statement newStatement = super.withAfterClasses(statement);
			final Method tearDownMethod = GeoWaveITRunner.class.getDeclaredMethod("tearDown");
			tearDownMethod.setAccessible(true);
			return new RunAfters(
					newStatement,
					Collections.singletonList(new FrameworkMethod(
							tearDownMethod)),
					this);
		}
		catch (NoSuchMethodException | SecurityException e) {
			LOGGER.warn(
					"Unable to find tearDown method",
					e);
		}
		return super.withAfterClasses(statement);
	}

	private class TestClassRunnerForStoreTypes extends
			BlockJUnit4ClassRunner
	{
		private final Map<String, GeoWaveStoreType> fieldNameStoreTypePair;
		private final String nameSuffix;

		private TestClassRunnerForStoreTypes(
				final Class<?> type,
				final Map<String, GeoWaveStoreType> fieldNameStoreTypePair )
				throws InitializationError {
			super(
					type);

			this.fieldNameStoreTypePair = fieldNameStoreTypePair;

			final StringBuilder nameBldr = new StringBuilder();
			for (final Entry<String, GeoWaveStoreType> e : fieldNameStoreTypePair.entrySet()) {
				nameBldr.append(
						" (").append(
						e.getKey()).append(
						"=").append(
						e.getValue().toString()).append(
						")");
			}

			nameSuffix = nameBldr.toString();
		}

		@Override
		public Object createTest()
				throws Exception {
			return createTestUsingFieldInjection();
		}

		private Object createTestUsingFieldInjection()
				throws IllegalAccessException,
				SecurityException,
				NoSuchFieldException,
				GeoWaveITException,
				InstantiationException {
			final Set<Pair<Field, GeoWaveTestStore>> fieldsAndStorePairs = new HashSet<Pair<Field, GeoWaveTestStore>>();
			if (typeIsAnnotated()) {
				final GeoWaveTestStore store = getTestClass().getJavaClass().getAnnotation(
						GeoWaveTestStore.class);
				for (final String fieldName : fieldNameStoreTypePair.keySet()) {
					final Field field = getTestClass().getJavaClass().getDeclaredField(
							fieldName);
					final GeoWaveTestStoreImpl storeWithOverrides = new GeoWaveTestStoreImpl(
							store);
					if (field.isAnnotationPresent(NamespaceOverride.class)) {
						storeWithOverrides.setNamespace(field.getAnnotation(
								NamespaceOverride.class).value());
					}
					else if (field.isAnnotationPresent(OptionsOverride.class)) {
						storeWithOverrides.setOptions(field.getAnnotation(
								OptionsOverride.class).value());
					}
					fieldsAndStorePairs.add(new ImmutablePair<Field, GeoWaveTestStore>(
							field,
							storeWithOverrides));
				}
			}
			else {
				final List<FrameworkField> annotatedFields = getStoreAnnotatedFields();
				if (annotatedFields.size() != fieldNameStoreTypePair.size()) {
					throw new GeoWaveITException(
							"Wrong number of stores and @GeoWaveTestStore fields."
									+ " @GeoWaveTestStore fields counted: " + annotatedFields.size()
									+ ", available parameters: " + fieldNameStoreTypePair.size() + ".");
				}
				for (final FrameworkField field : annotatedFields) {
					fieldsAndStorePairs.add(new ImmutablePair<Field, GeoWaveTestStore>(
							field.getField(),
							field.getField().getAnnotation(
									GeoWaveTestStore.class)));
				}
			}
			final Object testClassInstance = getTestClass().getJavaClass().newInstance();
			for (final Pair<Field, GeoWaveTestStore> field : fieldsAndStorePairs) {
				final GeoWaveStoreType type = fieldNameStoreTypePair.get(field.getLeft().getName());
				field.getLeft().setAccessible(
						true);
				final GeoWaveTestStore store = field.getRight();
				field.getLeft().set(
						testClassInstance,
						type.getTestEnvironment().getDataStoreOptions(
								store));
			}
			return testClassInstance;
		}

		@Override
		protected String getName() {
			return super.getName() + nameSuffix;
		}

		@Override
		protected String testName(
				final FrameworkMethod method ) {
			return method.getName() + " - " + getName();
		}

		@Override
		protected void validateFields(
				final List<Throwable> errors ) {
			super.validateFields(errors);
			if (typeIsAnnotated()) {
				if (fieldsAreAnnotated()) {
					errors.add(new GeoWaveITException(
							"Only type or fields can be annotated with @GeoWaveTestStore, not both"));
				}
				try {
					getDataStoreOptionFieldsForTypeAnnotation();
				}
				catch (final Exception e) {
					errors.add(e);
				}
			}
			else if (fieldsAreAnnotated()) {
				final List<FrameworkField> annotatedFields = getStoreAnnotatedFields();
				for (final FrameworkField field : annotatedFields) {
					if (!field.getType().isAssignableFrom(
							DataStorePluginOptions.class)) {
						errors.add(new GeoWaveITException(
								"'" + field.getName() + "' must be of type '" + DataStorePluginOptions.class.getName()
										+ "'"));
					}
				}
			}
		}

		@Override
		protected Statement classBlock(
				final RunNotifier notifier ) {
			return childrenInvoker(notifier);
		}

		@Override
		protected Annotation[] getRunnerAnnotations() {
			return new Annotation[0];
		}
	}

	private static final List<Runner> NO_RUNNERS = Collections.<Runner> emptyList();

	private final List<Runner> runners = new ArrayList<Runner>();
	private final Set<GeoWaveStoreType> storeTypes = new HashSet<GeoWaveStoreType>();
	private final TestEnvironment[] testEnvs;

	/**
	 * Only called reflectively. Do not use programmatically.
	 */
	public GeoWaveITRunner(
			final Class<?> klass )
			throws InitializationError,
			SecurityException,
			GeoWaveITException {
		super(
				klass,
				NO_RUNNERS);
		createRunnersForDataStores();
		testEnvs = getTestEnvironments();
	}

	@Override
	protected List<Runner> getChildren() {
		return runners;
	}

	private void createRunnersForDataStores()
			throws InitializationError,
			SecurityException,
			GeoWaveITException {
		final GeoWaveStoreRunnerConfig emptyConfig = new GeoWaveStoreRunnerConfig();
		List<GeoWaveStoreRunnerConfig> configs = new ArrayList<GeoWaveStoreRunnerConfig>();

		String storeTypeProp = System.getenv(STORE_TYPE_ENVIRONMENT_VARIABLE_NAME);
		if (!TestUtils.isSet(storeTypeProp)) {
			storeTypeProp = System.getProperty(STORE_TYPE_PROPERTY_NAME);
		}
		// See if user specified a single store type
		if (TestUtils.isSet(storeTypeProp)) {
			final Set<String> dataStoreOptionFields = getDataStoreOptionFieldsForTypeAnnotation();
			final GeoWaveStoreType storeType = GeoWaveStoreType.valueOf(storeTypeProp);

			// If no match, the configs list will be empty and the IT will be a
			// no-op
			if (containsAnnotationForType(storeType)) {
				configs.add(new GeoWaveStoreRunnerConfig(
						storeType,
						dataStoreOptionFields));
				storeTypes.add(storeType);
			}
		}
		else { // No user override - just use the IT's list of types
			if (typeIsAnnotated()) {
				if (fieldsAreAnnotated()) {
					throw new GeoWaveITException(
							"Only type or fields can be annotated with @GeoWaveTestStore, not both");
				}
				final Set<String> dataStoreOptionFields = getDataStoreOptionFieldsForTypeAnnotation();
				final GeoWaveTestStore store = getTestClass().getJavaClass().getAnnotation(
						GeoWaveTestStore.class);
				for (final GeoWaveStoreType storeType : store.value()) {
					configs.add(new GeoWaveStoreRunnerConfig(
							storeType,
							dataStoreOptionFields));
					storeTypes.add(storeType);
				}
			}
			else {
				configs.add(emptyConfig);
				final List<FrameworkField> storeFields = getStoreAnnotatedFields();
				for (final FrameworkField field : storeFields) {
					configs = addRunnerConfigsForField(
							field,
							configs,
							storeTypes);
				}
			}
		}

		// Create a test runner for each store type / config
		for (final GeoWaveStoreRunnerConfig config : configs) {
			final TestClassRunnerForStoreTypes runner = new TestClassRunnerForStoreTypes(
					getTestClass().getJavaClass(),
					config.fieldNameStoreTypePair);
			runners.add(runner);
		}
	}

	private boolean containsAnnotationForType(
			final GeoWaveStoreType storeType ) {
		if (typeIsAnnotated()) {
			final GeoWaveTestStore store = getTestClass().getJavaClass().getAnnotation(
					GeoWaveTestStore.class);
			for (final GeoWaveStoreType annotationType : store.value()) {
				if (annotationType == storeType) {
					return true;
				}
			}
		}
		else {
			for (final FrameworkField field : getTestClass().getAnnotatedFields(
					GeoWaveTestStore.class)) {
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
			throws SecurityException,
			GeoWaveITException {
		final Field[] fields = getTestClass().getJavaClass().getDeclaredFields();
		final Set<String> dataStoreOptionFields = new HashSet<String>();
		for (final Field field : fields) {
			if (field.getType().isAssignableFrom(
					DataStorePluginOptions.class)) {
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
			final Set<GeoWaveStoreType> storeTypes )
			throws GeoWaveITException {
		final GeoWaveTestStore store = field.getField().getAnnotation(
				GeoWaveTestStore.class);
		final GeoWaveStoreType[] types = store.value();
		if ((types == null) || (types.length == 0)) {
			throw new GeoWaveITException(
					MessageFormat.format(
							"{0} must have at least one GeoWaveStoreType",
							field.getName()));
		}
		final List<GeoWaveStoreRunnerConfig> newConfigs = new ArrayList<GeoWaveStoreRunnerConfig>();
		for (final GeoWaveStoreRunnerConfig config : currentConfigs) {
			for (final GeoWaveStoreType type : types) {
				newConfigs.add(new GeoWaveStoreRunnerConfig(
						config,
						field.getName(),
						type));

				storeTypes.add(type);
			}
		}
		return newConfigs;
	}

	private List<FrameworkField> getStoreAnnotatedFields() {
		return getTestClass().getAnnotatedFields(
				GeoWaveTestStore.class);
	}

	private List<FrameworkMethod> getTestEnvAnnotatedMethods() {
		return getTestClass().getAnnotatedMethods(
				Environments.class);
	}

	private TestEnvironment[] getTestEnvironments()
			throws NullPointerException {
		final Set<Environment> environments = new HashSet<Environment>();
		final Environments es = getTestClass().getJavaClass().getAnnotation(
				Environments.class);
		if (es != null) {
			final Environment[] envs = es.value();
			for (final Environment env : envs) {
				environments.add(env);
			}
		}
		final List<FrameworkMethod> envMethods = getTestEnvAnnotatedMethods();

		for (final FrameworkMethod m : envMethods) {
			final Environment[] envs = m.getMethod().getAnnotation(
					Environments.class).value();
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

	private TestEnvironment[] processDependencies(
			final TestEnvironment[] testEnvs ) {
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
		return getTestClass().getJavaClass().isAnnotationPresent(
				GeoWaveTestStore.class);
	}

	protected void setup()
			throws Exception {
		synchronized (MUTEX) {
			TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
			for (final TestEnvironment e : testEnvs) {
				e.setup();
			}
		}
	}

	protected void tearDown()
			throws Exception {
		synchronized (MUTEX) {
			if (!DEFER_CLEANUP.get()) {
				// Tearodwn in reverse
				final List<TestEnvironment> envs = Arrays.asList(testEnvs);
				final ListIterator<TestEnvironment> it = envs.listIterator(envs.size());
				while (it.hasPrevious()) {
					it.previous().tearDown();
				}
			}
		}
	}

	private static class GeoWaveStoreRunnerConfig
	{
		private final Map<String, GeoWaveStoreType> fieldNameStoreTypePair;

		public GeoWaveStoreRunnerConfig() {
			fieldNameStoreTypePair = new HashMap<String, GeoWaveStoreType>();
		}

		public GeoWaveStoreRunnerConfig(
				final GeoWaveStoreType storeType,
				final Set<String> fieldNames ) {
			fieldNameStoreTypePair = new HashMap<String, GeoWaveStoreType>();
			for (final String fieldName : fieldNames) {
				fieldNameStoreTypePair.put(
						fieldName,
						storeType);
			}
		}

		public GeoWaveStoreRunnerConfig(
				final GeoWaveStoreRunnerConfig previousConfig,
				final String name,
				final GeoWaveStoreType type ) {
			if ((previousConfig == null) || (previousConfig.fieldNameStoreTypePair == null)) {
				fieldNameStoreTypePair = new HashMap<String, GeoWaveStoreType>();
			}
			else {
				fieldNameStoreTypePair = new HashMap<String, GeoWaveStoreType>(
						previousConfig.fieldNameStoreTypePair);
			}
			fieldNameStoreTypePair.put(
					name,
					type);
		}
	}

	private static class GeoWaveITException extends
			Exception
	{

		/**
		 *
		 */
		private static final long serialVersionUID = 1L;

		public GeoWaveITException(
				final String message ) {
			super(
					message);
		}

	}

	private static class TestEnvironmentDependencyTree
	{
		// just keep a two-way mapping although I think we only need to traverse
		// in one direction
		Map<TestEnvironment, Set<TestEnvironment>> dependenciesMapping = new LinkedHashMap<TestEnvironment, Set<TestEnvironment>>();
		Map<TestEnvironment, Set<TestEnvironment>> requirementsMapping = new LinkedHashMap<TestEnvironment, Set<TestEnvironment>>();
		Set<TestEnvironment> independentEnvironments = new LinkedHashSet<TestEnvironment>();
		Set<TestEnvironment> visitedEnvs = new LinkedHashSet<TestEnvironment>();

		private TestEnvironmentDependencyTree() {}

		private void processDependencies(
				final TestEnvironment env ) {
			if (!visitedEnvs.contains(env)) {
				visitedEnvs.add(env);
				if ((env.getDependentEnvironments() == null) || (env.getDependentEnvironments().length == 0)) {
					independentEnvironments.add(env);
				}
				else {

					for (final TestEnvironment requiredEnv : env.getDependentEnvironments()) {
						Set<TestEnvironment> dependentSet = dependenciesMapping.get(requiredEnv);
						if (dependentSet == null) {
							dependentSet = new HashSet<TestEnvironment>();
							dependenciesMapping.put(
									requiredEnv,
									dependentSet);
						}
						dependentSet.add(env);
						Set<TestEnvironment> requiredSet = requirementsMapping.get(env);
						if (requiredSet == null) {
							requiredSet = new HashSet<TestEnvironment>();
							requirementsMapping.put(
									env,
									requiredSet);
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
			final Set<TestEnvironment> testsAddedToArray = new HashSet<TestEnvironment>();
			for (final TestEnvironment e : independentEnvironments) {
				retVal[i++] = e;
				testsAddedToArray.add(e);
			}
			for (final TestEnvironment entry : requirementsMapping.keySet()) {
				traverseRequirements(
						entry,
						retVal,
						i++,
						testsAddedToArray);
			}
			return retVal;
		}

		private int traverseRequirements(
				final TestEnvironment env,
				final TestEnvironment[] currentOrderedArray,
				final int startIndex,
				final Set<TestEnvironment> testsAddedToArray ) {
			int count = 0;
			final Set<TestEnvironment> requirements = requirementsMapping.get(env);
			for (final TestEnvironment req : requirements) {
				if (!testsAddedToArray.contains(req)) {
					count = traverseRequirements(
							req,
							currentOrderedArray,
							startIndex + count,
							testsAddedToArray);
				}
			}
			currentOrderedArray[startIndex + count++] = env;
			testsAddedToArray.add(env);
			return count;
		}
	}
}
